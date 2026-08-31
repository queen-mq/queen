#!/usr/bin/env python3
"""The M5 lane for kafka-python: SASL/PLAIN over TLS against queen-kafka.

THIS LANE FAILS TODAY, and the failure is in the facade, not in the client.

kafka-python builds its PLAIN initial response as

    '\\0'.join([username, username, password])          # kafka/sasl/plain.py:26
                                                        # kafka/conn.py:628 on 2.0.x

i.e. it puts the username in the authzid field as well as the authcid field.
RFC 4616 allows that, and Apache Kafka's own PlainSaslServer allows it: the
only authzid it refuses is one that DIFFERS from the username
("Authentication failed: Client requested an authorization id that is different
from username", verified in kafka-clients-3.9.1.jar).

queen-kafka refuses ANY non-empty authzid — `protocols/queen-kafka/src/sasl.rs:218`:

    if !authzid.is_empty() { return Err(PlainError::Impersonation); }

so every kafka-python release, on every config, is locked out of a SASL
listener. There is no client-side workaround: the username is the authzid, so
emptying one empties the other and the facade then answers `NoUsername`. Both
dead ends are exercised below, because "there is no workaround" is a claim that
has to be demonstrated rather than asserted.

Case 3 patches the client to send the empty authzid every other client sends,
and drives the whole produce + group-consume path over TLS through it. That is
not a workaround anybody can deploy — it monkeypatches a library — it is the
proof that this one check is the ONLY thing in the way, and that TLS, SNI
forwarding and credential forwarding are all fine behind it.

The rig's self-signed certificate carries SANs for kafka.example.com,
shared.queenmq.cloud, localhost and the IP 127.0.0.1, so a host client dialling
127.0.0.1 verifies the chain properly here rather than reaching for
ssl_check_hostname=False. A containerised client cannot: host.docker.internal
is not a SAN.

Usage:  sasl_tls.py [bootstrap] [runid]
Env:    QUEEN_KAFKA_SASL_TOKEN  the bearer token = the SASL password (required)
        QUEEN_KAFKA_TLS_CA      PEM to verify the listener with (required)
"""
import os
import re
import sys
import tempfile
import time

from kafka import KafkaConsumer, KafkaProducer

from _common import (api_version_from_env, bad, check, fmt_versions, info, ok,
                     print_environment, report, say, supported, Trace, watchdog)

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:19093"
RUN = sys.argv[2] if len(sys.argv) > 2 else str(int(time.time()))
TOKEN = os.environ.get("QUEEN_KAFKA_SASL_TOKEN", "")
CAFILE = os.environ.get("QUEEN_KAFKA_TLS_CA", "")
API_VERSION = api_version_from_env()

WORK = tempfile.mkdtemp(prefix="kafka-python-sasl.")
TOPIC = f"kp-tls-{RUN}"
GROUP = f"kp-tls-g-{RUN}"
COUNT = 64

IMPERSONATION = "an authorization identity was requested"
NO_USERNAME = "no username"


def sasl_cfg(password, username="kafka-python-compat", **kw):
    cfg = dict(
        bootstrap_servers=BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=username,  # a free label to the facade... and the authzid
        sasl_plain_password=password,  # the Queen bearer token
        ssl_cafile=CAFILE,
        ssl_check_hostname=True,
    )
    cfg.setdefault("request_timeout_ms", 10000)
    cfg.setdefault("api_version_auto_timeout_ms", 10000)
    cfg.update(kw)
    if API_VERSION is not None:
        cfg["api_version"] = API_VERSION
    return cfg


def try_send(label, username, password, budget=12):
    """Build a producer and actually SEND. Returns (sent, error_text, trace).

    The constructor is not the test: kafka-python's KafkaProducer() returns
    happily from a connection that was refused mid-SASL and only surfaces it as
    a metadata timeout on the first send. Asserting on the constructor would
    have this suite report a pass over a refused credential.
    """
    trace = Trace(os.path.join(WORK, f"{label}.log"))
    sent = False
    err = None
    with trace:
        try:
            p = KafkaProducer(**supported(KafkaProducer, **sasl_cfg(
                password, username=username, acks="all", retries=0,
                max_block_ms=budget * 1000)))
            p.send(TOPIC, key=b"k", value=b"v").get(timeout=budget)
            sent = True
            p.close(timeout=5)
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}"
    return sent, err, trace


def sasl_reason(trace):
    """The facade's own error_message, as kafka-python logged it."""
    m = re.search(r"SaslAuthenticationFailedError[: (]+([^\n)]+)", trace.text())
    return m.group(1).strip() if m else None


def auth_bytes_seen(trace):
    m = re.search(r"SaslAuthenticateRequest_v\d+\(auth_bytes=(b'[^']*'|b\"[^\"]*\")",
                  trace.text())
    if m:
        return m.group(1)
    m = re.search(r"SaslAuthenticateRequest\(version=\d+, auth_bytes=(b'[^']*')", trace.text())
    return m.group(1) if m else None


def main():
    print_environment(BOOTSTRAP, RUN, extra=f" ca={CAFILE} trace_dir={WORK}")
    watchdog(420, "sasl_tls.py")
    if not TOKEN or not CAFILE:
        bad("QUEEN_KAFKA_SASL_TOKEN and QUEEN_KAFKA_TLS_CA must both be set; "
            "this lane cannot run without them")
        return report()

    # ------------------------------------------------------------------ 1
    say("1. the RIGHT token, stock kafka-python config")
    sent, err, trace = try_send("right", "kafka-python-compat", TOKEN)
    info(f"PLAIN initial response on the wire: {auth_bytes_seen(trace)}")
    info(f"facade error_message: {sasl_reason(trace)}")
    reason = sasl_reason(trace) or ""
    if sent:
        ok("produce over SASL_SSL succeeded with a stock kafka-python config")
    else:
        bad("kafka-python cannot authenticate to a SASL listener at all: it sends "
            "username\\0username\\0password (kafka/sasl/plain.py:26, kafka/conn.py:628 "
            "on 2.0.x) and protocols/queen-kafka/src/sasl.rs:218 refuses any non-empty authzid. "
            "Apache Kafka's PlainSaslServer refuses only an authzid DIFFERENT from the "
            "username, so this rejects a response a real broker accepts")
        info(f"the client saw: {err}")
        if IMPERSONATION in reason:
            info("the facade's reason reached the client, so this release surfaces it")
        elif auth_bytes_seen(trace) is None:
            info("this release uses the SaslHandshake v0 RAW framing (the credential "
                 "goes as a bare length-prefixed blob, not inside a SaslAuthenticate "
                 "request), so nothing names the reason client-side. Read the facade "
                 "log: `sasl refused this connection: an authorization identity ...`")
        else:
            info("the client sent SaslAuthenticate but never saw the refusal: on 3.x "
                 "the socket closes behind the response too fast for its async "
                 "transport, so it reports 'socket disconnected' and retries. See "
                 "raw_sasl_probe.py, which reads that same response off the wire")
        info("either way the shape is the same: not a fast fatal error but a metadata "
             "timeout with reconnect backoff - kafka-python retries a refused "
             "credential instead of giving up")

    # ------------------------------------------------------------------ 2
    say("2. is there a client-side workaround? (empty the username to empty the authzid)")
    sent2, err2, trace2 = try_send("emptyuser", "", TOKEN)
    reason2 = sasl_reason(trace2) or ""
    info(f"PLAIN initial response on the wire: {auth_bytes_seen(trace2)}")
    info(f"facade error_message: {reason2}")
    check(not sent2,
          "no: emptying sasl_plain_username empties the authzid AND the username, "
          "and the facade then refuses the missing username. The two fields are "
          "the same string in this client, so no configuration reaches an "
          "acceptable PLAIN response"
          + (f" (facade said: {NO_USERNAME}...)" if NO_USERNAME in reason2
             else " (reason not visible client-side on this release; see the facade log)"))

    # ------------------------------------------------------------------ 3
    say("3. with the client patched to send an empty authzid, does the rest of the "
        "lane work?")
    patched = patch_empty_authzid()
    if not patched:
        info("this release builds the PLAIN response inline in BrokerConnection "
             "rather than in kafka.sasl.plain, so the patch does not apply here; "
             "skipping (run this case on 2.2+ for the proof)")
        return report()

    sent3, err3, trace3 = try_send("patched", "kafka-python-compat", TOKEN, budget=20)
    info(f"PLAIN initial response on the wire: {auth_bytes_seen(trace3)}")
    if not check(sent3, "produce over SASL_SSL succeeds once the authzid is empty "
                        f"({err3 or 'no error'})"):
        return report()

    with trace3:
        p = KafkaProducer(**supported(KafkaProducer, **sasl_cfg(
            TOKEN, acks="all", retries=0, max_block_ms=30000)))
        metas = [p.send(TOPIC, key=f"k{i}".encode(),
                        value=f"tls-{RUN}-{i:03d}".encode(),
                        headers=[("lane", b"m5")], partition=i % 8)
                 for i in range(COUNT)]
        p.flush(timeout=30)
        for m in metas:
            m.get(timeout=20)
        p.close(timeout=10)
        ok(f"{COUNT} records produced over the encrypted listener with acks=all, "
           f"chain verified against the rig cert (ssl_check_hostname=True; the "
           f"cert carries an IP SAN for 127.0.0.1)")

        c = KafkaConsumer(TOPIC, **supported(KafkaConsumer, **sasl_cfg(
            TOKEN, group_id=GROUP, auto_offset_reset="earliest",
            enable_auto_commit=False, consumer_timeout_ms=45000,
            request_timeout_ms=40000, max_poll_records=200)))
        got = []
        deadline = time.time() + 120
        while len(got) < COUNT and time.time() < deadline:
            for _tp, recs in c.poll(timeout_ms=1000, max_records=200).items():
                got.extend(recs)
        tls_records = [r for r in got if r.value.startswith(b"tls-")]
        check(len(tls_records) >= COUNT,
              f"a consumer GROUP read all {COUNT} back over the same TLS listener "
              f"(got {len(tls_records)})")
        check({r.value for r in tls_records} >= {f"tls-{RUN}-{i:03d}".encode()
                                                 for i in range(COUNT)},
              "byte-exact over TLS, no loss")
        check(all(r.headers == [("lane", b"m5")] for r in tls_records),
              "headers survive the encrypted path too")
        c.commit()
        c.close()

    # ------------------------------------------------------------------ 4
    say("4. and a WRONG token is still refused (the gate is real, not bypassed)")
    started = time.time()
    sent4, err4, trace4 = try_send("wrong", "kafka-python-compat", TOKEN + "-nope")
    reason4 = sasl_reason(trace4) or ""
    check(not sent4, "a wrong SASL password does not get to produce")
    info(f"facade error_message as this client saw it: {reason4 or '(none — see below)'}")
    if reason4:
        check("401" in reason4 or "credential" in reason4.lower(),
              f"and the refusal names the credential rather than something vague "
              f"({reason4[:80]!r})")
    elif auth_bytes_seen(trace4) is None:
        info("this release uses the SaslHandshake v0 RAW framing, where a refusal is "
             "a bare disconnect with no error code anywhere on the wire — that is the "
             "protocol, not the facade. The reason is in the facade log only")
    else:
        info("this release never surfaces the refusal: the facade answers "
             "error_code=58 with the message (proved by raw_sasl_probe.py) and closes "
             "with no gap, and kafka-python 3.x's async transport loses the response "
             "to the close. Holding the FIN 300ms makes 3.0.11 print it")
    info(f"the client took {time.time() - started:.1f}s to give up, and gives up on a "
         f"timeout rather than on a fatal auth error")

    say("API versions this client NEGOTIATED over TLS")
    seen = trace3.sent_versions()
    info(f"inferred broker release: {trace3.inferred()}")
    info(fmt_versions(seen))
    sasl_seen = {k: v for k, v in seen.items() if k.startswith("Sasl")}
    check(bool(sasl_seen), f"the SASL exchange is visible on the wire: "
                           f"{fmt_versions(sasl_seen)}")
    return report()


def patch_empty_authzid():
    """Make kafka-python send `\\0user\\0pass`, the way every other client does.

    NOT a deployable workaround — it rewrites library internals at runtime. It
    exists so that "the authzid check is the only thing in the way" is a
    measured claim rather than a hopeful one.

    Four code layouts across the releases this suite covers, so two patches:

      * 2.2+/3.x expose a mechanism class (`kafka.sasl.plain` on 2.2/2.3,
        `kafka.net.sasl.plain` on 3.x) with an `auth_bytes()` to override;
      * 2.0.x and kafka-python-ng build the response inline and push it down
        `BrokerConnection._send_bytes_blocking` as a length-prefixed blob —
        the SaslHandshake **v0** raw framing, with no Kafka request around it —
        so the patch goes on that method and rewrites the blob in flight.
    """
    for mod_name in ("kafka.net.sasl.plain", "kafka.sasl.plain"):
        try:
            mod = __import__(mod_name, fromlist=["x"])
        except ImportError:
            continue
        cls = getattr(mod, "SaslMechanismPlain", None)
        if cls is not None and hasattr(cls, "auth_bytes"):
            cls.auth_bytes = lambda self: "\0".join(
                ["", self.username, self.password]).encode("utf-8")
            info(f"patched {mod_name}.SaslMechanismPlain.auth_bytes to send an EMPTY "
                 f"authzid (what librdkafka, franz-go and the Java client all send)")
            return True

    try:
        from kafka.conn import BrokerConnection
    except ImportError:
        return False
    if not hasattr(BrokerConnection, "_send_bytes_blocking"):
        return False
    original = BrokerConnection._send_bytes_blocking

    def patched(self, data):
        # `Int32(len) + user\0user\0pass` on the raw v0 SASL framing. Rewriting
        # it here rather than at the call site because 2.0.x builds the string
        # inline from one config key used twice, so there is no other seam.
        # Count NULs in the BLOB, never in `data`: the Int32 length prefix is
        # itself mostly NUL bytes, which is how the first draft of this missed.
        if len(data) > 4:
            blob = data[4:]
            parts = blob.split(b"\0")
            if (len(parts) == 3 and parts[0] and parts[0] == parts[1]
                    and int.from_bytes(data[:4], "big") == len(blob)):
                blob = b"\0" + parts[1] + b"\0" + parts[2]
                data = len(blob).to_bytes(4, "big") + blob
        return original(self, data)

    BrokerConnection._send_bytes_blocking = patched
    info("patched BrokerConnection._send_bytes_blocking to empty the authzid on the "
         "raw v0 SASL framing this release uses")
    return True


if __name__ == "__main__":
    sys.exit(main())
