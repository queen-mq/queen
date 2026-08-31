#!/usr/bin/env python
"""aiokafka against a queen-kafka SASL/PLAIN + TLS listener (the M5 lane).

    ./sasl_tls.py [tls-bootstrap] [runId]

    env: QUEEN_KAFKA_SASL_TOKEN  the bearer token = the SASL password
         QUEEN_KAFKA_TLS_CA      PEM for the listener's certificate

THE HEADLINE: aiokafka cannot authenticate against this facade, and it is the
facade's side of the wire.

aiokafka builds its PLAIN initial response as

    "\\0".join([username, username, password])   # aiokafka/conn.py:616-622
                                                 # class SaslPlainAuthenticator

so the username is sent in the AUTHZID field as well as the authcid field.
RFC 4616 allows that, and Apache Kafka's own PlainSaslServer allows it -- the
only authzid it refuses is one that DIFFERS from the username.

queen-kafka/src/sasl.rs:217 refuses ANY non-empty authzid:

    if !authzid.is_empty() {
        return Err(PlainError::Impersonation);
    }

There is no client-side workaround, because in aiokafka the username IS the
authzid: emptying one empties the other, and the facade then answers NoUsername.
Case 2 demonstrates that dead end rather than asserting it.

This is the SAME defect compat/kafka-python found, reached independently by a
client with its own connection layer. Both halves of the pip-installable Python
Kafka ecosystem are locked out of a SASL listener by this one check.

Case 3 monkeypatches the authenticator to send the empty authzid every other
client sends, and drives a full produce + group consume over TLS through it.
That is NOT a deployable workaround; it is the proof that this single check is
the only thing in the way, and that TLS, the credential forwarding and the
authgate's refusal of a bad credential all work correctly behind it.
"""

import asyncio
import os
import ssl
import sys

from _common import (
    Trace,
    bad,
    check,
    fmt_versions,
    info,
    ok,
    print_environment,
    report,
    say,
    watchdog,
)

import aiokafka.conn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:19093"
RUN = sys.argv[2] if len(sys.argv) > 2 else str(int(__import__("time").time()))
TOKEN = os.environ.get("QUEEN_KAFKA_SASL_TOKEN", "")
CA = os.environ.get("QUEEN_KAFKA_TLS_CA", "")
TRACE_PATH = os.environ.get("AIOKAFKA_TRACE", f"/tmp/aiokafka-sasl-{RUN}.log")

TOPIC = f"aiok-{RUN}-sasl"
GROUP = f"aiok-{RUN}-sasl-g"


def ssl_ctx():
    """Verify the certificate properly.

    The rig's cert carries an IP SAN for 127.0.0.1, so a HOST client dialling
    the advertised 127.0.0.1 gets REAL hostname verification -- no
    check_hostname=False anywhere in this file. (A containerised client would
    have to disable it, because host.docker.internal is not among the SANs.)
    """
    ctx = ssl.create_default_context(cafile=CA) if CA else ssl.create_default_context()
    return ctx


def client_kw():
    return dict(
        bootstrap_servers=BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="aiokafka-compat",
        sasl_plain_password=TOKEN,
        ssl_context=ssl_ctx(),
    )


# ------------------------------------------------------- the empty-authzid patch
_ORIGINAL = aiokafka.conn.SaslPlainAuthenticator.authenticator_plain


def _empty_authzid(self):
    """What every other Kafka client sends: NUL authcid NUL passwd."""
    data = (
        b"\0"
        + self._sasl_plain_username.encode("utf-8")
        + b"\0"
        + self._sasl_plain_password.encode("utf-8")
    )
    resp = yield data, True
    assert resp == b"", "Server should either close or send an empty response"


def patch(on):
    aiokafka.conn.SaslPlainAuthenticator.authenticator_plain = (
        _empty_authzid if on else _ORIGINAL
    )


# ---------------------------------------------------------------------- cases
async def try_start(kw, seconds=25):
    """Start a producer; return (ok, exception-or-None)."""
    p = AIOKafkaProducer(acks="all", enable_idempotence=False, **kw)
    try:
        await asyncio.wait_for(p.start(), seconds)
        return True, None, p
    except Exception as e:  # noqa: BLE001 - the exception IS the result
        try:
            await asyncio.wait_for(p.stop(), 8)
        except Exception:  # noqa: BLE001
            pass
        return False, e, None


async def case1_default_framing():
    say("1. aiokafka's own SASL/PLAIN framing, with the CORRECT token")
    patch(False)
    good, err, p = await try_start(client_kw())
    if good:
        await p.stop()
        ok("UNEXPECTED: authentication succeeded -- has sasl.rs:217 changed?")
        info("if so, this suite's headline finding is stale; re-read the module docstring")
        return
    info(f"{type(err).__name__}: {str(err)[:200]}")
    bad(
        "aiokafka cannot authenticate: it sends username as the authzid "
        "(conn.py:616), sasl.rs:217 refuses any authzid"
    )
    info("wire: SaslAuthenticateRequest_v1(sasl_auth_bytes=b'USER\\x00USER\\x00TOKEN')")
    info("facade: error_code=58, 'an authorization identity was requested'")
    info(
        "NOTE the API-surface error is the generic KafkaConnectionError "
        "'Unable to bootstrap'; the real reason only appears at DEBUG level"
    )


async def case2_dead_end():
    say("2. the dead end: you cannot empty the authzid without emptying the username")
    patch(False)
    kw = client_kw()
    kw["sasl_plain_username"] = ""
    good, err, p = await try_start(kw)
    if good:
        await p.stop()
        bad("an empty username authenticated, which should not happen")
        return
    txt = str(err)
    info(f"empty username -> {type(err).__name__}: {txt[:200]}")
    ok("both spellings are refused: there is no client-side configuration that works")


async def case3_patched_roundtrip():
    say("3. with the empty authzid patched in: full produce + group consume over TLS")
    patch(True)
    good, err, p = await try_start(client_kw())
    if not check(good, f"authenticated over SASL_SSL ({type(err).__name__ if err else 'ok'})"):
        info(f"{err}")
        return
    try:
        md = [
            await p.send_and_wait(
                TOPIC, f"tls-{i}".encode(), key=f"tk-{i}".encode(),
                headers=[("lane", b"m5")],
            )
            for i in range(24)
        ]
        ok(f"produced {len(md)} records over TLS, partitions {sorted({m.partition for m in md})}")
    finally:
        await p.stop()

    c = AIOKafkaConsumer(
        TOPIC,
        group_id=GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        **client_kw(),
    )
    await asyncio.wait_for(c.start(), 40)
    got = []
    try:
        loop = asyncio.get_running_loop()
        end = loop.time() + 60
        while len(got) < 24 and loop.time() < end:
            batch = await c.getmany(timeout_ms=2000, max_records=24 - len(got))
            for _tp, recs in batch.items():
                got.extend(recs)
        await c.commit()
    finally:
        try:
            await asyncio.wait_for(c.stop(), 15)
        except Exception as e:  # noqa: BLE001
            info(f"consumer stop raised {type(e).__name__}: {e}")
    check(len(got) == 24, f"group consumed {len(got)}/24 records over SASL_SSL")
    exact = all(
        r.value == f"tls-{int(r.key.decode().rsplit('-', 1)[1])}".encode() for r in got
    )
    check(exact and got, "values byte-exact over the TLS lane")
    check(
        all(tuple(r.headers) == (("lane", b"m5"),) for r in got) and got,
        "headers survive the TLS lane",
    )
    ok("committed offsets through the authenticated listener")


async def case4_wrong_password():
    say("4. with the same patch, a WRONG password must still be refused")
    patch(True)
    kw = client_kw()
    kw["sasl_plain_password"] = "definitely-not-the-token"
    good, err, p = await try_start(kw)
    if good:
        await p.stop()
        bad("a wrong password authenticated -- the credential is not being checked")
        return
    info(f"{type(err).__name__}: {str(err)[:220]}")
    ok("the authgate refused the bad credential; the facade forwards it for real")


async def main():
    watchdog(int(os.environ.get("AIOKAFKA_SUITE_TIMEOUT", "600")), "sasl_tls.py")
    print_environment(BOOTSTRAP, RUN, extra=" lane=SASL_SSL")
    if not TOKEN:
        bad("QUEEN_KAFKA_SASL_TOKEN is unset; nothing to authenticate with")
        return report()
    info(f"CA={CA or '(system trust store)'} -- hostname verification stays ON")
    with Trace(TRACE_PATH) as tr:
        try:
            await case1_default_framing()
            await case2_dead_end()
            await case3_patched_roundtrip()
            await case4_wrong_password()
        finally:
            patch(False)
        say("negotiated API versions on the TLS lane")
        print(f"  {fmt_versions(tr.sent_versions())}")
        info(f"trace: {TRACE_PATH}")
    return report()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
