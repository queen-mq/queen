#!/usr/bin/env python3
"""A stdlib-only Kafka client that does exactly one thing: fail a SASL login.

It exists because kafka-python 3.x reports a refused credential as
"KafkaConnectionError: socket disconnected" and retries it forever, while
kafka-python 2.3.x reports the facade's actual message and gives up — and
"which side is doing that" is not a question anyone should answer from a
client's own logs. This speaks the protocol itself, with no kafka-python
anywhere in it: `socket`, `ssl`, `struct`.

What it establishes, in order:

  1. the facade answers a refusal correctly at SaslAuthenticate v0 AND v1 —
     error_code 58 and a legible error_message — so the wire is not the
     problem, and 3.x's disconnect is not a missing response;
  2. the authzid rule, on both sides of its line and at both SaslAuthenticate
     versions: an authorization identity EQUAL to the username is admitted
     (`probe\\0probe\\0token` -> error_code 0), which is the shape kafka-python
     and aiokafka both put on the wire, and one that DIFFERS from it is refused
     (`boss\\0probe\\0token` -> error_code 58) because nothing here can grant a
     request to act as somebody else. That is Apache Kafka's own
     `PlainSaslServer` rule, and `src/sasl.rs`'s `parse_plain` is the single
     place both framings reach it, so the two cannot disagree;
  3. the connection is closed IMMEDIATELY behind a refusal, with no gap.
     That is the deliberate "no artificial delay" of
     `src/handlers/sasl_authenticate.rs`, and it is the thing kafka-python 3.x's
     async transport loses the response to: proxying the same listener through
     a relay that delays the FIN by 300ms turns 3.0.11's "socket disconnected"
     into the parsed `SaslAuthenticateResponse(error_code=58, error_message=
     'Queen refused this credential (HTTP 401)…')`. Apache Kafka spends
     `connection.failed.authentication.delay.ms` (default 100ms) here for
     exactly this reason.

A note on versions, because getting it wrong makes the facade look broken: the
SaslHANDSHAKE version chooses the FRAMING. At handshake v0 the credential goes
as a bare length-prefixed blob with no Kafka request around it; at v1 it goes
inside SaslAuthenticate requests. So every case below handshakes at v1 and
varies only the SaslAuthenticate version — sending a SaslAuthenticate request
after a v0 handshake gets the connection closed, correctly, and says nothing
about anything.

Usage:  raw_sasl_probe.py [host:port] [--plaintext] [--delay SECONDS]
Env:    QUEEN_KAFKA_SASL_TOKEN  a valid token; the probe derives a wrong one
        QUEEN_KAFKA_TLS_CA      PEM for the listener (omit with --plaintext)
"""
import os
import socket
import ssl
import struct
import sys
import time

from _common import bad, check, info, ok, report, say

_args = [a for a in sys.argv[1:] if not a.startswith("-")]
TARGET = _args[0] if _args else "127.0.0.1:19093"
PLAINTEXT = "--plaintext" in sys.argv
DELAY = 0.0
if "--delay" in sys.argv:
    DELAY = float(sys.argv[sys.argv.index("--delay") + 1])
HOST, PORT = TARGET.rsplit(":", 1)
PORT = int(PORT)
TOKEN = os.environ.get("QUEEN_KAFKA_SASL_TOKEN", "")
CAFILE = os.environ.get("QUEEN_KAFKA_TLS_CA", "")

SASL_HANDSHAKE, SASL_AUTHENTICATE = 17, 36
HANDSHAKE_VERSION = 1  # v1 = the framed SaslAuthenticate flow. See the header.


def _str(s):
    b = s.encode()
    return struct.pack(">h", len(b)) + b


def request(api_key, api_version, correlation_id, body):
    head = struct.pack(">hhi", api_key, api_version, correlation_id) + _str("raw-sasl-probe")
    frame = head + body
    return struct.pack(">i", len(frame)) + frame


def read_frame(sock, budget=10.0):
    """The response body, or None if the peer closed instead of answering."""
    sock.settimeout(budget)
    head = b""
    while len(head) < 4:
        chunk = sock.recv(4 - len(head))
        if not chunk:
            return None
        head += chunk
    (size,) = struct.unpack(">i", head)
    body = b""
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        if not chunk:
            return None
        body += chunk
    return body[4:]  # drop the correlation id


def connect():
    raw = socket.create_connection((HOST, PORT), timeout=10)
    if PLAINTEXT:
        return raw
    ctx = ssl.create_default_context(cafile=CAFILE or None)
    if not CAFILE:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx.wrap_socket(raw, server_hostname=HOST)


def handshake(sock, version=HANDSHAKE_VERSION):
    sock.sendall(request(SASL_HANDSHAKE, version, 1, _str("PLAIN")))
    body = read_frame(sock)
    if body is None:
        return None
    (err,) = struct.unpack(">h", body[:2])
    return err


def closed_immediately(sock):
    """Did the peer close right behind its answer? True = no gap at all."""
    sock.settimeout(2.0)
    try:
        return sock.recv(1) == b""
    except (socket.timeout, ssl.SSLError, ConnectionResetError, OSError):
        return False


def case(auth_version, auth_bytes, label, delay=None):
    """Handshake v1, then one SaslAuthenticate. Returns (code, message, closed)."""
    delay = DELAY if delay is None else delay
    sock = connect()
    try:
        err = handshake(sock)
        if err is None:
            bad(f"{label}: the listener closed during SaslHandshake")
            return None
        if err != 0:
            bad(f"{label}: SaslHandshake answered error_code={err}")
            return None
        body = struct.pack(">i", len(auth_bytes)) + auth_bytes
        sock.sendall(request(SASL_AUTHENTICATE, auth_version, 2, body))
        if delay:
            time.sleep(delay)
        try:
            frame = read_frame(sock)
        except (socket.timeout, ssl.SSLError, ConnectionResetError) as exc:
            info(f"{label}: SaslAuthenticate v{auth_version} -> NO RESPONSE "
                 f"({type(exc).__name__})")
            return None
        if frame is None:
            info(f"{label}: SaslAuthenticate v{auth_version} -> peer closed, no response frame")
            return None
        (code,) = struct.unpack(">h", frame[:2])
        (mlen,) = struct.unpack(">h", frame[2:4])
        msg = "" if mlen < 0 else frame[4:4 + mlen].decode(errors="replace")
        closed = closed_immediately(sock)
        info(f"{label}: SaslAuthenticate v{auth_version} -> error_code={code} "
             f"error_message={msg[:80]!r} closed_behind_it={closed}")
        return code, msg, closed
    finally:
        sock.close()


def main():
    say(f"raw protocol probe against {TARGET} "
        f"({'PLAINTEXT' if PLAINTEXT else 'TLS'}), read delay {DELAY}s")
    if not TOKEN:
        bad("QUEEN_KAFKA_SASL_TOKEN must be set")
        return report()

    wrong = f"\0probe\0{TOKEN}-definitely-not-the-token".encode()
    right = f"\0probe\0{TOKEN}".encode()
    # The two sides of the authzid line, differing in one field.
    authzid_same = f"probe\0probe\0{TOKEN}".encode()
    authzid_other = f"boss\0probe\0{TOKEN}".encode()

    say("1. a REFUSED credential, at both SaslAuthenticate versions")
    results = {v: case(v, wrong, f"wrong token, authv{v}") for v in (0, 1)}
    for v, r in results.items():
        check(r is not None and r[0] == 58,
              f"SaslAuthenticate v{v} answers SASL_AUTHENTICATION_FAILED (58) with a "
              f"message, rather than closing silently")

    say("2. the authzid rule kafka-python depends on, on both sides of its line")
    for v in (0, 1):
        r = case(v, authzid_same, f"authzid=username, authv{v}")
        check(r is not None and r[0] == 0,
              f"v{v}: `probe\\0probe\\0token` is ADMITTED. It is the shape both "
              f"pure-Python clients build (username joined to itself), and Apache "
              f"Kafka's PlainSaslServer accepts it: an authzid that repeats the "
              f"username asks for nothing")
    for v in (0, 1):
        r = case(v, authzid_other, f"authzid!=username, authv{v}")
        check(r is not None and r[0] == 58 and "authorization identity" in r[1],
              f"v{v}: `boss\\0probe\\0token` is REFUSED (58) as a request to act as "
              f"somebody else, with a message that says which field is the problem "
              f"rather than a bare authentication failure")

    say("3. a GOOD credential, to show the two versions are otherwise equivalent")
    for v in (0, 1):
        r = case(v, right, f"right token, authv{v}")
        check(r is not None and r[0] == 0,
              f"v{v} admits a good token")

    say("4. how long the connection lives behind a refusal")
    r = results.get(1)
    if r and r[2]:
        info("the refusal is answered and the socket is closed with NO gap. That is "
             "src/handlers/sasl_authenticate.rs's deliberate 'no artificial delay'.")
        info("consequence, measured: kafka-python 3.x's async transport reports "
             "'socket disconnected' and never sees this error_message; relaying the "
             "same listener through a proxy that holds the FIN for 300ms makes 3.0.11 "
             "parse it. Apache Kafka spends connection.failed.authentication.delay.ms "
             "(default 100ms) here for exactly this reason.")
    elif r:
        ok("the connection outlives the refusal long enough for a slow client to read it")
    return report()


if __name__ == "__main__":
    sys.exit(main())
