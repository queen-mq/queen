#!/usr/bin/env python3
# =============================================================================
#  SCRIPTED PLAN SERVER for the broker-free half of the HTTP suite.
#
#  It is not a broker mock and must never become one: it knows nothing about kv,
#  timers or transactions. It does exactly two things, and both exist so that the
#  unit suite can make assertions no live broker can make:
#
#    1. RECORDS THE REQUEST EXACTLY AS IT LEFT THE CLIENT — method, path with its
#       query string, content-type and the body BYTES, one JSON object per line
#       in $PLAN_DIR/requests.jsonl. A live broker parses the body and answers
#       from the parse, so a wire shape that is wrong in a way the broker happens
#       to tolerate (a rider inside `operations` on a broker that also reads the
#       top level, a `ttl` beside a `ttlSeconds`) passes green against it. The
#       recorded bytes are the only place that difference is visible.
#
#    2. REPLIES WITH A CANNED ANSWER read from $PLAN_DIR/reply — first line the
#       status code, everything after the first newline the body. That is what
#       lets the suite pin the commit contract of §8.3: a lost precondition
#       arrives as HTTP 200 with success:false, and the client must RETURN it as
#       a verdict, while every other failure must be raised. Against a real
#       broker only the happy branch is reachable on demand; the 503 branch would
#       need a broken database.
#
#  Requests are handled one at a time (single-threaded, on purpose): the suite
#  sends one request per assertion and then reads the last recorded line, so a
#  concurrent server would introduce a race into the test harness itself.
#
#  It binds 127.0.0.1 only. Nothing here authenticates anything, and it must
#  never be reachable from outside the container/host running the suite.
# =============================================================================
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PLAN_DIR = os.environ.get("PLAN_DIR", ".")
LOG = os.path.join(PLAN_DIR, "requests.jsonl")
REPLY = os.path.join(PLAN_DIR, "reply")


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.1 so curl's `Expect: 100-continue` (which it sends for bodies over
    # 1 KiB) is answered by the base class instead of timing out.
    protocol_version = "HTTP/1.1"

    def _record_and_reply(self):
        n = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(n) if n > 0 else b""
        rec = {
            "method": self.command,
            "path": self.path,
            "contentType": self.headers.get("content-type"),
            # `replace` and not `strict`: a client bug that sends invalid UTF-8
            # must show up as a failed body comparison, not as a crashed harness
            # that reads like an infrastructure problem.
            "body": raw.decode("utf-8", "replace"),
            "bodyLen": len(raw),
        }
        with open(LOG, "a") as f:
            f.write(json.dumps(rec) + "\n")
            f.flush()

        status, body = 200, '{"ok":true}'
        try:
            with open(REPLY) as f:
                canned = f.read()
            head, _, rest = canned.partition("\n")
            status, body = int(head.strip()), rest
        except (OSError, ValueError):
            pass

        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_PUT = do_POST = do_DELETE = do_PATCH = _record_and_reply

    def log_message(self, *args):  # silence; the suite owns stdout
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18632
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
