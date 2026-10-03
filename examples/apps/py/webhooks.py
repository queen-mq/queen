# docs:start(app-py-webhooks)
#
# A webhook sender: ordered per endpoint, retried by the broker, and
# dead-lettered with its error when it never succeeds.
#
# Deliveries to one endpoint have to arrive in the order the events happened,
# an endpoint that is down must not slow anybody else down, a failure is
# retried a bounded number of times, and what never succeeds has to end up
# somewhere a person can read it. Each endpoint gets a partition of its own,
# created by its first delivery, so a dead endpoint backs up its own partition
# and nothing else. Retries are the broker's retry budget, and an exhausted
# delivery lands in the dead-letter queue with the error attached.
#
#   webhook-deliveries (one partition per endpoint)
#     `-- group "sender"  POSTs each delivery; a raise spends one retry
#           `-- retry_limit spent -> dead-letter queue, with the error
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 webhooks.py

import asyncio
import os
import sys
import time

from queen import Queen

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")

# The name is prefixed per language and suffixed per run, so every application
# in every language can share one broker and two runs never read each other's
# messages.
RUN = f"{int(time.time() * 1000):x}"
DELIVERIES = f"app-py-webhooks-{RUN}"
GROUP = "sender"

# Three subscribers. One of them answers 500 to everything. It is listed first,
# so its deliveries are the oldest in the queue and its partition is usually
# handed out first: a sender that let a failing endpoint hold up the others
# would fail the checks below.
ENDPOINTS = {
    "initech.example": {"healthy": False},
    "acme.example": {"healthy": True},
    "globex.example": {"healthy": True},
}
EVENTS_PER_ENDPOINT = 3
RETRY_LIMIT = 2

CHECKS = 0


def check(condition: bool, description: str) -> None:
    """Record one verified fact, or abort the run.

    This raises instead of using the `assert` statement, because `python3 -O`
    removes `assert` and the checks are the whole point of the program.
    """
    global CHECKS
    if not condition:
        raise AssertionError(description)
    CHECKS += 1
    print(f"  ok: {description}")


async def post_to_endpoint(endpoint: str, event: dict) -> dict:
    """Stands in for the HTTP POST to the subscriber.

    A real sender calls httpx and raises on any status that is not 2xx, which
    is what this does.
    """
    if not ENDPOINTS[endpoint]["healthy"]:
        raise RuntimeError(f"{endpoint} answered 500")
    return {"status": 200}


async def main() -> int:
    # The whole client is async: every call below is awaited, and this is the
    # one event loop they all run on. Unlike the JavaScript client there is no
    # handleSignals switch, so SIGINT and SIGTERM are always handled for you.
    queen = Queen(url=QUEEN_URL)
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        # retry_limit is the delivery budget: a delivery that fails
        # RETRY_LIMIT + 1 times is filed in the dead-letter queue with its last
        # error, because dlq_after_max_retries is on. lease_time is how long
        # the broker waits for a sender that took a delivery and never came
        # back before it hands the delivery to another sender. The config keys
        # are snake_case in Python and the client converts them to the
        # camelCase the broker expects.
        await queen.queue(DELIVERIES).config(
            {"lease_time": 30, "retry_limit": RETRY_LIMIT, "dlq_after_max_retries": True}
        ).create()

        # ------------------------------------------------------------ queuing
        #
        # The application emits events. Each delivery goes into the partition
        # of the endpoint it is for, which is what makes "in order per
        # subscriber" a property of the storage instead of something the
        # sender has to arrange.
        print("\nqueuing deliveries")
        for seq in range(1, EVENTS_PER_ENDPOINT + 1):
            for endpoint in ENDPOINTS:
                await queen.queue(DELIVERIES).partition(endpoint).push(
                    {
                        # The event id. An application that retries its own
                        # emit does not create a second delivery. The key is
                        # camelCase because it is the broker's wire name.
                        "transactionId": f"{endpoint}-evt-{seq}",
                        "data": {
                            "endpoint": endpoint,
                            "seq": seq,
                            "type": "invoice.paid",
                            "invoiceId": f"INV-{seq}",
                        },
                    }
                )
        print(f"  {EVENTS_PER_ENDPOINT * len(ENDPOINTS)} deliveries queued")

        # ------------------------------------------------------------ sending
        #
        # The sender pool. auto_ack is on by default, so a handler that returns
        # acknowledges the delivery and a handler that raises gives it back
        # with the error: the broker redelivers it and counts one retry, and
        # msg["deliveryAttempt"] says which attempt this is. The retries live
        # in the broker, so they survive the sender dying halfway, which a
        # retry loop inside the handler would not.
        #
        # partitions(1): every pop takes ONE endpoint. After a failed delivery
        # the client skips the rest of that pop, so deliveries to other
        # endpoints that came in the same pop would wait for their lease to
        # run out.
        print("\nsending")
        delivered_to: dict = {}
        attempts: dict = {}

        async def send(msg) -> None:
            endpoint = msg["data"]["endpoint"]
            seq = msg["data"]["seq"]
            attempts[endpoint] = attempts.get(endpoint, 0) + 1
            try:
                await post_to_endpoint(endpoint, msg["data"])
            except Exception as err:
                print(f"  {endpoint} <- event {seq} failed on attempt {msg['deliveryAttempt']}: {err}")
                raise
            delivered_to.setdefault(endpoint, []).append(seq)
            print(f"  {endpoint} <- event {seq}")

        await (
            queen.queue(DELIVERIES)
            .group(GROUP)
            # A group created after the messages were pushed starts at the tail,
            # so without this it would see nothing.
            .subscription_mode("all")
            .concurrency(3)
            .partitions(1)
            .each()
            # Long polls end after a second and a sender stops after three
            # quiet seconds, so the program finishes. A service runs without
            # these two.
            .timeout_millis(1000)
            .idle_millis(3000)
            .consume(send)
        )

        # ----------------------------------------------------------- checking
        print("\nchecking")

        for endpoint, meta in ENDPOINTS.items():
            if not meta["healthy"]:
                continue
            seqs = delivered_to.get(endpoint, [])
            got = ",".join(map(str, seqs)) or "none"
            check(
                len(seqs) == EVENTS_PER_ENDPOINT,
                f"{endpoint} received its {EVENTS_PER_ENDPOINT} events (got {len(seqs)})",
            )
            check(seqs == [1, 2, 3], f"{endpoint} received them in the order they happened (got {got})")

        check("initech.example" not in delivered_to, "the dead endpoint received nothing")
        check(
            attempts.get("initech.example", 0) == EVENTS_PER_ENDPOINT * (RETRY_LIMIT + 1),
            f"each dead delivery was tried {RETRY_LIMIT + 1} times before it was given up "
            f"({attempts.get('initech.example', 0)} attempts)",
        )

        # The dead letters are records you can query. Each one keeps the
        # payload, so it names the endpoint and the invoice, and the last
        # error, which is what answers "why did this customer not get the
        # webhook". They come back as plain dicts, with the payload under
        # "data" and the error under the broker's wire name, "errorMessage".
        dlq = await queen.queue(DELIVERIES).dlq().limit(50).get()
        dead = [m for m in dlq["messages"] if m["data"]["endpoint"] == "initech.example"]

        check(
            len(dead) == EVENTS_PER_ENDPOINT,
            f"all {EVENTS_PER_ENDPOINT} dead deliveries are in the dead-letter queue",
        )
        check(
            all("answered 500" in (m.get("errorMessage") or "") for m in dead),
            "each dead letter carries the error that killed it",
        )
        check(
            len(dlq["messages"]) == len(dead),
            "no healthy delivery ended up in the dead-letter queue",
        )

        listing = "; ".join(
            f"{m['data']['endpoint']}/{m['data']['invoiceId']}: {m.get('errorMessage')}" for m in dead
        )
        print(f"\n  dead letters: {listing}")

        # Clean up on success only: a failed run leaves the queue on the broker
        # to be looked at.
        await queen.queue(DELIVERIES).delete()

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        # close() flushes the client-side buffers and closes the HTTP pool. It
        # narrates its own shutdown on stdout, which is why the verdict is
        # printed after it: PASS or FAIL stays the last line of a run.
        await queen.close()

    # A failure goes to stderr, like the rest of the set. Flush stdout first so
    # the verdict still lands last when the two are piped into one file.
    sys.stdout.flush()
    print(verdict, file=sys.stderr if failed else sys.stdout)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
# docs:end
