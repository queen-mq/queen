# docs:start(tut-py-streaming)
#
# Tutorial 5 of 5: streaming.
#
# The four tutorials before this one move messages. This one aggregates them:
# a tumbling window per entity, whose state, output and acknowledgements commit
# in the same PostgreSQL transaction. That is exactly-once aggregation with no
# changelog topic and no state store to operate, because the state and the
# queue are already in the same database.
#
# A stream is a running process, so the order here is the order you would use
# in production: start it, then let events arrive.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 05_streaming.py

import asyncio
import json
import os
import sys
import time

from queen import Queen, Stream

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
RUN = f"{int(time.time() * 1000):x}"
EVENTS = f"tut-py-stream-events-{RUN}"
TOTALS = f"tut-py-stream-totals-{RUN}"

# The query id is this streaming query's identity in the database. Its window
# state is keyed by it, so restarting the program with the same id resumes the
# same windows instead of starting new ones.
QUERY_ID = f"tut-py-totals-{RUN}"

SALES = [
    {"customer": "acme", "amount": 10},
    {"customer": "acme", "amount": 32.5},
    {"customer": "globex", "amount": 7.25},
    {"customer": "acme", "amount": 0.25},
    {"customer": "globex", "amount": 100},
]

CHECKS = 0


def check(condition: bool, description: str) -> None:
    """Record one verified fact, or abort the run."""
    global CHECKS
    if not condition:
        raise AssertionError(description)
    CHECKS += 1
    print(f"  ok: {description}")


async def main() -> int:
    queen = Queen(url=QUEEN_URL)
    stream = None
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        # Both queues are created up front here, rather than by the first push,
        # so the stream has something to attach to before any event exists. The
        # config keys are snake_case in Python and the client converts them to
        # the camelCase the broker expects.
        await queen.queue(EVENTS).config({"lease_time": 30, "retry_limit": 3}).create()
        await queen.queue(TOTALS).config({"lease_time": 30}).create()

        print("\nstarting the stream")
        stream = await (
            # from_ carries a trailing underscore because `from` is a Python
            # keyword; it is the same entry point as the other clients'.
            Stream.from_(queen.queue(EVENTS))
            # Tumbling: fixed, non-overlapping windows, one set per partition. A
            # window closes when its time is up; idle_flush_ms also closes one
            # whose partition has gone quiet, which is what lets a short program
            # finish.
            .window_tumbling(seconds=2, idle_flush_ms=800)
            # The extractors receive the message payload itself, not the
            # envelope: it is s["amount"], not s["data"]["amount"]. Getting it
            # wrong is at least loud in Python, where the missing key raises
            # KeyError inside the cycle rather than aggregating to zero.
            .aggregate(
                {
                    "count": lambda s: 1,
                    "sum": lambda s: s["amount"],
                    "max": lambda s: s["amount"],
                }
            )
            # Every closed window is pushed here, in the same transaction that
            # commits the window state and acknowledges the inputs it was
            # computed from.
            .to(queen.queue(TOTALS))
            # run() registers the query and then leaves the polling loop running
            # as an asyncio task. Its options are keyword arguments here, in
            # snake_case.
            .run(
                query_id=QUERY_ID,
                url=QUEEN_URL,
                batch_size=100,
                max_partitions=8,
                max_wait_millis=200,
            )
        )

        # run() returns as soon as the query is registered, and the polling task
        # it spawned has not had a turn on the event loop yet: its first poll is
        # what creates the query's consumer group, and a group is created at the
        # tail. Yield long enough for that first poll to reach the broker, or the
        # events pushed below race it and the earliest ones are never seen.
        await asyncio.sleep(0.5)

        print("\npushing sales")
        for sale in SALES:
            # The partition is the aggregation key: window state is per
            # partition, so one customer's totals are computed from that
            # customer's lane alone.
            await queen.queue(EVENTS).partition(sale["customer"]).push({"data": sale})
            print(f"  {sale['customer']} {sale['amount']}")

        print("\ncollecting closed windows")

        # A window is a slice of time, so a customer's sales can fall on either
        # side of a boundary and arrive as two windows instead of one. That is
        # what windowing is, and it is why this adds the windows up per customer
        # instead of expecting exactly one each.
        #
        # The loop waits for the totals it expects, with a deadline. Waiting for
        # a quiet period instead would be a race: the last window closes when its
        # timer says so, not when the reader is tired of waiting.
        totals = {}

        def complete() -> bool:
            return (
                totals.get("acme", {}).get("count") == 3
                and totals.get("globex", {}).get("count") == 2
            )

        deadline = time.monotonic() + 30

        while not complete() and time.monotonic() < deadline:
            closed = await (
                queen.queue(TOTALS)
                .group("tut-py-stream-collector")
                .subscription_mode("all")
                .batch(20)
                .partitions(10)
                .wait(True)
                .timeout_millis(2000)
                .pop()
            )

            for msg in closed:
                # The window's key is the partition it was computed for.
                t = totals.setdefault(msg["partition"], {"count": 0, "sum": 0, "max": 0})
                t["count"] += msg["data"]["count"]
                t["sum"] += msg["data"]["sum"]
                t["max"] = max(t["max"], msg["data"]["max"])
                print(f"  {msg['partition']}: {json.dumps(msg['data'])}")
                # This loop pops rather than consumes, so nothing acks for it.
                # The group has to be named again here: an ack without it moves
                # the queue's own cursor and leaves this group's where it was.
                await queen.ack(msg, True, {"group": "tut-py-stream-collector"})

        check(complete(), "every sale reached a closed window before the deadline")
        check(abs(totals["acme"]["sum"] - 42.75) < 0.001, "acme summed to 42.75 across its windows")
        check(abs(totals["globex"]["sum"] - 107.25) < 0.001, "globex summed to 107.25")
        check(totals["globex"]["max"] == 100, "globex kept its largest single sale")

        await stream.stop()
        stream = None

        await queen.queue(EVENTS).delete()
        await queen.queue(TOTALS).delete()

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        # stop() cancels the polling loop and drains a flush already in flight,
        # so a failed run leaves nothing writing behind its back.
        if stream:
            await stream.stop()
        await queen.close()

    # A failure goes to stderr, like the rest of the set. Flush stdout first so
    # the verdict still lands last when the two are piped into one file.
    sys.stdout.flush()
    print(verdict, file=sys.stderr if failed else sys.stdout)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
# docs:end
