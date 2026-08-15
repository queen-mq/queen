# docs:start(tut-py-replay)
#
# Tutorial 4 of 5: replay.
#
# Acknowledging a message does not delete it. Consumption is a cursor per
# consumer group, and the messages stay until retention removes them, so a new
# group can read the whole history and an existing group can be moved back.
#
# This is the tutorial that shows what a cursor buys you: reprocessing after a
# bug, backfilling a new consumer, and auditing what was delivered, all without
# asking the producer to send anything twice.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 04_replay.py

import asyncio
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from queen import Queen

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
RUN = f"{int(time.time() * 1000):x}"
EVENTS = f"tut-py-replay-{RUN}"

EVENTS_IN = [
    {"seq": 1, "type": "created"},
    {"seq": 2, "type": "updated"},
    {"seq": 3, "type": "shipped"},
    {"seq": 4, "type": "delivered"},
]

CHECKS = 0


def check(condition: bool, description: str) -> None:
    """Record one verified fact, or abort the run."""
    global CHECKS
    if not condition:
        raise AssertionError(description)
    CHECKS += 1
    print(f"  ok: {description}")


async def drain(queen: Queen, group: str, expected: int, mode: Optional[str] = None) -> list:
    """Read one lane with one group, and report the sequence numbers seen."""
    seen = []

    builder = (
        queen.queue(EVENTS)
        .partition("order-1")
        .group(group)
        .each()
        .limit(expected)
        .idle_millis(4000)
    )
    # Every builder method mutates and returns the same builder, so a mode that
    # is only sometimes wanted can be applied on its own line.
    if mode:
        builder.subscription_mode(mode)

    async def collect(msg):
        seen.append(msg["data"]["seq"])

    await builder.consume(collect)
    return seen


async def main() -> int:
    queen = Queen(url=QUEEN_URL)
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        for event in EVENTS_IN:
            await queen.queue(EVENTS).partition("order-1").push({"data": event})
        print(f"pushed {len(EVENTS_IN)} events")

        # The live consumer. It drains the lane and commits as it goes.
        print("\nthe live consumer")
        live = await drain(queen, "tut-py-live", 4, "all")
        print(f"  saw {', '.join(str(n) for n in live)}")
        check(live == [1, 2, 3, 4], "the live group read the lane in order")

        # A second group, created now, after every message was already stored and
        # acknowledged by someone else. subscription_mode("all") is what points
        # its new cursor at the beginning: the default for a new group is the
        # tail, so without it this group would sit idle waiting for the next
        # event.
        #
        # The mode applies when the cursor is created and never again, so it
        # cannot rewind a group that already exists. That is what seek below is
        # for.
        print("\na new group, backfilled from the beginning")
        audit = await drain(queen, "tut-py-audit", 4, "all")
        print(f"  saw {', '.join(str(n) for n in audit)}")
        check(audit == [1, 2, 3, 4], "a new group replayed the whole history")

        # Nothing was re-pushed and nothing was copied: both groups read the same
        # stored messages through their own cursors.
        print("\nrewinding an existing group")

        # Move the live group's cursor back an hour, which is before anything in
        # this run was pushed. The seek also releases any live lease, so an
        # in-flight batch is abandoned rather than acknowledged.
        #
        # The broker takes any ISO 8601 instant, so the "+00:00" that Python's
        # isoformat() writes is as good as the "Z" the other clients send. What
        # it will not do is guess a timezone: a naive datetime.now() is read as
        # UTC, so from any zone ahead of it the seek lands in the future and
        # skips the history instead of rewinding into it, silently. Making the
        # datetime timezone-aware is the whole trick.
        an_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        await queen.admin.seek_consumer_group(
            "tut-py-live",
            EVENTS,
            {"timestamp": an_hour_ago.isoformat()},
        )

        replayed = await drain(queen, "tut-py-live", 4)
        print(f"  saw {', '.join(str(n) for n in replayed)}")
        check(
            replayed == [1, 2, 3, 4],
            "the rewound group read the same events again, in the same order",
        )

        # Replay is per group. The audit group was not moved, so it stays where
        # it was and sees nothing new.
        audit_again = await (
            queen.queue(EVENTS)
            .partition("order-1")
            .group("tut-py-audit")
            .batch(10)
            .wait(False)
            .pop()
        )
        check(len(audit_again) == 0, "rewinding one group left the other where it was")

        await queen.queue(EVENTS).delete()

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        await queen.close()

    # A failure goes to stderr, like the rest of the set. Flush stdout first so
    # the verdict still lands last when the two are piped into one file.
    sys.stdout.flush()
    print(verdict, file=sys.stderr if failed else sys.stdout)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
# docs:end
