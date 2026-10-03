# docs:start(app-py-rate-limiter)
#
# A rate limiter built from a streaming query.
#
# The textbook rate limiter counts requests per API key in a fixed window,
# usually with a counter in Redis, which is one more system to run and one
# more place where the count can drift from the requests.
#
# Here the counter is a windowed aggregation over the request queue itself.
# Each cycle of the stream commits the window state, the closed windows it
# emits and the ack of the requests it counted as one entry in the broker's
# log, so the count cannot drift from the requests, and it survives a restart
# of this process because the state is in the broker.
#
#   api-requests (one partition per API key)
#     `-- streaming query: tumbling window, count per key
#           `-- api-usage  -> the gate: over quota becomes a throttle decision
#                 `-- api-throttled
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 rate_limiter.py

import asyncio
import os
import sys
import time

from queen import Queen, Stream

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")

# The names are prefixed per language and suffixed per run, so every application
# in every language can share one broker and two runs never read each other's
# messages.
RUN = f"{int(time.time() * 1000):x}"
REQUESTS = f"app-py-api-requests-{RUN}"
USAGE = f"app-py-api-usage-{RUN}"
THROTTLED = f"app-py-api-throttled-{RUN}"

# The query id is this streaming query's identity in the broker. Its window
# state is keyed by it, so restarting the program with the same id resumes the
# same windows instead of starting new ones.
QUERY_ID = f"app-py-rate-limiter-{RUN}"

WINDOW_SECONDS = 2
QUOTA_PER_WINDOW = 5

# Two tenants. One is a well behaved integration, the other is a runaway script
# someone left in a loop.
QUIET_KEY = "key-quiet"
NOISY_KEY = "key-noisy"
QUIET_REQUESTS = 3
NOISY_REQUESTS = 20

# Why those numbers make the check deterministic: a window is a slice of time,
# so a burst can land on either side of a boundary. Twenty requests split in
# any way at all leave at least ten on one side, which is over a quota of five,
# so the noisy key is always caught. Three requests cannot reach five however
# they are split, so the quiet key is never caught by accident.

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


async def main() -> int:
    # The whole client is async: every call below is awaited, and this is the
    # one event loop they all run on, including the stream's polling task.
    queen = Queen(url=QUEEN_URL)
    stream = None
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        for name in (REQUESTS, USAGE, THROTTLED):
            # The config keys are snake_case in Python and the client converts
            # them to the camelCase the broker expects.
            await queen.queue(name).config({"lease_time": 30, "retry_limit": 3}).create()

        # ------------------------------------------------------------ counter
        #
        # The stream runs in this process, as a consumer group of its own. A
        # new group starts at the tail of the queue unless it asks otherwise,
        # and a request pushed while the stream was still starting would be
        # missed, so it asks for subscription_mode "all": every request in the
        # queue is counted.
        #
        # The partition is the aggregation key, so the window state is per API
        # key without a word about keys here: the producer decides, by
        # partition.
        print("\nstarting the counter")
        stream = await (
            # from_ carries a trailing underscore because `from` is a Python
            # keyword; it is the same entry point as the other clients'.
            Stream.from_(queen.queue(REQUESTS))
            # Tumbling: fixed, non-overlapping windows, one set per partition.
            # A window closes when its time is up; idle_flush_ms also closes one
            # whose partition has gone quiet, which is what lets a short program
            # finish.
            .window_tumbling(seconds=WINDOW_SECONDS, idle_flush_ms=800)
            .aggregate(
                {
                    # The extractors receive the payload itself, without the
                    # envelope around it: the cost is r["cost"].
                    "requests": lambda r: 1,
                    "cost": lambda r: r.get("cost", 1),
                }
            )
            .to(queen.queue(USAGE))
            # run() registers the query and then leaves the polling loop running
            # as an asyncio task. Its options are keyword arguments here, in
            # snake_case.
            .run(
                query_id=QUERY_ID,
                url=QUEEN_URL,
                subscription_mode="all",
                batch_size=200,
                max_partitions=8,
                max_wait_millis=200,
            )
        )

        # ------------------------------------------------------------ traffic
        print("\ntaking traffic")

        async def send(key: str, n: int) -> None:
            for _ in range(n):
                await queen.queue(REQUESTS).partition(key).push(
                    {"data": {"key": key, "path": "/v1/things", "cost": 1, "at": int(time.time() * 1000)}}
                )
            print(f"  {key}: {n} requests")

        await send(QUIET_KEY, QUIET_REQUESTS)
        await send(NOISY_KEY, NOISY_REQUESTS)

        # --------------------------------------------------------------- gate
        #
        # The enforcement point. It reads each closed window and turns the ones
        # over quota into throttle decisions. It is separate from the counter on
        # purpose: the counting is exact and stays the same, while the policy is
        # yours and changes on its own schedule.
        print("\nenforcing")
        counted: dict = {}
        decisions = []

        def complete() -> bool:
            return (
                counted.get(QUIET_KEY, 0) == QUIET_REQUESTS
                and counted.get(NOISY_KEY, 0) == NOISY_REQUESTS
            )

        # One key's burst can fall on either side of a window boundary and
        # arrive as two windows. This adds the windows up per key and waits for
        # the totals it expects, with a deadline. Waiting for a quiet period
        # instead would race the timer that closes the last window.
        deadline = time.monotonic() + 30

        while not complete() and time.monotonic() < deadline:
            windows = await (
                queen.queue(USAGE)
                .group("rate-limiter-gate")
                .subscription_mode("all")
                .batch(50)
                # partitions(10) lets this one call claim up to ten partitions,
                # with batch as the total budget across all of them. Left unset,
                # the broker chooses.
                .partitions(10)
                .wait(True)
                .timeout_millis(2000)
                .pop()
            )

            # This loop pops instead of consuming, so it acks every window
            # itself, and names the group each time: the lease belongs to the
            # group, and an ack without it is refused.
            for w in windows:
                # The window's key is the partition it was computed for.
                key = w["partition"]
                counted[key] = counted.get(key, 0) + w["data"]["requests"]
                over_by = w["data"]["requests"] - QUOTA_PER_WINDOW

                if over_by > 0:
                    # The decision is a message: whatever enforces it (an edge
                    # worker, a gateway, the API itself) reads this queue and
                    # gets the decisions in order, per key. It commits with the
                    # ack of the window it came from, so a crash in between
                    # cannot lose a decision or make two.
                    await (
                        queen.transaction()
                        .queue(THROTTLED)
                        .partition(key)
                        .push(
                            {
                                "data": {
                                    "key": key,
                                    "window": w["data"]["requests"],
                                    "quota": QUOTA_PER_WINDOW,
                                    "overBy": over_by,
                                }
                            }
                        )
                        .ack(w, "completed", {"consumer_group": "rate-limiter-gate"})
                        .commit()
                    )
                    decisions.append({"key": key, "overBy": over_by})
                    print(f"  {key}: {w['data']['requests']} in a window, over by {over_by}")
                else:
                    await queen.ack(w, True, {"group": "rate-limiter-gate"})
                    print(f"  {key}: {w['data']['requests']} in a window, within quota")

        # ----------------------------------------------------------- checking
        print("\nchecking")
        check(complete(), "every request reached a closed window before the deadline")
        check(counted[QUIET_KEY] == QUIET_REQUESTS, "the quiet key was counted exactly")
        check(counted[NOISY_KEY] == NOISY_REQUESTS, "the noisy key was counted exactly")

        check(len(decisions) > 0, "the noisy key was throttled")
        check(
            all(d["key"] == NOISY_KEY for d in decisions),
            "the quiet key was never throttled, so the limiter is not just firing at everything",
        )

        # The decisions are readable by whatever enforces them, in order, per
        # key. No group is named, so this read goes through the queue's own
        # cursor, which starts at the beginning.
        throttled = await queen.queue(THROTTLED).batch(50).partitions(10).wait(True).pop()
        check(
            len(throttled) == len(decisions),
            "every decision is on the queue the gateway reads",
        )
        check(
            all(m["data"]["window"] > m["data"]["quota"] for m in throttled),
            "each decision carries the count and the quota that produced it",
        )

        # stop() cancels the idle-flush timer, waits for the polling loop to
        # finish the cycle it is in, and drains a flush already in flight, so
        # nothing is still writing when the queues go away.
        await stream.stop()
        stream = None

        # Clean up on success only: a failed run leaves the queues on the broker
        # to be looked at.
        for name in (REQUESTS, USAGE, THROTTLED):
            await queen.queue(name).delete()

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        if stream:
            await stream.stop()
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
