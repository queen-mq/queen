# docs:start(app-py-saga)
#
# A booking saga whose compensation is a timer, and whose every step is a row in
# the same PostgreSQL as the queue.
#
# The war story is a room hold that never came back. A booking system held
# inventory when a reservation started and released it when the payment either
# settled or failed, and the release lived in a sleeping task inside the worker.
# A rolling deploy replaced the workers; every hold in flight lost its release;
# and a fortnight later somebody noticed a hotel had been sold out on paper for
# nine nights it had spent empty.
#
# The release was not slow, it was in the wrong place. A compensation is not a
# timeout, it is an obligation, and an obligation has to outlive the process
# that took it on. Here the gate, the saga state, the compensation timer, the
# payment request and the acknowledgement are ONE PostgreSQL transaction. If the
# room is held, the compensation exists. If the room is not held, nothing else
# happened either.
#
#   bookings
#     `-- group "reserver"    ONE bundle: gate + state + timer + push + ack
#           |-- payments (partitioned by booking)
#           |     `-- group "payer"    confirm + CANCEL the timer + ack, one bundle
#           `-- expiries (delivered by the timer, at the hold's expiry)
#                 `-- group "compensator"   reads the state BEFORE compensating
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 saga.py

import asyncio
import os
import sys
import time
from datetime import timedelta

from queen import Queen

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")

# A suffix on the queue names AND on the KV namespace. The queues need it
# because delete-then-recreate leaves stale partition state for up to 30
# seconds; the namespace needs it for the opposite reason -- a saga row outlives
# the run that wrote it, so a second run under the same namespace would find
# every booking already held and measure nothing.
RUN = f"{int(time.time() * 1000):x}"
BOOKINGS = f"app-py-saga-bookings-{RUN}"
PAYMENTS = f"app-py-saga-payments-{RUN}"
EXPIRIES = f"app-py-saga-expiries-{RUN}"
NS = f"app-py-saga-{RUN}"

# How long a room stays held before the compensation fires. It has to outlast
# the reserve and pay phases, or a timer would fire before the payment that
# cancels it and the run would be measuring a race rather than a design.
# deliverAt is a floor and never a ceiling, so a timer can only be late: a
# margin here is sound, where a margin the other way would not be.
HOLD_MS = 15000

# Every phase below ends on a COUNT, and this is the deadline behind the count.
# Never wait for silence: a phase that stops when nothing has arrived for a
# while passes on a broker that delivered nothing at all.
PHASE_MS = 20000

# The compensation phase gets its own, longer deadline. A timer fires no earlier
# than its delay plus one sweeper cycle, and a broker whose timer table has been
# empty for a while wakes up lazily.
TIMER_DEADLINE_MS = 90000

# Four bookings, five submissions. B-2 is submitted twice: the same booking, two
# messages, which is what a redelivery looks like from the reserver's side and
# the reason the bundle opens with a gate rather than with a check.
BOOKINGS_IN = [
    {"bookingId": "B-1", "room": "101", "cents": 24000},
    {"bookingId": "B-2", "room": "102", "cents": 31000},
    {"bookingId": "B-2", "room": "102", "cents": 31000},
    {"bookingId": "B-3", "room": "103", "cents": 18000},
    {"bookingId": "B-4", "room": "104", "cents": 27000},
]
BOOKING_IDS = ["B-1", "B-2", "B-3", "B-4"]

# B-3's card is declined, so its saga never reaches "confirmed" and the timer is
# the thing that gives the room back.
DECLINED = "B-3"

# B-4 pays, but its cancel is deliberately skipped, which makes the race
# deterministic: a cancel that arrives after the fire answers `absent`, and
# ABSENT MAY MEAN ALREADY DELIVERED. So the compensation for a confirmed booking
# has to be refused by the consumer that receives it, never prevented by the
# cancel alone.
CANCEL_SKIPPED = "B-4"

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


def saga_key(booking_id: str) -> str:
    """The saga's state key. It derives from the booking id, which is also the
    partition key of the payments queue: that is what makes the payer's
    read-then-write safe, and it is stated here because it is a property of the
    naming and nothing enforces it."""
    return f"saga:{booking_id}"


async def main() -> int:
    global CHECKS
    queen = Queen(url=QUEEN_URL)

    reserve_decisions: list = []
    payments_requested: list = []
    compensations_delivered: list = []
    rooms_released: list = []
    compensations_refused: list = []
    preconditions_lost = 0
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        for queue in (BOOKINGS, PAYMENTS, EXPIRIES):
            await queen.queue(queue).config({"lease_time": 30, "retry_limit": 3}).create()

        # ------------------------------------------------------------ queuing
        print("\nsubmitting bookings")
        for index, booking in enumerate(BOOKINGS_IN):
            await queen.queue(BOOKINGS).push(
                {
                    # Distinct transaction ids on purpose. Deduplication would
                    # swallow the duplicate submission and the gate would never
                    # be tested, and a real redelivery arrives with an identity
                    # of its own too.
                    "transactionId": f"submit-{index}-{booking['bookingId']}",
                    "data": booking,
                }
            )
        print(f"  {len(BOOKINGS_IN)} submissions for {len(BOOKING_IDS)} bookings")

        # ---------------------------------------------------------- reserving
        #
        # The bundle, and the whole point of the example: five things commit
        # together, so there is no ordering between them left to get wrong.
        print("\nreserving")

        async def reserve(msg) -> None:
            nonlocal preconditions_lost
            booking = msg["data"]
            booking_id, room, cents = booking["bookingId"], booking["room"], booking["cents"]
            group = msg.get("consumerGroup")

            tx = queen.transaction()
            # 1. The gate AND the first state, in one row. required=True is what
            #    makes it a gate instead of a verdict: without it a lost race
            #    would come back applied=False while the payment and the timer
            #    went out anyway.
            tx.kv.put_if_absent(
                NS,
                saga_key(booking_id),
                {"step": "held", "room": room, "cents": cents},
                # The Python client takes a timedelta where the JavaScript one
                # takes "1h"; both resolve to the one field the wire has,
                # ttlSeconds.
                ttl=timedelta(hours=1),
                required=True,
            )
            # 2. The obligation. From the moment this commits it is a row in the
            #    broker's own table, so it survives this handler, this process,
            #    this deploy and this machine. The key is chosen by us, which is
            #    the entire reason it can be cancelled later by name.
            tx.timer(EXPIRIES).key(booking_id).after_ms(HOLD_MS).payload(
                {"bookingId": booking_id, "room": room}
            ).schedule()
            # 3. The work. Partitioned by booking, so every message about one
            #    booking is in one lane.
            tx.queue(PAYMENTS).partition(booking_id).push(
                {"transactionId": f"pay-{booking_id}", "data": {"bookingId": booking_id, "cents": cents}}
            )
            # 4. The acknowledgement, carrying this delivery's lease. An expired
            #    lease refuses the ack and takes the other three down with it,
            #    which is the guarantee no compare-and-swap can give.
            res = await tx.ack(msg, "completed", {"consumer_group": group}).commit()

            reserve_decisions.append(booking_id)

            # A lost gate is RETURNED, not raised: HTTP 200, success=False,
            # reason "kv_precondition". It is the ordinary outcome of every
            # legitimate redelivery, which makes it one of the most frequent
            # answers this product gives, and it does not belong in an except
            # block where the reflex is to retry.
            if res.get("success") is False and res.get("reason") == "kv_precondition":
                # Nothing was written: no second payment, no second timer, no
                # second row. The message still has to leave the cursor, so it
                # is acknowledged on its own.
                preconditions_lost += 1
                await queen.ack(msg, "completed", {"group": group})
                print(f"  {booking_id}: already held, whole bundle rolled back ({res.get('kvReason')})")
                return

            print(f"  {booking_id}: room {room} held, compensation armed for {HOLD_MS} ms")

        await (
            queen.queue(BOOKINGS)
            .group("reserver")
            .subscription_mode("all")
            .auto_ack(False)
            .each()
            # The count that ends the phase, with the deadline behind it.
            .limit(len(BOOKINGS_IN))
            .idle_millis(PHASE_MS)
            .consume(reserve)
        )

        check(
            len(reserve_decisions) == len(BOOKINGS_IN),
            f"the reserver reached a decision on every submission ({len(BOOKINGS_IN)}, got {len(reserve_decisions)})",
        )
        check(preconditions_lost == 1, "the duplicate submission lost the gate exactly once")

        # Pending timers are a table you can read, not a promise you have to
        # trust.
        armed = await queen.timers.list(EXPIRIES, limit=50)
        print(f"  timers armed: {', '.join(sorted(row['timerKey'] for row in armed['rows']))}")
        check(
            len(armed["rows"]) == len(BOOKING_IDS),
            f"one compensation is armed per booking and the duplicate added none "
            f"({len(BOOKING_IDS)}, got {len(armed['rows'])})",
        )

        # ------------------------------------------------------------- paying
        #
        # The other end of the saga. A settled payment confirms the state and
        # calls the compensation off in one commit; a declined card leaves the
        # state where it is and lets the timer do its work.
        print("\npaying")

        async def pay(msg) -> None:
            booking_id = msg["data"]["bookingId"]
            group = msg.get("consumerGroup")
            payments_requested.append(booking_id)

            # A read in one call and a write in the next. It is safe HERE
            # because the key derives from the partition key: every message
            # about this booking arrives in one lane of this queue, and a lane
            # has one reader per group. Where a key does not derive from the
            # partition key this shape is a race and the atomics are the answer,
            # which is exactly the compensator's situation further down.
            state = await queen.kv.get(NS, saga_key(booking_id))

            if booking_id == DECLINED:
                # A declined card is a business outcome, not a delivery failure:
                # the message is done with. The room stays held, and nothing in
                # this process is responsible for giving it back.
                await queen.ack(msg, "completed", {"group": group})
                print(f"  {booking_id}: card declined, hold left to expire")
                return

            tx = queen.transaction()
            # `expect` makes the serialisation assumption falsifiable instead of
            # silent. If the lane really serialises, it never fails and costs
            # nothing; the day it fails, two consumers are serving one partition
            # and you learn it as a verdict rather than as a wrong total.
            tx.kv.put(
                NS,
                saga_key(booking_id),
                {**state["value"], "step": "confirmed"},
                ttl=timedelta(hours=1),
                expect=state["version"],
                required=True,
            )

            if booking_id != CANCEL_SKIPPED:
                # The cancel rides the bundle. Either the booking is confirmed
                # and the compensation is called off, or neither happened.
                tx.timer(EXPIRIES).key(booking_id).cancel()

            res = await tx.ack(msg, "completed", {"consumer_group": group}).commit()
            if res.get("success") is False:
                raise AssertionError(f"{booking_id}: confirmation lost its fence ({res.get('kvReason')})")

            tail = (
                ", compensation deliberately NOT cancelled"
                if booking_id == CANCEL_SKIPPED
                else ", compensation cancelled"
            )
            print(f"  {booking_id}: paid and confirmed{tail}")

        await (
            queen.queue(PAYMENTS)
            .group("payer")
            .subscription_mode("all")
            .auto_ack(False)
            .each()
            .limit(len(BOOKING_IDS))
            .idle_millis(PHASE_MS)
            .consume(pay)
        )

        check(
            len(payments_requested) == len(BOOKING_IDS),
            f"every booking was asked to pay once and the duplicate produced no second payment "
            f"({len(BOOKING_IDS)}, got {len(payments_requested)})",
        )
        check(len(set(payments_requested)) == len(payments_requested), "no booking was asked to pay twice")

        # The cancel is observable before anything is delivered: the row is gone
        # from the staging table. A peek is how you ask, and a miss is
        # {"found": false} with HTTP 200, never a 404.
        peeked = {b: (await queen.timers.peek(EXPIRIES, b))["found"] for b in BOOKING_IDS}
        check(peeked["B-1"] is False, "the compensation cancelled inside the confirming bundle is gone from the table")
        check(peeked[DECLINED] is True, f"{DECLINED} was never confirmed, so its compensation is still armed")
        check(
            peeked[CANCEL_SKIPPED] is True,
            f"{CANCEL_SKIPPED} is confirmed but its compensation is still armed on purpose",
        )

        # ------------------------------------------------------- compensating
        #
        # What the timers deliver, and the consumer that must not trust them.
        #
        # A compensation message is not an instruction, it is a question: is
        # this saga still open? A fired timer leaves no tombstone, so a cancel
        # that arrives a millisecond late answers `absent` and the message goes
        # out anyway. The state is the authority and it is read first.
        #
        # And here the key does NOT derive from the partition key: this message
        # arrives on another queue entirely, in a lane that has nothing to do
        # with the payments lane, so no partitioning could serialise the two
        # writers. That is what `expect` is for, and on this path it is
        # load-bearing rather than an assertion.
        print("\ncompensating")

        async def compensate_one(msg) -> None:
            booking_id, room = msg["data"]["bookingId"], msg["data"]["room"]
            group = msg.get("consumerGroup")
            compensations_delivered.append(booking_id)

            state = await queen.kv.get(NS, saga_key(booking_id))

            if not state["found"] or state["value"]["step"] != "held":
                # The booking was confirmed before this fired. Compensating here
                # is how a saga unwinds a sale that has already shipped.
                compensations_refused.append(booking_id)
                await queen.ack(msg, "completed", {"group": group})
                step = state["value"]["step"] if state["found"] else "gone"
                print(f"  {booking_id}: state is {step}, compensation refused")
                return

            res = await (
                queen.transaction()
                .kv.put(
                    NS,
                    saga_key(booking_id),
                    {**state["value"], "step": "expired"},
                    ttl=timedelta(hours=1),
                    expect=state["version"],
                    required=True,
                )
                .ack(msg, "completed", {"consumer_group": group})
                .commit()
            )

            if res.get("success") is False:
                # Somebody confirmed it between the read and the commit. The
                # fence held, nothing was written, and the room stays sold.
                compensations_refused.append(booking_id)
                await queen.ack(msg, "completed", {"group": group})
                print(f"  {booking_id}: confirmed under us, compensation refused by the fence")
                return

            rooms_released.append(room)
            print(f"  {booking_id}: hold expired, room {room} released")

        def compensator(limit: int, idle_millis: int):
            return (
                queen.queue(EXPIRIES)
                .group("compensator")
                .subscription_mode("all")
                .auto_ack(False)
                .each()
                .limit(limit)
                .idle_millis(idle_millis)
                .consume(compensate_one)
            )

        # Two timers were left armed, so two messages must arrive: that is the
        # count, and TIMER_DEADLINE_MS is the deadline behind it.
        await compensator(2, TIMER_DEADLINE_MS)
        check(
            len(compensations_delivered) == 2,
            f"both uncancelled compensations were delivered (2, got {len(compensations_delivered)}"
            f"{': ' + ', '.join(compensations_delivered) if compensations_delivered else ''})",
        )

        # Then a bounded second pass with room for two more. It is the only
        # honest way to say "a cancelled timer never arrived": the first pass
        # would have stopped at two whatever those two were, so the claim is
        # really that nothing else shows up afterwards.
        await compensator(2, 4000)

        # ----------------------------------------------------------- checking
        print("\nchecking")

        check(
            len(compensations_delivered) == 2,
            f"nothing else arrived on a second pass: still 2 compensations (got {len(compensations_delivered)})",
        )
        check(
            "B-1" not in compensations_delivered and "B-2" not in compensations_delivered,
            "a cancelled compensation was never delivered",
        )
        check(
            rooms_released == ["103"],
            f"exactly one room went back on sale, the one whose card was declined "
            f"(got {', '.join(rooms_released) or 'none'})",
        )
        check(
            compensations_refused == [CANCEL_SKIPPED],
            "the compensation for the confirmed booking was refused by the consumer, not prevented by the cancel",
        )

        states = await queen.kv.get_many(NS, [saga_key(b) for b in BOOKING_IDS])
        check(
            len(states["rows"]) == len(BOOKING_IDS) and len(states["missing"]) == 0,
            f"every booking left exactly one saga row ({len(BOOKING_IDS)}, got {len(states['rows'])})",
        )

        step = {row["key"].replace("saga:", ""): row["value"]["step"] for row in states["rows"]}
        check(
            step["B-1"] == "confirmed" and step["B-2"] == "confirmed",
            "the two ordinary bookings ended confirmed",
        )
        check(
            step[CANCEL_SKIPPED] == "confirmed",
            f"{CANCEL_SKIPPED} is still confirmed after its compensation was delivered",
        )
        check(step[DECLINED] == "expired", f"{DECLINED} was unwound by its timer, with nobody awake to do it")

        print("\n  final: " + ", ".join(f"{k}={v}" for k, v in sorted(step.items())))

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:  # noqa: BLE001 - the program's verdict is its exit code
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        # ---------------------------------------------------------- purge
        #
        # Three things to remove, and the first two are the ones that are easy
        # to forget. The saga rows live in their own table, and a pending timer
        # lives in the staging table keyed by NAME: neither is reached by
        # deleting the queue, and a timer whose queue no longer exists still
        # fires and provisions it again on the way out.
        #
        # Unconditional, in a finally, because a run that FAILED is exactly the
        # run whose leftovers matter: an armed timer would deliver into the next
        # run and a surviving saga row would make the next run pass without
        # holding anything.
        #
        # Best effort: a purge that raised would replace the real verdict with
        # its own.
        try:
            for booking_id in BOOKING_IDS:
                await queen.timers.cancel(EXPIRIES, booking_id)
                await queen.kv.delete(NS, saga_key(booking_id))
            for queue in (BOOKINGS, PAYMENTS, EXPIRIES):
                await queen.queue(queue).delete()
        except Exception as err:  # noqa: BLE001 - the run's verdict outranks this
            print(f"  (purge incomplete: {err})")
        await queen.close()

    sys.stdout.flush()
    print(verdict, file=sys.stderr if failed else sys.stdout)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
# docs:end
