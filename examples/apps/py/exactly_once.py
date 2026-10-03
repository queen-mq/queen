# docs:start(app-py-exactly-once)
#
# Charging each order exactly once, when orders can be delivered twice.
#
# Any consumer that charges cards sees some orders twice: a lease runs out
# while the card network is slow, a node restarts, someone replays the queue.
# The usual guard is an "already charged" flag in a database next to the
# broker, written after the charge and before the ack. The flag and the ack
# then commit separately, and in the gap between them the work can happen
# twice.
#
# Here the flag is a KV entry in the broker itself, written in the same
# transaction as the ack. The order is marked and acknowledged together, or
# neither happens, and a redelivery finds the marker and charges nothing.
#
#   orders
#     |-- group "charger"  marker + ack in ONE transaction
#     `-- group "replay"   reads every order again and must charge nothing
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 exactly_once.py

import asyncio
import os
import sys
import time
from datetime import timedelta

from queen import Queen

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")

# A fresh queue and a fresh KV namespace per run. The namespace needs it more
# than the queue does: markers outlive the queue that produced them (they
# expire with their TTL), so a second run in the same namespace would find
# every order already charged and pass without charging anything.
RUN = f"{int(time.time() * 1000):x}"
ORDERS = f"app-py-exactly-once-{RUN}"
NS = f"app-py-exactly-once-{RUN}"
GROUP = "charger"
REPLAY_GROUP = "replay"

# Five orders. The first attempt at ORD-3 fails before it charges anything, the
# failure that must leave no trace at all.
ORDER_IDS = ["ORD-1", "ORD-2", "ORD-3", "ORD-4", "ORD-5"]
CRASHING_ORDER = "ORD-3"

# The charging phase ends after six deliveries, five orders plus the retry of
# ORD-3, and the deadline is there so that a stall fails the run instead of
# hanging it.
CHARGE_DELIVERIES = len(ORDER_IDS) + 1
PHASE_MS = 30000

CHECKS = 0

# The external effect. Each entry is a charge on a real card, and the ledger is
# what the customers' statements would show.
LEDGER: list = []
ATTEMPTS: dict = {}


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


def charge_card(order: dict) -> str:
    charge_id = f"ch_{order['orderId']}_{len(LEDGER)}"
    LEDGER.append({"orderId": order["orderId"], "chargeId": charge_id, "cents": order["cents"]})
    return charge_id


def marker_key(order_id: str) -> str:
    return f"charge:{order_id}"


async def main() -> int:
    queen = Queen(url=QUEEN_URL)
    verdict, failed = "", False

    # One order per pop (batch(1)). A failed ack gives up the partition's lease,
    # so in a batch of several orders every order after a failed one would be
    # charged, refused at its commit, and charged again when it came back.
    def charger(group: str, limit: int):
        return (
            queen.queue(ORDERS)
            .group(group)
            .subscription_mode("all")
            # The ack rides the transaction.
            .auto_ack(False)
            .batch(1)
            .each()
            .limit(limit)
            .idle_millis(PHASE_MS)
        )

    try:
        print(f"broker {QUEEN_URL}")

        await queen.queue(ORDERS).config({"lease_time": 30, "retry_limit": 5}).create()

        print("\nqueuing orders")
        for index, order_id in enumerate(ORDER_IDS):
            await queen.queue(ORDERS).push(
                {"transactionId": f"order-{order_id}", "data": {"orderId": order_id, "cents": 1000 + index}}
            )
        print(f"  {len(ORDER_IDS)} orders queued")

        # ----------------------------------------------------------- charging
        #
        # handle() is the whole pattern: four steps, in this order. It returns
        # True when this delivery charged the card and False when it found the
        # order already charged. Every redelivery, whatever caused it, has to
        # come back False.
        observed: list = []

        async def handle(msg) -> bool:
            order = msg["data"]
            order_id = order["orderId"]
            ATTEMPTS[order_id] = ATTEMPTS.get(order_id, 0) + 1
            group = msg.get("consumerGroup")

            # 1. Was this order charged already? "found" is its own key
            #    because None is a value you can store.
            marker = await queen.kv.get(NS, marker_key(order_id))
            if marker["found"]:
                # Nothing to do, but the message still has to leave this
                # group's cursor, or it comes back forever.
                await queen.ack(msg, "completed", {"group": group})
                return False

            # 2. Everything that can fail without touching the card goes here,
            #    before the charge: validation, lookups. This is where ORD-3
            #    fails, once.
            if order_id == CRASHING_ORDER and ATTEMPTS[order_id] == 1:
                raise RuntimeError("card network timed out")

            # 3. The external effect.
            charge_id = charge_card(order)

            # 4. The marker and the ack, in ONE transaction.
            #
            #    required=True turns the put_if_absent into a gate. If the
            #    marker already exists (another delivery of this order got here
            #    first), the whole transaction rolls back, ack included, and
            #    only the winner's ack lands. tx.once(...) is the same gate
            #    under a shorter name; it is spelled out here so that required
            #    is visible.
            #
            #    The ack carries this delivery's lease. If the lease ran out
            #    while the card was being charged, the ack is refused and the
            #    marker with it: the broker answers with reason rejected_ack
            #    and commit() raises. A compare-and-set on the marker could not
            #    do that: a version that still matches succeeds for a worker
            #    that no longer owns the message.
            res = await (
                queen.transaction()
                .kv.put_if_absent(
                    NS,
                    marker_key(order_id),
                    {"chargeId": charge_id, "cents": order["cents"]},
                    # The Python client takes a timedelta where the JavaScript
                    # client takes "1h"; both become ttlSeconds on the wire.
                    ttl=timedelta(hours=1),
                    required=True,
                )
                .ack(msg, "completed", {"consumer_group": group})
                .commit()
            )

            # A lost gate comes back as a value (HTTP 200, success False,
            # reason "kv_precondition"). It is the normal outcome of a
            # duplicate delivery, so it is handled here and kept out of the
            # error path, where a retry would be the reflex.
            if res.get("success") is False and res.get("reason") == "kv_precondition":
                await queen.ack(msg, "completed", {"group": group})
                return False

            return True

        print("\ncharging")

        async def charge(msg) -> None:
            group = msg.get("consumerGroup")
            try:
                ran = await handle(msg)
                observed.append({"group": GROUP, "orderId": msg["data"]["orderId"], "ran": ran})
                print(f"  {msg['data']['orderId']}: {'charged' if ran else 'already charged, skipped'}")
            except RuntimeError as err:
                # auto_ack is off, so the failure is reported explicitly. It
                # spends one retry and brings the order back.
                print(f"  {msg['data']['orderId']}: {err} (will be redelivered)")
                await queen.ack(msg, "failed", {"group": group, "error": str(err)})

                # The moment to check the claim: right after a failure before
                # the commit.
                marker = await queen.kv.get(NS, marker_key(msg["data"]["orderId"]))
                check(
                    not marker["found"],
                    f"{msg['data']['orderId']} failed before its commit and left no marker behind",
                )

        await charger(GROUP, CHARGE_DELIVERIES).consume(charge)

        charged = [o for o in observed if o["group"] == GROUP]
        check(
            len(charged) == len(ORDER_IDS),
            f"the charger decided every order ({len(ORDER_IDS)}, got {len(charged)})",
        )

        # ------------------------------------------------------------ replay
        #
        # A second group reads the same orders from the beginning: the same
        # messages, the same handler, and only the markers stand between them
        # and a second charge.
        print("\nreplaying")

        async def replay(msg) -> None:
            ran = await handle(msg)
            observed.append({"group": REPLAY_GROUP, "orderId": msg["data"]["orderId"], "ran": ran})
            print(f"  {msg['data']['orderId']}: ran is {ran}")

        await charger(REPLAY_GROUP, len(ORDER_IDS)).consume(replay)

        # ----------------------------------------------------------- checking
        print("\nchecking")

        check(
            len(LEDGER) == len(ORDER_IDS),
            f"{len(ORDER_IDS)} orders, {len(LEDGER)} charges",
        )
        per_order = {order_id: sum(1 for row in LEDGER if row["orderId"] == order_id) for order_id in ORDER_IDS}
        check(
            all(count == 1 for count in per_order.values()),
            "one charge per order, none twice and none missing",
        )
        check(
            ATTEMPTS.get(CRASHING_ORDER, 0) >= 2,
            f"{CRASHING_ORDER} came back after its failure and was charged then",
        )

        replayed = [o for o in observed if o["group"] == REPLAY_GROUP]
        check(
            len(replayed) == len(ORDER_IDS),
            f"the replay received all {len(ORDER_IDS)} orders (got {len(replayed)})",
        )
        check(
            all(o["ran"] is False for o in replayed),
            "the replay charged none of them",
        )
        check(
            len(LEDGER) == len(ORDER_IDS),
            f"the ledger still has {len(ORDER_IDS)} charges after the replay",
        )

        # The markers are readable state. Each one names the charge it stands
        # for, so "was this order billed, and by which charge" has an answer in
        # the broker.
        markers = await queen.kv.get_many(NS, [marker_key(o) for o in ORDER_IDS])
        check(
            len(markers["rows"]) == len(ORDER_IDS),
            f"every order has its marker ({len(markers['rows'])})",
        )
        check(len(markers["missing"]) == 0, "no order is missing its marker")
        charge_ids = {row["chargeId"] for row in LEDGER}
        check(
            all(row["value"]["chargeId"] in charge_ids for row in markers["rows"]),
            "each marker names the charge that was made",
        )

        print("\n  ledger: " + ", ".join(f"{row['orderId']}={row['chargeId']}" for row in LEDGER))

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        # Clean up in every case, a failed run included. The markers are KV
        # entries and deleting the queue does not remove them; they would
        # expire after their hour, but a program should not leave state behind
        # on a shared broker. Best effort: a cleanup that raised would replace
        # the real verdict with its own.
        try:
            for order_id in ORDER_IDS:
                await queen.kv.delete(NS, marker_key(order_id))
            await queen.queue(ORDERS).delete()
        except Exception as err:  # noqa: BLE001 - the run's verdict outranks this
            print(f"  (cleanup incomplete: {err})")
        await queen.close()

    sys.stdout.flush()
    print(verdict, file=sys.stderr if failed else sys.stdout)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
# docs:end
