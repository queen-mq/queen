# docs:start(tut-py-multi-queue-flow)
#
# Tutorial 2 of 5: a multi-queue flow.
#
# One queue partitioned per customer, two consumer groups reading it
# independently, and a second queue downstream. This is the shape most
# applications end up with, and it shows the three things that make it work:
# a partition keeps one entity's events in order, a consumer group is a cursor
# so every group sees everything, and a queue is created by the push.
#
#   orders (partition = customer)
#     |-- group "billing"    -> charges, and pushes to the shipping queue
#     `-- group "analytics"  -> counts, and pushes nothing
#   shipping
#     `-- group "warehouse"  -> ships
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 02_multi_queue_flow.py

import asyncio
import os
import sys
import time

from queen import Queen

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
RUN = f"{int(time.time() * 1000):x}"
ORDERS = f"tut-py-orders-{RUN}"
SHIPPING = f"tut-py-shipping-{RUN}"

INPUT = [
    {"orderId": "A-1", "customer": "acme", "total": 120.5},
    {"orderId": "A-2", "customer": "acme", "total": 12.0},
    {"orderId": "B-1", "customer": "globex", "total": 88.75},
    {"orderId": "C-1", "customer": "initech", "total": 310.0},
    {"orderId": "A-3", "customer": "acme", "total": 9.99},
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
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        # Push each order into the partition named after its customer.
        # Everything about one customer stays in order; different customers
        # never wait for each other. The partition key is the only ordering
        # decision you make.
        print("\npushing")
        for order in INPUT:
            await queen.queue(ORDERS).partition(order["customer"]).push({"data": order})
            print(f"  {order['orderId']} -> partition {order['customer']}")

        # Group one. It reads every order, charges it, and hands the paid ones
        # to the shipping queue. subscription_mode("all") matters: a group
        # created after the messages were pushed starts at the tail by default,
        # so without it this group would see nothing.
        #
        # The handler is an async def taking one message: consume() awaits it
        # for every message, and acknowledges on return. Raising from inside it
        # is a nack instead.
        print("\nbilling")
        billed = []

        async def bill(msg):
            billed.append(msg["data"]["orderId"])
            print(f"  charged {msg['data']['orderId']} ({msg['data']['total']})")

            # The push to the next queue creates it on first use, exactly like
            # the first queue. Partitioning it by customer as well keeps a
            # customer's shipments in the order their orders were charged.
            await queen.queue(SHIPPING).partition(msg["data"]["customer"]).push(
                {"data": {"orderId": msg["data"]["orderId"], "customer": msg["data"]["customer"]}}
            )

        await (
            queen.queue(ORDERS)
            .group("tut-py-billing")
            .subscription_mode("all")
            .each()
            .limit(len(INPUT))
            # Stop after 5s of silence, so a lost message fails the run instead
            # of hanging it. The check happens between polls, so a consumer that
            # is already parked in a long poll finishes that poll first.
            .idle_millis(5000)
            .consume(bill)
        )

        check(len(billed) == len(INPUT), f"billing saw all {len(INPUT)} orders")

        # Group two reads the same stored messages through its own cursor. It
        # was not affected by billing acking them: that is what fan-out means
        # here, and it costs no extra copy of the data.
        print("\nanalytics")
        total = 0.0

        async def count(msg):
            nonlocal total
            total += msg["data"]["total"]

        await (
            queen.queue(ORDERS)
            .group("tut-py-analytics")
            .subscription_mode("all")
            .each()
            .limit(len(INPUT))
            .idle_millis(5000)
            .consume(count)
        )

        check(
            abs(total - sum(o["total"] for o in INPUT)) < 0.001,
            "analytics summed every order, independently of billing",
        )

        # The order inside one partition is the order it was pushed in. Check
        # the customer with more than one order.
        print("\nwarehouse")
        acme_shipments = []

        async def ship(msg):
            acme_shipments.append(msg["data"]["orderId"])
            print(f"  shipping {msg['data']['orderId']}")

        await (
            queen.queue(SHIPPING)
            .partition("acme")
            .group("tut-py-warehouse")
            .subscription_mode("all")
            .each()
            .limit(3)
            .idle_millis(5000)
            .consume(ship)
        )

        check(
            acme_shipments == ["A-1", "A-2", "A-3"],
            "one customer's shipments arrived in the order they were pushed",
        )

        await queen.queue(ORDERS).delete()
        await queen.queue(SHIPPING).delete()

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
