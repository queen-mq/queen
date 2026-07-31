# docs:start(full-py-produce-consume)
"""Queen in Python: the smallest complete loop.

Create a queue, push a handful of orders, consume them with a consumer group,
acknowledge each one, and verify that every order arrived exactly once.

Run it with the client source on the path:

    cd /path/to/queen
    PYTHONPATH=clients/client-py QUEEN_URL=http://localhost:6699 \
        python3 examples/full/py/01_produce_consume.py

Exits 0 when every check passes, 1 on the first failure.
"""

import asyncio
import os
import sys
import uuid

from queen import Queen

BROKER_URL = os.environ.get("QUEEN_URL", "http://localhost:6699")

# A fresh queue name per run keeps this run independent of every previous one.
RUN_ID = uuid.uuid4().hex[:8]
QUEUE = f"ex-py-produce-consume-{RUN_ID}"

# A consumer group is a named cursor over the queue. The ack is what moves that
# cursor forward, so the group is the unit of "who has read how far".
GROUP = "receipts"

ORDERS = [
    {"orderId": "A-1", "customer": "acme", "amount": 120},
    {"orderId": "A-2", "customer": "acme", "amount": 40},
    {"orderId": "B-1", "customer": "globex", "amount": 250},
    {"orderId": "B-2", "customer": "globex", "amount": 15},
    {"orderId": "C-1", "customer": "initech", "amount": 999},
]


def check(condition, description):
    """Assert one claim and say so out loud.

    The example is documentation, so it must never print a success it did not
    verify. A failed check exits non-zero.
    """
    if not condition:
        print(f"FAIL  {description}")
        sys.exit(1)
    print(f"ok    {description}")


async def main():
    # The Python client is async throughout, and an async context manager, so
    # exiting the block flushes buffers and closes the HTTP client.
    async with Queen(BROKER_URL) as client:
        print(f"broker {BROKER_URL}")
        print(f"queue  {QUEUE}\n")

        # ------------------------------------------------------------------
        # 1. Create the queue.
        # ------------------------------------------------------------------
        # /api/v1/configure is a full replace: every key you leave out goes back
        # to its default, so send the whole configuration you want every time.
        created = await (
            client.queue(QUEUE)
            .config(
                {
                    "lease_time": 60,  # seconds a popped message stays claimed by its consumer
                    "retry_limit": 3,  # failed deliveries before the message lands in the DLQ
                    "dedup_window_seconds": 300,  # how far back the broker remembers transaction ids
                }
            )
            .create()
        )
        check(created.get("configured") is True, f"queue {QUEUE} created")
        check(created["options"]["leaseTime"] == 60, "lease time came back as configured")

        # ------------------------------------------------------------------
        # 2. Push the orders.
        # ------------------------------------------------------------------
        # One push call carries the whole list. The broker answers with one
        # result per item, in the order sent: queued, duplicate or failed.
        results = await client.queue(QUEUE).push([{"data": order} for order in ORDERS])

        for order, result in zip(ORDERS, results):
            print(f"      pushed {order['orderId']} -> {result['status']}")
        check(len(results) == len(ORDERS), f"broker answered for all {len(ORDERS)} pushed messages")
        check(
            all(r["status"] == "queued" for r in results),
            "every push was accepted as new work",
        )

        # ------------------------------------------------------------------
        # 3. Consume them.
        # ------------------------------------------------------------------
        received = []

        async def handle(message):
            # With batch(1) the Python handler receives a single message dict.
            # With batch(n) for n > 1 it receives a list instead, unless you add
            # .each() to unroll the batch one message at a time.
            order = message["data"]
            received.append(order)
            print(f"      consumed {order['orderId']} for {order['customer']}")
            # Returning without raising is the commit: the consumer acks the
            # message for this group, and that ack advances the group's cursor.
            # Raising here would nack it instead, and the broker would redeliver.

        await (
            client.queue(QUEUE)
            .group(GROUP)
            .batch(1)
            .limit(len(ORDERS))  # stop this worker after five messages so the example ends
            .idle_millis(5000)  # and stop anyway if the broker has nothing more to give
            .timeout_millis(2000)  # length of one long poll
            .consume(handle)
        )

        # ------------------------------------------------------------------
        # 4. Verify: everything, once.
        # ------------------------------------------------------------------
        pushed_ids = sorted(o["orderId"] for o in ORDERS)
        received_ids = sorted(o["orderId"] for o in received)

        check(len(received) == len(ORDERS), f"received {len(received)} messages, expected {len(ORDERS)}")
        check(len(set(received_ids)) == len(received_ids), "no order was delivered twice")
        check(received_ids == pushed_ids, "every pushed order came back")

        # A drained queue is the other half of "exactly once": the acks moved the
        # group's cursor past the last message, so a further pop finds nothing.
        leftover = await (
            client.queue(QUEUE).group(GROUP).batch(10).wait(True).timeout_millis(2000).pop()
        )
        check(leftover == [], f"nothing is left for group {GROUP}")

        # ------------------------------------------------------------------
        # 5. Clean up. Queues are cheap, but this one was for this run only.
        # ------------------------------------------------------------------
        deleted = await client.queue(QUEUE).delete()
        check(deleted.get("deleted") is True, f"queue {QUEUE} deleted")

        print("\nPASS 01_produce_consume")


asyncio.run(main())
# docs:end
