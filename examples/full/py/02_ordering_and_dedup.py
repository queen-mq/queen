# docs:start(full-py-ordering-dedup)
"""Queen in Python: per-entity ordering, and dedup by transaction id.

One partition per customer. Messages for a customer come back in the order they
were pushed, even though the three customers are interleaved on the wire. Then
the same message is pushed twice under one transaction id, and the broker
answers "duplicate" without storing anything new.

Run it with the client source on the path:

    cd /path/to/queen
    PYTHONPATH=clients/client-py QUEEN_URL=http://localhost:6699 \
        python3 examples/full/py/02_ordering_and_dedup.py

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
QUEUE = f"ex-py-ordering-dedup-{RUN_ID}"
GROUP = "audit"

# A partition is Queen's ordering lane. Order is guaranteed inside a partition
# and nowhere else, so the partition key is the entity whose history must stay
# in sequence: here, one customer.
CUSTOMERS = ["cust-acme", "cust-globex", "cust-initech"]
STEPS = ["created", "paid", "packed", "shipped"]


def check(condition, description):
    """Assert one claim and say so out loud. A failed check exits non-zero."""
    if not condition:
        print(f"FAIL  {description}")
        sys.exit(1)
    print(f"ok    {description}")


async def main():
    async with Queen(BROKER_URL) as client:
        print(f"broker {BROKER_URL}")
        print(f"queue  {QUEUE}\n")

        await (
            client.queue(QUEUE)
            .config(
                {
                    "lease_time": 60,
                    "retry_limit": 3,
                    # The window the broker keeps transaction ids in for the
                    # duplicate check. Outside it, the same id is new work again.
                    "dedup_window_seconds": 300,
                }
            )
            .create()
        )
        print(f"      created {QUEUE} with a 300s dedup window\n")

        # ------------------------------------------------------------------
        # 1. Push, interleaved across the three customers.
        # ------------------------------------------------------------------
        # Step by step rather than customer by customer, so no customer's
        # messages are contiguous on the wire.
        push_order = []
        statuses = []
        for step in STEPS:
            for customer in CUSTOMERS:
                event = {"customer": customer, "step": step}
                # .partition(customer) puts this message in that customer's lane.
                result = await client.queue(QUEUE).partition(customer).push({"data": event})
                statuses.append(result[0]["status"])
                push_order.append(f"{customer}:{step}")

        print(f"      push order: {' '.join(push_order)}")
        check(len(push_order) == len(CUSTOMERS) * len(STEPS), f"pushed {len(push_order)} messages")
        check(all(s == "queued" for s in statuses), "every push was accepted as new work")

        # ------------------------------------------------------------------
        # 2. Consume all of them.
        # ------------------------------------------------------------------
        arrival = []

        async def handle(messages):
            # batch(n) with n > 1 hands the Python handler a list. partitions(3)
            # lets one pop claim up to three lanes at once, so a single call can
            # drain all three customers; the messages stay grouped by lane.
            for message in messages:
                arrival.append(message)
            lanes = sorted({m["partition"] for m in messages})
            print(f"      received {len(messages)} messages from lanes {lanes}")

        await (
            client.queue(QUEUE)
            .group(GROUP)
            .batch(len(push_order))
            .partitions(len(CUSTOMERS))
            .limit(len(push_order))
            .idle_millis(5000)
            .timeout_millis(2000)
            .consume(handle)
        )

        arrival_order = [f"{m['partition']}:{m['data']['step']}" for m in arrival]
        print(f"      arrival order: {' '.join(arrival_order)}\n")

        # ------------------------------------------------------------------
        # 3. Verify the ordering guarantee, per lane.
        # ------------------------------------------------------------------
        check(len(arrival) == len(push_order), f"all {len(push_order)} messages came back")

        for customer in CUSTOMERS:
            seen = [m["data"]["step"] for m in arrival if m["partition"] == customer]
            check(seen == STEPS, f"{customer} arrived in push order: {' -> '.join(seen)}")

        # The global sequence is free to differ from the push sequence (it is
        # grouped by lane above), and that is exactly the trade: ordering is a
        # per-partition promise, which is what lets partitions run in parallel.

        # ------------------------------------------------------------------
        # 4. Push the same message twice under one transaction id.
        # ------------------------------------------------------------------
        # The transaction id is the message's identity for the duplicate check.
        # Supply the id your business already has, and a retried push (a timeout,
        # a redelivered webhook, a restarted job) costs nothing and creates
        # nothing. Left unset, the client mints a fresh UUIDv7 per push, which
        # can never be a duplicate.
        payment_id = f"payment-{RUN_ID}-7781"
        payment = {"customer": "cust-acme", "step": "refunded", "paymentId": payment_id}

        first = await (
            client.queue(QUEUE)
            .partition("cust-acme")
            .push({"transactionId": payment_id, "data": payment})
        )
        print(f"      first push of {payment_id}: {first[0]['status']}")
        check(first[0]["status"] == "queued", "the first push of a transaction id is queued")

        # Byte-for-byte the same push, as a retry would send it.
        second = await (
            client.queue(QUEUE)
            .partition("cust-acme")
            .push({"transactionId": payment_id, "data": payment})
        )
        print(f"      second push of {payment_id}: {second[0]['status']}")
        check(second[0]["status"] == "duplicate", "the second push is rejected as a duplicate")
        check(
            second[0]["message_id"] == first[0]["message_id"],
            "the duplicate points at the original message rather than a new one",
        )

        # ------------------------------------------------------------------
        # 5. Verify the duplicate stored nothing.
        # ------------------------------------------------------------------
        # If the second push had been stored, this pop would return two messages.
        tail = await (
            client.queue(QUEUE).group(GROUP).batch(10).wait(True).timeout_millis(2000).pop()
        )
        check(len(tail) == 1, f"exactly one new message exists, not two (got {len(tail)})")
        check(tail[0]["transactionId"] == payment_id, "and it is the one pushed first")

        # Acking is a manual call when you pop rather than consume. partitionId
        # comes off the message and is mandatory: it is what makes the ack refer
        # to one message and no other.
        acked = await client.ack(tail[0], True, {"group": GROUP})
        check(acked.get("success") is True, "the refund message was acknowledged")

        deleted = await client.queue(QUEUE).delete()
        check(deleted.get("deleted") is True, f"queue {QUEUE} deleted")

        print("\nPASS 02_ordering_and_dedup")


asyncio.run(main())
# docs:end
