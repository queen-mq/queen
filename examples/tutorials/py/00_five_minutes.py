# Queen in five minutes: the program behind the homepage guide, in Python.
#
# The homepage shows the regions between `guide:start(<step>)` and `guide:end`,
# verbatim. Everything outside them gives the program a clean start and checks
# that each step did what the page says it does.
#
# Run it against the node that step 1 of the guide starts:
#   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
#   PYTHONPATH=clients/client-py python3 examples/tutorials/py/00_five_minutes.py
# run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

import asyncio
import uuid

import queen as setup


async def main() -> None:
    admin = setup.Queen(url="http://localhost:6632")
    for q in ["orders", "shipments", "reminders", "reports", "revenue-per-minute"]:
        try:
            await admin.queue(q).delete()
        except Exception:
            pass

    # guide:start(configure)
    from queen import Queen

    queen = Queen(url="http://localhost:6632")

    await (
        queen.queue("orders")
        .namespace("shop")                 # groups queues in the dashboard
        .config({
            "leaseTime": 30,               # seconds a worker holds a message before it is redelivered
            "retryLimit": 3,               # failures before the message goes to the dead-letter queue
            "retentionEnabled": True,      # retention runs only when this is on
            "retentionSeconds": 86400,     # then messages older than a day are removed
        })
        .create()
    )
    # guide:end
    print("ok: configure")

    # guide:start(push)
    await (
        queen.queue("orders")
        .partition("customer-42")          # one ordered partition per customer, created by this push
        .push([{
            "transactionId": "order-1001",  # the dedup key: pushing the same id again is a no-op
            "data": {"orderId": 1001, "customer": 42, "total": 59.9},
        }])
    )
    # guide:end
    print("ok: push")

    # guide:start(consume)
    async def handle(msg):                 # acked when it returns, retried if it raises
        print(msg["data"])

    await (
        queen.queue("orders")
        .each()                            # the handler gets one message at a time
        .limit(1)                          # stop after one message; a worker leaves this out
        .consume(handle)
    )
    # guide:end
    assert len(await queen.queue("orders").wait(False).pop()) == 0, "queue mode acked the order"
    print("ok: consume")

    billed = []
    # guide:start(consume-group)
    async def bill(msg):
        ok = msg["data"]["total"] > 0
        await queen.ack(msg, "completed" if ok else "failed", {"group": "billing"})
        billed.append(msg["data"]["orderId"])  # guide:hide

    await (
        queen.queue("orders")
        .group("billing")                  # every group gets every message
        .subscription_mode("all")          # a new group starts at the tail unless told otherwise
        .batch(100)                        # up to 100 messages per pop...
        .partitions(10)                    # ...from up to 10 partitions
        .renew_lease(True, 2000)           # heartbeat every 2 s while the handler runs
        .auto_ack(False)                   # the handler decides the outcome
        .each()
        .limit(1)
        .consume(bill)
    )
    # guide:end
    assert billed == [1001], "the billing group read the order pushed before it existed"
    print("ok: consume-group")

    # guide:start(transaction)
    from datetime import timedelta

    async def fulfil(msg):
        order_id, customer = msg["data"]["orderId"], msg["data"]["customer"]
        await (
            queen.transaction()
            .ack(msg, "completed", {"consumer_group": "fulfillment"})  # the input is done
            .kv.put("orders", f"order-{order_id}", {"status": "paid"}, ttl=timedelta(days=7))
            .queue("shipments").partition(msg["partition"])             # the next event
            .push([{"transactionId": f"ship-{order_id}", "data": {"orderId": order_id}}])
            .timer("reminders").key(f"review-{order_id}").after(timedelta(hours=24))  # tomorrow
            .payload({"orderId": order_id, "customer": customer}).schedule()
            .commit()
        )

    await (
        queen.queue("orders")
        .group("fulfillment")
        .subscription_mode("all")
        .auto_ack(False)
        .each()
        .limit(1)
        .consume(fulfil)
    )
    # guide:end
    state = await queen.kv.get("orders", "order-1001")
    assert state["value"]["status"] == "paid", f"the KV write committed: {state}"
    shipped = await queen.queue("shipments").wait(False).pop()
    assert len(shipped) == 1, "the shipment was pushed"
    assert shipped[0]["partition"] == "customer-42", "into the same customer partition"
    print("ok: transaction")

    replayed = []
    # guide:start(replay)
    async def audit(msg):
        print("again:", msg["data"])
        replayed.append(msg["data"]["orderId"])  # guide:hide

    await (
        queen.queue("orders")
        .group("audit")                              # a new group, with its own cursor
        .subscription_from("2026-10-22T00:00:00Z")   # everything pushed from this time on
        .each()
        .limit(1)
        .consume(audit)
    )
    # guide:end
    assert replayed == [1001], "the audit group read the order again"
    print("ok: replay")

    # guide:start(fetch)
    read = await queen.queue("orders").fetch(["customer-42"])  # no group, lease or ack
    for record in read["customer-42"]["records"]:
        print(record["offset"], record["payload"])
    # guide:end
    assert read["customer-42"]["records"][0]["payload"]["orderId"] == 1001, "fetch read the partition"
    print("ok: fetch")

    # guide:start(lock)
    lock = queen.lock("daily-report", ttl_seconds=30)  # a lease with a fencing token
    if await lock.acquire():                           # False: another worker has it
        try:
            await (
                queen.transaction()
                .guard(lock)                           # commits only while the lock is ours
                .queue("reports")
                .push([{"transactionId": "report-2026-10-22", "data": {"day": "2026-10-22"}}])
                .commit()
            )
        finally:
            await lock.release()
    # guide:end
    assert len(await queen.queue("reports").wait(False).pop()) == 1, "the guarded report was pushed"
    print("ok: lock")

    # guide:start(stream)
    from queen import Stream

    revenue = await (
        Stream.from_(queen.queue("orders"))           # one window per partition: per customer
        .window_tumbling(seconds=60)
        .aggregate({"count": lambda order: 1, "sum": lambda order: order["total"]})
        .to(queen.queue("revenue-per-minute"))        # one result per customer per minute
        .run(query_id="revenue-per-minute", url="http://localhost:6632")
    )
    # guide:end
    await asyncio.sleep(1)  # let the stream's group register at the tail
    await queen.queue("orders").partition("customer-42").push(
        [{"transactionId": f"order-1002-{uuid.uuid4()}", "data": {"orderId": 1002, "customer": 42, "total": 40.1}}]
    )
    windows = []
    for _ in range(100):
        windows = await queen.queue("revenue-per-minute").wait(True).timeout_millis(1000).pop()
        if windows:
            break
    await revenue.stop()
    assert len(windows) == 1, "the window closed and its result was pushed"
    assert windows[0]["data"]["count"] == 1, f"one order in the window: {windows[0]['data']}"
    assert windows[0]["data"]["sum"] == 40.1, "its total summed"
    print("ok: stream", windows[0]["data"])

    async def worker():
        # guide:start(worker)
        result = await queen.ephemeral.pop("price-requests", group="pricers", wait=True)
        for m in result["messages"]:
            await queen.ephemeral.push(m["payload"]["replyTo"], [{"sku": m["payload"]["sku"], "price": 59.9}])
        await queen.ephemeral.ack("price-requests", result["messages"], group="pricers")
        # guide:end

    async def caller():
        await asyncio.sleep(0.3)  # the worker is already waiting when a caller asks
        # guide:start(caller)
        inbox = f"inbox-{uuid.uuid4()}"                # a private reply queue
        await queen.ephemeral.push("price-requests", [{"sku": "A-1", "replyTo": inbox}])
        reply = await queen.ephemeral.pop(inbox, wait=True, timeout=5000, commit_on_delivery=True)
        print(reply["messages"][0]["payload"])         # {'sku': 'A-1', 'price': 59.9}
        # guide:end
        return reply

    _, reply = await asyncio.gather(worker(), caller())
    assert reply["messages"][0]["payload"]["price"] == 59.9, "the reply came back"
    print("ok: ephemeral")

    await queen.close()
    await admin.close()
    print("\nPASS")


if __name__ == "__main__":
    asyncio.run(main())
