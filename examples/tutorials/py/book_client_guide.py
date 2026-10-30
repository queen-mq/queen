# The little book of QueenMQ: the examples of chapter 2 ("Writing applications")
# that are not already in the five-minute guide (00_five_minutes.py), in Python.
#
# The book shows the regions between `guide:start(<id>)` and `guide:end`,
# verbatim, minus lines ending in `guide:hide`. Everything outside them gives
# the program a clean start and checks that each example does what the book
# says it does.
#
# Run it against the node that the book's first section starts:
#   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
#   PYTHONPATH=clients/client-py python3 examples/tutorials/py/book_client_guide.py
# The wire-forms example needs zstd: Python 3.14, or pip install "queen-mq[zstd]".
# run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

import asyncio
import signal
import time
from datetime import timedelta

import httpx

from queen import Queen, Stream

QUEUES = ["jobs", "emails", "events", "reviews", "prices", "metrics", "tasks", "payments",
          "logins", "login-rate", "clicks", "sales", "sales-per-hour", "outgoing-sms", "invoices"]


# The helpers the examples call. In a real program they are yours.
async def send_email(data):
    pass


async def set_when(condition, event):
    """Test bookkeeping: stop a worker that runs until a signal, once its work is done."""
    while not condition():
        await asyncio.sleep(0.02)
    event.set()


async def main() -> None:
    queen = Queen(url="http://localhost:6632")
    for q in QUEUES:
        try:
            await queen.queue(q).delete()
        except Exception:
            pass

    # --- Configure ------------------------------------------------------------
    # guide:start(configure-jobs)
    await queen.queue("jobs").config({
        "leaseTime": 120,                   # 2 minutes per job, then it goes to another worker
        "retryLimit": 5,                    # after 5 failures, the dead-letter queue
        "retryDelay": 10000,                # a failed job comes back after 10 s, not at once
        "delayedProcessing": 30,            # a new job waits 30 s before anyone can take it
        "windowBuffer": 5,                  # a burst waits 5 s, then is handed over at once
        "dedupWindowSeconds": 86400,        # a transactionId is remembered for a day
        "retentionEnabled": True,           # retention runs only when this is on:
        "completedRetentionSeconds": 3600,  #   jobs every group has finished go after an hour
        "maxWaitTimeSeconds": 604800,       # anything older than a week goes, finished or not
    }).create()
    # guide:end
    options = (await queen.admin.get_queue("jobs"))["options"]
    assert options["retryDelay"] == 10000, options
    assert options["delayedProcessing"] == 30, options
    assert options["windowBuffer"] == 5, options
    assert options["retentionEnabled"] is True, options
    assert options["completedRetentionSeconds"] == 3600, options
    assert options["maxWaitTimeSeconds"] == 604800, options
    print("ok: configure-jobs")

    # --- Push -----------------------------------------------------------------
    # guide:start(push-callbacks)
    async def stored(items):
        print("stored", len(items))

    async def already_stored(items, error):
        print("already stored", items[0]["transactionId"])

    async def refused(items, error):
        print("refused", error)

    await (
        queen.queue("emails")
        .partition("customer-42")
        .push([{"transactionId": "welcome-42", "data": {"to": "ada@example.com"}}])
        .on_success(stored)
        .on_duplicate(already_stored)
        .on_error(refused)
    )
    # guide:end
    duplicate = []

    async def remember(items, error):
        duplicate.append(items[0]["transactionId"])

    await (
        queen.queue("emails").partition("customer-42")
        .push([{"transactionId": "welcome-42", "data": {"to": "ada@example.com"}}])
        .on_duplicate(remember)
    )
    assert duplicate == ["welcome-42"], "the second push of the same id reached on_duplicate"
    print("ok: push-callbacks")

    # guide:start(push-buffer)
    for i in range(1000):
        await (
            queen.queue("events")
            .partition(f"device-{i % 10}")
            .buffer({"message_count": 100, "time_millis": 1000})  # 100 at once, or every second
            .push([{"transactionId": f"reading-{i}", "data": {"device": i % 10, "value": i}}])
        )
    await queen.flush_all_buffers()                       # before exiting, send what is left
    # guide:end
    read = await queen.queue("events").fetch([f"device-{i}" for i in range(10)])
    total = sum(len(p["records"]) for p in read.values())
    assert total == 1000, f"every buffered reading was stored ({total})"
    print("ok: push-buffer")

    # Record what goes over the wire, to check the binary forms below.
    wire = []
    real_send = httpx.AsyncClient.send

    async def recording_send(self, request, **kwargs):
        response = await real_send(self, request, **kwargs)
        wire.append({"url": str(request.url), "body": request.content,
                     "sent": request.headers.get("content-type"),
                     "got": response.headers.get("content-type")})
        return response

    httpx.AsyncClient.send = recording_send
    wire_data = []
    # guide:start(wire-forms)
    fast = Queen(
        url="http://localhost:6632",
        binary_push=True,       # a push travels as one binary body, not JSON
        push_compression=True,  # with binary_push: zstd-compresses each partition's batch here
        framed_pop=True,        # a pop gets the records as stored, and unpacks them here
    )

    await fast.queue("invoices").partition("customer-42").push(
        [{"transactionId": "invoice-2001", "data": {"orderId": 2001, "total": 12.5}}]
    )

    async def show(msg):
        wire_data.append(msg["data"])  # guide:hide
        print(msg["data"])             # {'orderId': 2001, 'total': 12.5}, as with JSON

    await fast.queue("invoices").each().limit(1).consume(show)
    # guide:end
    httpx.AsyncClient.send = real_send
    push = next(w for w in wire if "/api/v1/push" in w["url"])
    pop = next(w for w in wire if "/api/v1/pop/" in w["url"] and "frames=true" in w["url"])
    assert push["sent"] == "application/x-queen-push", "the push went as a binary body"
    zstd_magic = bytes([0x28, 0xB5, 0x2F, 0xFD])
    assert zstd_magic in push["body"], "the binary push carried a zstd frame"
    assert pop["got"] == "application/x-queen-pop", "the broker answered the pop with frames"
    assert wire_data == [{"orderId": 2001, "total": 12.5}], wire_data
    await fast.close()
    print("ok: wire-forms")

    # --- Consume --------------------------------------------------------------
    archived = []
    # guide:start(consume-batch)
    async def archive(messages):          # no .each(): the handler gets the whole batch
        print("archiving", len(messages))
        archived.extend(messages)  # guide:hide

    await (
        queen.queue("events")
        .group("archiver")
        .subscription_mode("all")
        .batch(100)                       # up to 100 messages per call
        .limit(1000)
        .consume(archive)                 # all acked if it returns, all nacked if it raises
    )
    # guide:end
    assert len(archived) == 1000, f"the batch handler saw every reading ({len(archived)})"
    assert len({m["transactionId"] for m in archived}) == 1000, "each reading once"
    print("ok: consume-batch")

    # guide:start(pop-ack)
    messages = await (
        queen.queue("events")
        .group("auditor")
        .subscription_mode("all")
        .batch(50)
        .wait(True)                       # long poll: wait for messages, never return empty
        .pop()
    )
    print("auditing", len(messages))
    await queen.ack(messages, "completed", {"group": "auditor"})   # the whole batch at once
    # guide:end
    assert 0 < len(messages) <= 50, f"pop returned a batch ({len(messages)})"
    print("ok: pop-ack")

    await queen.queue("reviews").partition("shop-3").push([
        {"transactionId": "review-1", "data": {"stars": 5}},
        {"transactionId": "review-2", "data": {"stars": 1}},
    ])
    first, second = await (
        queen.queue("reviews").partition("shop-3").group("moderation")
        .subscription_mode("all").batch(2).wait(True).pop()
    )
    # guide:start(ack-each-status)
    await queen.ack([
        {**first, "_status": "completed"},
        {**second, "_status": "failed", "_error": "timeout"},
    ], "completed", {"group": "moderation"})
    # guide:end
    # A long poll already waiting is not woken when retryDelay passes (a broker
    # gap, reported); a new pop after the delay gets the message.
    back = []
    for _ in range(5):
        back = await (
            queen.queue("reviews").partition("shop-3").group("moderation")
            .batch(2).wait(True).timeout_millis(1500).pop()
        )
        if back:
            break
    assert [m["transactionId"] for m in back] == ["review-2"], "only the failed review came back"
    await queen.ack(back[0], "completed", {"group": "moderation"})
    print("ok: ack-each-status")

    # guide:start(consume-callbacks)
    async def deliver(msg):
        await send_email(msg["data"])

    async def completed(msg, result):
        await queen.ack(msg, "completed", {"group": "mailer"})

    async def failed(msg, error):
        status = "dlq" if getattr(error, "permanent", False) else "failed"
        await queen.ack(msg, status, {"group": "mailer", "error": str(error)})

    await (
        queen.queue("emails")
        .group("mailer")
        .subscription_mode("all")
        .each()
        .limit(1)
        .consume(deliver)
        .on_success(completed)
        .on_error(failed)
    )
    # guide:end
    mailed = len(await queen.queue("emails").group("mailer").wait(False).pop())
    assert mailed == 0, "the on_success callback acked the email"
    print("ok: consume-callbacks")

    for price in [10, 11, 12]:
        await queen.queue("prices").partition("sku-A-1").push(
            [{"transactionId": f"price-A-1-{price}", "data": {"sku": "A-1", "price": price}}]
        )
    # guide:start(conflation)
    [latest] = await (
        queen.queue("prices")
        .group("repricer")
        .subscription_mode("all")
        .conflation()                     # only the newest message of each partition
        .wait(False)
        .pop()
    )
    print(latest["data"])                 # {'sku': 'A-1', 'price': 12}: 10 and 11 are skipped
    await queen.ack(latest, "completed", {"group": "repricer"})
    # guide:end
    assert latest["data"]["price"] == 12, "conflation delivered the newest price"
    again = await queen.queue("prices").group("repricer").conflation().wait(False).pop()
    assert len(again) == 0, "and moved past the older ones"
    print("ok: conflation")

    for i in range(5):
        await queen.queue("metrics").partition("cpu").push(
            [{"transactionId": f"cpu-{i}", "data": {"load": i}}]
        )
    # guide:start(commit-on-delivery)
    readings = await (
        queen.queue("metrics")
        .group("live-chart")
        .subscription_mode("all")
        .commit_on_delivery()             # the cursor moves as they are handed out: no ack
        .batch(100)
        .wait(False)
        .pop()
    )
    # guide:end
    assert len(readings) == 5, "commit_on_delivery handed out every reading"
    left = await queen.queue("metrics").group("live-chart").wait(False).pop()
    assert len(left) == 0, "and nothing is left to ack or redeliver"
    print("ok: commit-on-delivery")

    # guide:start(seek)
    await queen.admin.seek_consumer_group("archiver", "events", {
        "timestamp": "2026-10-22T00:00:00Z",
    })
    # the archiver group now reads again everything pushed since that time

    await queen.admin.seek_consumer_group("archiver", "events", {"toEnd": True})
    # and now it skips everything it has not read yet
    # guide:end
    left = await queen.queue("events").group("archiver").wait(False).pop()
    assert len(left) == 0, "after a seek to the end the group has nothing left"
    print("ok: seek")

    for i in range(40):
        await queen.queue("tasks").partition(f"account-{i % 8}").push(
            [{"transactionId": f"task-{i}", "data": {"n": i}}]
        )
    ran = 0

    async def run_task(data):
        nonlocal ran
        ran += 1
        await asyncio.sleep(0.005)

    # guide:start(consume-run)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)   # a deploy stops the worker cleanly
    watcher = asyncio.create_task(set_when(lambda: ran >= 40, stop))  # guide:hide

    async def handle(task):
        await run_task(task["data"])

    await (
        queen.queue("tasks")
        .group("workers")
        .subscription_mode("all")
        .concurrency(4)                   # four handlers at once, on four partitions
        .renew_lease(True, 10000)         # long tasks keep their lease alive
        .idle_millis(60000)               # or stop after a minute with nothing to do
        .timeout_millis(1000)  # guide:hide
        .each()
        .consume(handle, signal=stop)
    )
    # guide:end
    watcher.cancel()
    loop.remove_signal_handler(signal.SIGTERM)
    assert ran == 40, f"four workers ran every task, then the signal stopped them ({ran})"
    print("ok: consume-run")

    # --- Retries and the dead-letter queue --------------------------------------
    await queen.queue("payments").config({"retryLimit": 1, "retryDelay": 100}).create()
    await queen.queue("payments").partition("order-7").push(
        [{"transactionId": "charge-7", "data": {"amount": 25}}]
    )

    async def decline(msg):
        raise RuntimeError("card declined")

    await (
        queen.queue("payments").group("charger").subscription_mode("all").each().limit(2)
        .idle_millis(5000).consume(decline)
    )
    await asyncio.sleep(0.3)
    # guide:start(dlq-read)
    dead = await queen.queue("payments").dlq("charger").limit(20).get()
    for letter in dead["messages"]:
        print(letter["transactionId"], letter["errorMessage"], letter["retryCount"])
    # guide:end
    assert len(dead["messages"]) == 1, "the failing payment is in the dead-letter queue"
    assert dead["messages"][0]["errorMessage"] == "card declined", dead["messages"][0]
    print("ok: dlq-read")

    # guide:start(dlq-replay)
    for letter in dead["messages"]:
        # back into its partition, and out of the dead-letter queue
        await queen.admin.retry_message(letter["partitionId"], letter["transactionId"])
    # guide:end
    again = await (
        queen.queue("payments").group("charger").batch(10).wait(True).timeout_millis(3000).pop()
    )
    assert len(again) == 1, "the replayed payment is delivered again"
    await queen.ack(again, "completed", {"group": "charger"})
    left = await queen.queue("payments").dlq("charger").get()
    assert len(left["messages"]) == 0, "and it left the dead-letter queue"
    print("ok: dlq-replay")

    # The four ack statuses the book's table describes, checked here. 'retry'
    # gives a message back at once without spending a retry; 'dlq' files it now.
    await queen.queue("payments").partition("order-8").push(
        [{"transactionId": "charge-8", "data": {"amount": 9}}]
    )
    await queen.queue("payments").partition("order-9").push(
        [{"transactionId": "charge-9", "data": {"amount": 4}}]
    )
    [r1] = await (
        queen.queue("payments").partition("order-8").group("statuses")
        .subscription_mode("all").batch(1).wait(True).pop()
    )
    await queen.ack(r1, "retry", {"group": "statuses"})
    [r2] = await (
        queen.queue("payments").partition("order-8").group("statuses").batch(1).wait(True).pop()
    )
    assert r2["transactionId"] == r1["transactionId"], "'retry' redelivered the same message at once"
    assert (r2.get("retryCount") or 0) == (r1.get("retryCount") or 0), "'retry' spent no retry"
    await queen.ack(r2, "dlq", {"group": "statuses", "error": "bad payload"})
    filed = await queen.queue("payments").dlq("statuses").get()
    assert len(filed["messages"]) == 1, "'dlq' filed the message in the dead-letter queue"
    assert filed["messages"][0]["errorMessage"] == "bad payload", filed["messages"][0]
    print("ok: ack statuses")

    # --- KV -------------------------------------------------------------------
    await queen.kv.delete("carts", "customer-42")
    await queen.kv.delete("limits", "api:customer-42")
    # guide:start(kv)
    # Every write names its lifetime: a ttl, or forever.
    await queen.kv.put("carts", "customer-42", {"items": 3}, ttl=timedelta(days=7))
    cart = await queen.kv.get("carts", "customer-42")   # {found, value, version, ...}

    # Write only if nobody wrote since we read it: the version is the condition.
    saved = await queen.kv.put("carts", "customer-42", {"items": 4},
                               ttl=timedelta(days=7), expect=cart["version"])
    print(saved["applied"])                              # False if the version moved

    # A counter with a ceiling: past max, the increment does not apply.
    hit = await queen.kv.incr("limits", "api:customer-42", delta=1, max=100,
                              ttl=timedelta(minutes=1))
    if not hit["applied"]:
        print("over the limit")                          # reason: 'limit'
    # guide:end
    assert cart["found"] is True, "the cart was read back"
    assert cart["value"] == {"items": 3}, cart
    assert saved["applied"] is True, f"the conditional write applied: {saved}"
    stale = await queen.kv.put("carts", "customer-42", {"items": 9},
                               ttl=timedelta(days=7), expect=cart["version"])
    assert stale["applied"] is False, "a write from the old version is refused"
    assert stale["reason"] == "version", stale
    assert stale["value"] == {"items": 4}, f"and answers the current value: {stale}"
    assert hit["applied"] is True, hit
    capped = await queen.kv.incr("limits", "api:customer-42", delta=100, max=100,
                                 ttl=timedelta(minutes=1))
    assert capped["applied"] is False, "an increment past max does not apply"
    assert capped["reason"] == "limit", capped
    assert capped["value"] == 1, f"and the counter keeps its value: {capped}"
    print("ok: kv")

    # --- Streams ----------------------------------------------------------------
    # guide:start(stream-sliding)
    login_rate = await (
        Stream.from_(queen.queue("logins"))           # one partition per user
        .window_sliding(size=600, slide=60)           # the last 10 minutes, every minute
        .aggregate({"count": lambda login: 1})
        .to(queen.queue("login-rate"))
        .run(query_id="login-rate", url="http://localhost:6632")
    )
    # guide:end
    await asyncio.sleep(1.2)
    await login_rate.stop()
    print("ok: stream-sliding")

    # guide:start(stream-session)
    visits = await (
        Stream.from_(queen.queue("clicks"))           # one partition per visitor
        .window_session(gap=1800)                     # a visit ends after 30 minutes of quiet
        .aggregate({"count": lambda click: 1})
        .foreach(lambda visit: print("visit ended", visit))   # acked after this returns
        .run(query_id="visits", url="http://localhost:6632")
    )
    # guide:end
    await asyncio.sleep(1.2)
    await visits.stop()
    print("ok: stream-session")

    # guide:start(stream-hourly)
    hourly = await (
        Stream.from_(queen.queue("sales"))
        .window_cron(
            every="hour",                  # on the clock: 10:00 to 11:00, 11:00 to 12:00
            event_time=lambda msg: msg["data"]["soldAt"],  # when it happened, not on arrival
            allowed_lateness=120,          # a sale up to 2 minutes late still counts
        )
        .aggregate({
            "count": lambda sale: 1,
            "sum": lambda sale: sale["amount"],
            "max": lambda sale: sale["amount"],
        })
        .to(queen.queue("sales-per-hour"))
        .run(query_id="sales-per-hour", url="http://localhost:6632", subscription_mode="all")
    )
    # guide:end
    # One sale, to run the event_time function: its hour stays open, so nothing is
    # emitted yet, but a time the function cannot read fails the cycle.
    from datetime import datetime, timezone
    await queen.queue("sales").partition("shop-1").push([{
        "transactionId": f"sale-{time.time_ns()}",
        "data": {"soldAt": datetime.now(timezone.utc).isoformat(), "amount": 30},
    }])
    for _ in range(50):
        m = hourly.metrics()
        if m["messagesTotal"] or m["errorsTotal"]:
            break
        await asyncio.sleep(0.1)
    m = hourly.metrics()
    assert m["errorsTotal"] == 0, f"event_time read the sale time: {m}"
    assert m["messagesTotal"] >= 1, f"the hourly window took the sale: {m}"
    await hourly.stop()
    print("ok: stream-hourly")

    sent = []

    async def send_sms(sms):
        sent.append(sms)

    # guide:start(stream-gate)
    from queen import token_bucket_gate

    limiter = await (
        Stream.from_(queen.queue("outgoing-sms"))     # one partition per customer
        .gate(token_bucket_gate(capacity=10, refill_per_sec=1))  # 10 at once, then 1 a second
        .foreach(send_sms)
        .run(query_id="sms-limiter", url="http://localhost:6632")
    )
    # guide:end
    await asyncio.sleep(1)
    for i in range(3):
        await queen.queue("outgoing-sms").partition("customer-9").push(
            [{"transactionId": f"sms-{time.time_ns()}-{i}",
              "data": {"to": "+390000000", "text": f"hello {i}"}}]
        )
    for _ in range(100):
        if len(sent) >= 3:
            break
        await asyncio.sleep(0.1)
    await limiter.stop()
    assert len(sent) == 3, f"the gate let the first three messages through ({len(sent)})"
    print("ok: stream-gate")

    await queen.close()
    print("\nPASS")


if __name__ == "__main__":
    asyncio.run(main())
