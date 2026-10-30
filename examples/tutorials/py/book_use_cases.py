# The little book of QueenMQ: the use cases of chapter 3 ("Real applications examples"),
# in Python.
#
# The book shows the regions between `guide:start(<id>)` and `guide:end`,
# verbatim, minus lines ending in `guide:hide`. Everything outside them gives
# the program a clean start and checks that each use case does what the book
# says it does.
#
# Run it against a node:
#   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
#   PYTHONPATH=clients/client-py python3 examples/tutorials/py/book_use_cases.py
# run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

import asyncio
import http.server
import json
import signal
import threading
import time
import uuid
from datetime import timedelta

from queen import Queen, Stream, token_bucket_gate

url = "http://localhost:6632"
QUEUES = ["thumbnails", "order-placed", "chat", "order-events", "shipping", "trace-events",
          "traces", "sms-out", "availability", "review-reminders", "webhooks", "daily-reports"]


async def set_when(condition, event):
    """Test bookkeeping: stop a worker that runs until a signal, once its work is done."""
    while not condition():
        await asyncio.sleep(0.05)
    event.set()


class Receiver(http.server.BaseHTTPRequestHandler):
    """A webhook receiver that fails the first call, and keeps each idempotency key once."""

    calls = 0
    received = {}

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        Receiver.calls += 1
        status = 500 if Receiver.calls == 1 else 200
        if status == 200:
            Receiver.received[self.headers.get("idempotency-key")] = body
        self.send_response(status)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


async def main() -> None:
    queen = Queen(url=url)
    for q in QUEUES:
        try:
            await queen.queue(q).delete()
        except Exception:
            pass
    for ns, key in [("orders", "order-2001")]:
        try:
            await queen.kv.delete(ns, key)
        except Exception:
            pass
    # A run stopped half way can leave the reminder pending; the first schedule
    # below must find no timer of that key.
    for key in ["review-2001", "review-2002"]:
        try:
            await queen.timers.cancel("review-reminders", key)
        except Exception:
            pass

    # --- Work queue: competing consumers ---------------------------------------
    done = []

    async def resize(photo, worker):
        done.append({"photo": photo, "worker": worker})
        await asyncio.sleep(0.05)

    # guide:start(uc-work)
    # Twelve photos to resize, from four users.
    for i in range(1, 13):
        await queen.queue("thumbnails").partition(f"user-{i % 4}").push(
            [{"data": {"photo": f"p{i}.jpg"}}]
        )

    # Three workers compete for the jobs: each job goes to exactly one of them.
    def worker(name):
        async def handle(job):
            await resize(job["data"]["photo"], name)

        return (
            queen.queue("thumbnails")
            .partitions(1)                # one user's photos at a time
            .each()
            .idle_millis(1500)            # stop when there is nothing left
            .consume(handle)
        )

    await asyncio.gather(worker("w1"), worker("w2"), worker("w3"))
    # guide:end
    photos = [d["photo"] for d in done]
    assert len(photos) == 12, f"twelve jobs handled ({len(photos)})"
    assert len(set(photos)) == 12, "each job once"
    workers = {d["worker"] for d in done}
    assert len(workers) >= 2, f"the jobs were shared ({sorted(workers)})"
    share = {w: sum(1 for d in done if d["worker"] == w) for w in sorted(workers)}
    print("ok: uc-work", json.dumps(share))

    # --- Publish/subscribe and fan-out: consumer groups ------------------------
    seen = {"billing": [], "shipping": [], "analytics": []}
    # guide:start(uc-pubsub)
    def subscribe(group):
        async def handle(event):
            seen[group].append(event["data"]["orderId"])  # guide:hide
            print(group, event["data"]["orderId"])

        return (
            queen.queue("order-placed")
            .group(group)                 # each group gets every message
            .subscription_mode("all")
            .each()
            .idle_millis(1000)  # guide:hide
            .consume(handle)
        )

    for order_id in [3001, 3002, 3003]:
        await queen.queue("order-placed").partition(f"order-{order_id}").push(
            [{"data": {"orderId": order_id}}]
        )
    await asyncio.gather(subscribe("billing"), subscribe("shipping"))

    # Months later, analytics subscribes too, and reads every order ever placed.
    await subscribe("analytics")
    # guide:end
    for g in ["billing", "shipping", "analytics"]:
        assert sorted(seen[g]) == [3001, 3002, 3003], f"{g} got every order ({seen[g]})"
    # A group that does not ask for 'all' starts at the end of the queue.
    late = await queen.queue("order-placed").group("late").wait(False).batch(10).pop()
    assert len(late) == 0, "a new group without subscription_mode all skips the history"
    print("ok: uc-pubsub")

    # --- 1. Chat processing: a burst is one batch (windowBuffer) ---------------
    heard = []
    t0 = 0.0
    # guide:start(uc-chat)
    # A burst waits 2 seconds, then goes to the handler as one batch.
    await queen.queue("chat").config({"windowBuffer": 2}).create()
    t0 = time.monotonic()  # guide:hide

    # The guest sends three messages in a row.
    for text in ["Hi!", "Is breakfast included?", "And is there parking?"]:
        await queen.queue("chat").partition("conv-42").push([{"data": {"text": text}}])

    # The assistant answers the whole burst once.
    async def answer(messages):
        question = " ".join(m["data"]["text"] for m in messages)
        print(question)                   # Hi! Is breakfast included? And is there parking?
        heard.append(question)  # guide:hide

    await (
        queen.queue("chat")
        .group("assistant")
        .subscription_mode("all")
        .batch(10)                        # the handler gets every message of the burst
        .limit(3)  # guide:hide
        .consume(answer)
    )
    # guide:end
    waited = round((time.monotonic() - t0) * 1000)
    assert len(heard) == 1, f"the three messages reached the handler in one call ({heard})"
    assert heard[0] == "Hi! Is breakfast included? And is there parking?", heard[0]
    assert waited >= 1500, f"the burst was held for the window ({waited} ms)"
    print(f"ok: uc-chat (one call, after {waited} ms)")

    # --- 2. Order workflow: transitions that cannot race (expect) --------------
    await queen.kv.put("orders", "order-2001", {"status": "paid"}, ttl=timedelta(days=30))
    await queen.queue("order-events").partition("order-2001").push(
        [{"transactionId": "order-2001-paid", "data": {"orderId": "order-2001"}}]
    )
    verdicts = []
    # guide:start(uc-order)
    async def step(msg):
        order_id = msg["data"]["orderId"]
        order = await queen.kv.get("orders", order_id)          # {'status': 'paid'}
        if order["value"]["status"] != "paid":                   # already moved: just ack
            return await queen.ack(msg, "completed", {"group": "workflow"})

        # Do the work of this step here: charge the card, book the courier, ...

        result = await (
            queen.transaction()
            .ack(msg, "completed", {"consumer_group": "workflow"})
            .kv.put("orders", order_id, {"status": "shipping"},
                    ttl=timedelta(days=30), expect=order["version"],
                    required=True)                               # only from what we read
            .queue("shipping").partition(order_id)
            .push([{"transactionId": f"ship-{order_id}", "data": {"orderId": order_id}}])
            .commit()
        )
        verdicts.append(bool(result))  # guide:hide
        if not result:                    # moved by someone else: nothing written
            return

    await (
        queen.queue("order-events")
        .group("workflow")
        .subscription_mode("all")
        .auto_ack(False)
        .each()
        .limit(1)  # guide:hide
        .consume(step)
    )
    # guide:end
    assert verdicts == [True], verdicts
    order = await queen.kv.get("orders", "order-2001")
    assert order["value"]["status"] == "shipping", order
    # The same transition from the old version is refused, and writes nothing.
    stale = await (
        queen.transaction()
        .kv.put("orders", "order-2001", {"status": "shipping"},
                ttl=timedelta(days=30), expect=order["version"] - 1, required=True)
        .queue("shipping").partition("order-2001")
        .push([{"transactionId": "ship-again", "data": {}}])
        .commit()
    )
    assert stale["success"] is False, f"a transition from a stale version rolls back ({stale})"
    read = await queen.queue("shipping").fetch(["order-2001"])
    assert len(read["order-2001"]["records"]) == 1, "one shipment, not two"
    print("ok: uc-order")

    # --- 3. Trace management: events merged per trace (session window) ---------
    # guide:start(uc-trace)
    # A trace is complete when it has been quiet for 2 seconds.
    merger = await (
        Stream.from_(queen.queue("trace-events"))
        .window_session(gap=2, idle_flush_ms=500)
        .reduce(lambda t, e: {"spans": t["spans"] + 1, "ms": t["ms"] + e["ms"]},
                {"spans": 0, "ms": 0})
        .to(queen.queue("traces"))                      # one message per finished trace
        .run(query_id="trace-merger", url=url, subscription_mode="all")
    )

    # Every SDK call is one event, in the partition of its trace.
    def event(span, ms):
        return {"transactionId": f"trace-7f3a:{span}", "data": {"span": span, "ms": ms}}

    await queen.queue("trace-events").partition("trace-7f3a").push([
        event("http", 120), event("db", 45), event("cache", 3),
    ])
    await queen.queue("trace-events").partition("trace-7f3a").push(
        [event("db", 45)]                               # a retried call: stored once
    )
    # guide:end
    merged = None
    for _ in range(40):
        await asyncio.sleep(0.25)
        read = await queen.queue("traces").fetch(["trace-7f3a"])
        records = read["trace-7f3a"]["records"]
        if records:
            merged = records[0]["payload"]
            break
    await merger.stop()
    assert merged, "the finished trace reached the traces queue"
    value = merged.get("value", merged)
    assert value["spans"] == 3, f"three spans: the retried event was stored once ({merged})"
    assert value["ms"] == 168, merged
    print("ok: uc-trace", json.dumps(merged))

    # --- 4. Rate limiting: a token bucket gate per customer -------------------
    sent = []
    t4 = time.monotonic()

    async def send_sms(sms):
        sent.append({"n": sms["n"], "at": round((time.monotonic() - t4) * 1000)})

    # guide:start(uc-rate)
    # The SMS provider accepts 5 messages a second for each customer.
    # A refused message comes back when its lease ends: here after 1 second.
    await queen.queue("sms-out").config({"leaseTime": 1}).create()

    limiter = await (
        Stream.from_(queen.queue("sms-out"))                    # one partition per customer
        .gate(token_bucket_gate(capacity=5, refill_per_sec=5))  # 5 at once, then 5 a second
        .foreach(send_sms)                                      # only what the gate allows
        .run(query_id="sms-gate", url=url, subscription_mode="all")
    )

    # Twelve messages for one customer, all at once.
    await queen.queue("sms-out").partition("customer-42").push(
        [{"data": {"n": i + 1, "text": f"#{i + 1}"}} for i in range(12)]
    )
    # guide:end
    for _ in range(80):
        if len(sent) >= 12:
            break
        await asyncio.sleep(0.1)
    await limiter.stop()
    assert [x["n"] for x in sent] == list(range(1, 13)), f"all twelve were sent, in order ({sent})"
    first = len([x for x in sent if x["at"] < sent[0]["at"] + 800])
    assert first <= 5, f"at most 5 in the first burst ({first})"
    spread = sent[11]["at"] - sent[0]["at"]
    assert spread >= 1000, f"the rest waited ({spread} ms)"
    print("ok: uc-rate", json.dumps([x["at"] - sent[0]["at"] for x in sent]))

    # --- 5. Only the newest value (conflation) ----------------------------------
    # guide:start(uc-latest)
    # The availability of a hotel changes often; the screen needs only the last value.
    for rooms in [12, 11, 9]:
        await queen.queue("availability").partition("hotel-7").push(
            [{"data": {"hotel": 7, "rooms": rooms}}]
        )

    [latest] = await (
        queen.queue("availability")
        .group("screen")
        .subscription_mode("all")
        .conflation()                     # the newest message of each hotel, the rest skipped
        .wait(False)
        .pop()
    )
    print(latest["data"])                 # {'hotel': 7, 'rooms': 9}
    await queen.ack(latest, "completed", {"group": "screen"})
    # guide:end
    assert latest["data"] == {"hotel": 7, "rooms": 9}, latest["data"]
    again = await queen.queue("availability").group("screen").wait(False).pop()
    assert len(again) == 0, "the older values were skipped, not delivered later"
    print("ok: uc-latest")

    # --- 6. Reminders and delayed jobs (timers) ---------------------------------
    # Record what each schedule answered, without touching the example.
    statuses = []
    real_schedule = queen.timers.schedule

    async def recording_schedule(*args, **kwargs):
        result = await real_schedule(*args, **kwargs)
        statuses.append(result["status"])
        return result

    queen.timers.schedule = recording_schedule
    # guide:start(uc-reminder)
    # When an order is delivered, ask for a review in 24 hours.
    await (
        queen.timer("review-reminders").key("review-2001").after(timedelta(hours=24))
        .payload({"orderId": 2001}).schedule()            # status: 'scheduled'
    )

    # The delivery was late: schedule the same key again, and the reminder moves.
    await (
        queen.timer("review-reminders").key("review-2001").after(timedelta(hours=48))
        .payload({"orderId": 2001}).schedule()            # status: 'rescheduled'
    )

    # The guest wrote a review already: no reminder at all.
    await queen.timer("review-reminders").key("review-2001").cancel()
    # guide:end
    queen.timers.schedule = real_schedule
    assert statuses == ["scheduled", "rescheduled"], statuses
    peek = await queen.timers.peek("review-reminders", "review-2001")
    assert peek.get("status") != "scheduled" and not peek.get("found"), (
        f"the cancelled reminder is gone ({peek})")
    # A timer fires as a real message of its queue.
    await (
        queen.timer("review-reminders").key("review-2002").after_ms(200)
        .payload({"orderId": 2002}).schedule()
    )
    fired = []

    async def mail(m):
        fired.append(m["data"])

    await (
        queen.queue("review-reminders").group("mailer").subscription_mode("all").each().limit(1)
        .consume(mail)
    )
    assert fired == [{"orderId": 2002}], fired
    print("ok: uc-reminder")

    # --- 7. Request and reply between services (ephemeral queues) --------------
    # guide:start(uc-reply)
    # The pricing service answers quote requests, in memory.
    async def pricing():
        messages = (await queen.ephemeral.pop("quotes", group="pricing", wait=True))["messages"]
        for m in messages:
            reply = {"room": m["payload"]["room"], "price": 129}
            await queen.ephemeral.push(m["payload"]["replyTo"], [reply])
        await queen.ephemeral.ack("quotes", messages, group="pricing")

    # The booking page asks, and waits at most half a second for the answer.
    async def quote(room):
        inbox = f"inbox-{uuid.uuid4()}"
        await queen.ephemeral.push("quotes", [{"room": room, "replyTo": inbox}])
        reply = await queen.ephemeral.pop(inbox, wait=True, timeout=500,
                                          commit_on_delivery=True)
        if reply["messages"]:
            return reply["messages"][0]["payload"]
        return {"room": room, "price": None}            # too slow: show "ask us"

    deluxe, _ = await asyncio.gather(quote("deluxe"), pricing())
    print(deluxe)                                       # {'room': 'deluxe', 'price': 129}
    # guide:end
    assert deluxe == {"room": "deluxe", "price": 129}, deluxe
    t = time.monotonic()
    suite = await quote("suite")                        # nobody answers now
    assert suite == {"room": "suite", "price": None}, suite
    assert time.monotonic() - t < 3, "the caller gave up after its timeout"
    leftover = await queen.ephemeral.pop("quotes", group="pricing", wait=False)
    await queen.ephemeral.ack("quotes", leftover["messages"], group="pricing")
    print("ok: uc-reply")

    # --- 8. Calling a webhook (retries, dead letters, idempotency key) ----------
    receiver = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    threading.Thread(target=receiver.serve_forever, daemon=True).start()
    hook_url = f"http://127.0.0.1:{receiver.server_address[1]}/hooks"
    await queen.queue("webhooks").config({"retryLimit": 5, "retryDelay": 1000}).create()
    await queen.queue("webhooks").partition("customer-42").push([{
        "transactionId": "evt-9001",
        "data": {"url": hook_url, "event": {"type": "booking.confirmed", "id": 9001}},
    }])
    # guide:start(uc-webhook)
    import httpx

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)     # a deploy stops the sender cleanly
    hook_watch = asyncio.create_task(set_when(lambda: Receiver.received, stop))  # guide:hide

    async with httpx.AsyncClient() as client:
        async def call(msg):
            res = await client.post(
                msg["data"]["url"],
                json=msg["data"]["event"],
                headers={"idempotency-key": msg["transactionId"]},
            )
            res.raise_for_status()                        # retried, then dead-lettered

        await (
            queen.queue("webhooks")
            .group("sender")
            .subscription_mode("all")
            .each()
            .timeout_millis(1000)  # guide:hide
            .consume(call, signal=stop)
        )
    # guide:end
    hook_watch.cancel()
    loop.remove_signal_handler(signal.SIGTERM)
    receiver.shutdown()
    assert Receiver.calls == 2, f"the failed call was retried once ({Receiver.calls})"
    assert list(Receiver.received) == ["evt-9001"], "the receiver got the event under its key"
    print("ok: uc-webhook")

    # --- 9. A job only one worker runs (lock + guard) ---------------------------
    async def nightly_worker(name):
        # guide:start(uc-singleton)
        lock = queen.lock("nightly-report", ttl_seconds=30)
        if not await lock.acquire(wait=1):                # another worker has it
            return "skipped"
        try:
            report = {"day": "2026-10-24", "bookings": 412}
            result = await (
                queen.transaction()
                .guard(lock)                              # commits only while the lock is ours
                .queue("daily-reports")
                .push([{"transactionId": f"report-{report['day']}", "data": report}])
                .commit()
            )
            if not result:
                return "lost the lock"
            await asyncio.sleep(1.5)  # guide:hide
            return "done"
        finally:
            await lock.release()
        # guide:end

    outcomes = await asyncio.gather(nightly_worker("a"), nightly_worker("b"), nightly_worker("c"))
    assert outcomes.count("done") == 1, f"one worker ran it ({outcomes})"
    read = await queen.queue("daily-reports").fetch(["Default"])
    assert len(read["Default"]["records"]) == 1, "one report"
    print("ok: uc-singleton", json.dumps(outcomes))

    await queen.close()
    print("all use cases checked")


if __name__ == "__main__":
    asyncio.run(main())
