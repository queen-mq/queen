"""Streams auto-link on a feed source (PLAN_ROUTING.md §6, §7): a ``.to()``
whose record is the feed message unchanged writes a ``link`` operation in the
commit's transaction instead of pushing a copy; anything transformed is still a
push; ``link=False`` turns it off; a broker that refuses links gets the same
commit with the records pushed."""

import pytest

from queen.streams.runtime.feed_runner import output_prefix
from queen.streams.stream import Stream

from .fake_broker import FakeBroker, fake_queen, fast, payloads, until, wait


def orders(b, n):
    for i in range(1, n + 1):
        b.push({"queue": "orders", "partition": f"c{i % 2}", "data": {"n": i, "country": "IT" if i % 3 else "FR"}})


def link_ops(b):
    return [ln for t in b.txns for ln in t["links"]]


async def test_an_unchanged_message_is_linked_with_its_id_and_transaction_id_not_copied():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 6)
    runner = await (Stream.from_feed(queen, queues=["orders"], from_="earliest")
                    .where("data.country", "=", "IT")
                    .filter(lambda m: m["data"]["n"] != 4)
                    .to(queen.queue("orders-it"))
                    .run(query_id="it-links", **fast()))
    await until(lambda: len(b.messages("orders-it")) >= 3)
    await wait(50)
    await runner.stop()

    src = {e["transactionId"]: e for e in b.messages("orders")}
    out = b.messages("orders-it")
    # One commit, grouped by target partition: the order holds inside each.
    assert sorted(e["payload"]["n"] for e in out) == [1, 2, 5]
    assert [e["payload"]["n"] for e in out if e["partition"] == "c1"] == [1, 5]
    assert all(e["kind"] == "link" for e in out), "links, not pushes"
    for e in out:
        s = src[e["transactionId"]]
        assert (e["id"], e["partition"]) == (s["id"], s["partition"]), "the same message, in its source partition's name"
        assert e["from"] == {"queue": "orders", "partition": s["partition"], "offset": s["offset"], "transactionId": s["transactionId"]}
        assert not e["transactionId"].startswith(output_prefix("it-links"))
    assert runner.metrics()["linked"] == 3
    assert runner.metrics()["outputs"] == 3
    # One link per message, in the commit of the position.
    assert len(link_ops(b)) == 3


async def test_a_transformed_record_is_pushed_and_link_false_pushes_copies():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 3)
    mapped = await (Stream.from_feed(queen, queues=["orders"], from_="earliest")
                    .map(lambda m: {"n": m["data"]["n"]})
                    .to(queen.queue("mapped"))
                    .run(query_id="mapped", **fast()))
    copied = await (Stream.from_feed(queen, queues=["orders"], from_="earliest")
                    .where("data.n", ">", 0)
                    .to(queen.queue("copied"), link=False)
                    .run(query_id="copied", **fast()))
    await until(lambda: len(b.messages("mapped")) >= 3 and len(b.messages("copied")) >= 3)
    await mapped.stop()
    await copied.stop()

    assert [e["kind"] for e in b.messages("mapped") + b.messages("copied")] == ["push"] * 6
    assert sorted(p["n"] for p in payloads(b, "mapped")) == [1, 2, 3]
    assert sorted(p["n"] for p in payloads(b, "copied")) == [1, 2, 3]
    assert all(e["transactionId"].startswith(output_prefix("copied")) for e in b.messages("copied"))
    assert link_ops(b) == []
    assert mapped.metrics()["linked"] == 0 and copied.metrics()["linked"] == 0


async def test_a_link_into_a_partition_that_holds_the_message_is_a_no_op_and_the_stream_goes_on():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 2)
    first = await Stream.from_feed(queen, queues=["orders"], from_="earliest").to(queen.queue("copy")).run(query_id="once", **fast())
    await until(lambda: len(b.messages("copy")) >= 2)
    await first.stop()
    # Started over: every message is linked again, into partitions that hold it.
    again = await (Stream.from_feed(queen, queues=["orders"], from_="earliest").to(queen.queue("copy"))
                   .run(query_id="once", reset="again", **fast()))
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 3}})
    await until(lambda: len(b.messages("copy")) >= 3)
    await wait(50)
    await again.stop()

    assert [p["n"] for p in payloads(b, "copy")] == [1, 2, 3], "each message once"
    assert again.metrics()["errors"] == 0 and again.metrics()["conflicts"] == 0
    assert any(ln["duplicate"] for ln in link_ops(b)), "the broker answered the second links as no-ops"


async def test_another_stream_reading_the_linked_target_sees_the_outputs_as_the_source_messages():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 3)
    router = await (Stream.from_feed(queen, queues=["orders"], from_="earliest")
                    .where("data.country", "=", "IT")
                    .to(queen.queue("orders-it"))
                    .run(query_id="it-router", **fast()))
    seen = []
    reader = await (Stream.from_feed(queen, queues=["orders-it"], from_="earliest")
                    .foreach(lambda value, ctx: seen.append((ctx["queue"], ctx["transactionId"], value)))
                    .run(query_id="it-reader", **fast()))
    await until(lambda: len(seen) >= 2)
    await wait(50)
    await router.stop()
    await reader.stop()

    src = {e["transactionId"]: e for e in b.messages("orders")}
    assert [e["kind"] for e in b.messages("orders-it")] == ["link", "link"]
    assert [(q, src[t]["payload"]) for q, t, _ in seen] == [("orders-it", {"n": 1, "country": "IT"}), ("orders-it", {"n": 2, "country": "IT"})]
    assert all(value == src[t]["payload"] for _, t, value in seen), "the source's payload, read through"


@pytest.mark.parametrize("queues", [["orders*"], None], ids=["a pattern matching the target", "no queues filter"])
async def test_a_stream_pushes_a_copy_into_a_queue_it_reads_and_never_reads_its_own_output_back(queues):
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "o1", "data": {"id": 1, "country": "IT"}})
    b.push({"queue": "orders", "partition": "o2", "data": {"id": 2, "country": "FR"}})
    source = {"queues": queues} if queues else {}
    runner = await (Stream.from_feed(queen, from_="earliest", **source)
                    .to(lambda order: f"orders-{order['country']}")
                    .run(query_id="self-reader", **fast()))
    await until(lambda: len(b.messages("orders-IT")) >= 1 and len(b.messages("orders-FR")) >= 1)
    await wait(100)
    await runner.stop()

    out = b.messages("orders-IT") + b.messages("orders-FR")
    assert [(e["kind"], e["payload"]["id"]) for e in out] == [("push", 1), ("push", 2)], "copies, once each"
    assert all(e["transactionId"].startswith(output_prefix("self-reader")) for e in out)
    m = runner.metrics()
    assert (m["entriesRead"], m["linked"], m["outputs"]) == (2, 0, 2), "its own outputs are never read back"


async def test_a_link_whose_source_is_gone_pushes_that_commit_only():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1)
    b.gone.add(b.messages("orders")[0]["transactionId"])
    runner = await Stream.from_feed(queen, queues=["orders"], from_="earliest").to(queen.queue("copy")).run(query_id="gone", **fast())
    await until(lambda: len(b.messages("copy")) >= 1)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 2}})
    await until(lambda: len(b.messages("copy")) >= 2)
    await runner.stop()

    first, second = b.messages("copy")
    assert (first["kind"], first["payload"]["n"]) == ("push", 1), "that commit, pushed"
    assert (second["kind"], second["payload"]["n"]) == ("link", 2), "the next one links again"
    m = runner.metrics()
    assert (m["linked"], m["errors"]) == (1, 0)


@pytest.mark.parametrize("refusal", ["unknown_op", "cluster_version"])
async def test_a_broker_that_refuses_links_gets_the_same_commit_pushed_and_hears_it_once(refusal):
    b = FakeBroker()
    b.links = refusal
    queen = fake_queen(b)
    orders(b, 2)
    heard = []
    runner = await (Stream.from_feed(queen, queues=["orders"], from_="earliest")
                    .to(queen.queue("copy"))
                    .run(query_id="old-broker", **fast(on_error=lambda err, ctx: heard.append((err, ctx)))))
    await until(lambda: len(b.messages("copy")) >= 2)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 3}})
    await until(lambda: len(b.messages("copy")) >= 3)
    await runner.stop()

    assert [p["n"] for p in payloads(b, "copy")] == [1, 2, 3]
    assert all(e["kind"] == "push" for e in b.messages("copy"))
    assert all(e["transactionId"].startswith(output_prefix("old-broker")) for e in b.messages("copy"))
    m = runner.metrics()
    assert (m["linked"], m["errors"], m["outputs"]) == (0, 0, 3)
    assert len(heard) == 1, "said once"
    err, ctx = heard[0]
    assert err.code == "feed_links_refused"
    assert ctx["action"] == "pushing"
    assert ctx["reason"] == ("bad_request" if refusal == "unknown_op" else "cluster_version")
