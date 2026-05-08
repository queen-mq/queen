"""
Stream._compile() + config_hash sanity tests. 1:1 port of configHash.test.js.
"""

from __future__ import annotations

import pytest

from queen.streams.stream import Stream


def fake_queue(name: str):
    """Minimal queue-builder stub: anything with a name suffices for compile."""
    class _FakeQB:
        _queue_name = name
        queue_name = name

    return _FakeQB()


class TestStreamChainValidation:
    """Group: 'Stream chain validation' (9 tests)."""

    def test_compiles_a_simple_stateless_chain(self):
        s = (
            Stream.from_(fake_queue("orders"))
            .map(lambda m: m.get("data"))
            .filter(lambda v: v.get("amount", 0) > 0)
            .to(fake_queue("orders.filtered"))
        )
        c = s._compile()
        assert len(c.stages.pre) == 2
        assert c.stages.sink.kind == "sink"
        assert c.stages.sink.queue_name == "orders.filtered"

    def test_compiles_a_windowed_aggregation_chain(self):
        s = (
            Stream.from_(fake_queue("orders"))
            .window_tumbling(seconds=60)
            .aggregate({"sum": lambda m: m.get("amount", 0)})
            .to(fake_queue("orders.totals"))
        )
        c = s._compile()
        assert c.stages.window is not None
        assert c.stages.reducer is not None
        assert c.stages.reducer.kind == "aggregate"

    def test_allows_post_reducer_stateless_ops(self):
        s = (
            Stream.from_(fake_queue("m"))
            .window_tumbling(seconds=30)
            .reduce(lambda a, m: a + m.get("value", 0), 0)
            .map(lambda v: {"v": v})
            .filter(lambda v: v["v"] > 0)
            .to(fake_queue("out"))
        )
        c = s._compile()
        assert len(c.stages.post) == 2

    def test_rejects_reduce_without_a_window(self):
        s = Stream.from_(fake_queue("x")).reduce(lambda a, m: a + m, 0)
        with pytest.raises(ValueError, match=r"reduce.*requires a preceding window"):
            s._compile()

    def test_rejects_double_window(self):
        s = (
            Stream.from_(fake_queue("x"))
            .window_tumbling(seconds=10)
            .window_tumbling(seconds=30)
        )
        with pytest.raises(ValueError, match=r"only one window operator"):
            s._compile()

    def test_rejects_sink_not_at_the_end(self):
        s = Stream.from_(fake_queue("x")).to(fake_queue("y")).map(lambda m: m)
        with pytest.raises(ValueError, match=r"sink operators.*must be the last"):
            s._compile()

    def test_produces_stable_config_hash_for_identical_chain_shapes(self):
        a = (
            Stream.from_(fake_queue("q"))
            .window_tumbling(seconds=60)
            .aggregate({"sum": lambda m: m["x"]})
            .to(fake_queue("out"))
        )
        b = (
            Stream.from_(fake_queue("q"))
            .window_tumbling(seconds=60)
            .aggregate({"sum": lambda m: m["y"]})  # different fn, same field
            .to(fake_queue("out"))
        )
        assert a._compile().config_hash == b._compile().config_hash

    def test_config_hash_differs_when_sink_queue_changes(self):
        a = Stream.from_(fake_queue("q")).map(lambda m: m).to(fake_queue("out-a"))
        b = Stream.from_(fake_queue("q")).map(lambda m: m).to(fake_queue("out-b"))
        assert a._compile().config_hash != b._compile().config_hash

    def test_config_hash_differs_when_aggregate_field_set_changes(self):
        a = (
            Stream.from_(fake_queue("q"))
            .window_tumbling(seconds=60)
            .aggregate({"sum": lambda m: 0})
            .to(fake_queue("out"))
        )
        b = (
            Stream.from_(fake_queue("q"))
            .window_tumbling(seconds=60)
            .aggregate({"sum": lambda m: 0, "count": lambda m: 1})
            .to(fake_queue("out"))
        )
        assert a._compile().config_hash != b._compile().config_hash
