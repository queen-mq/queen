"""
Buffer option resolution: the bound is always a bound.

`max_size` absent or 0 resolves to 4 x message_count -- NOT to infinity.
Unbounded is what lost 20.9M messages in the 2026-08-20 measurement, so it is
deliberately not expressible through the config, and a max_size below
message_count is floored up to it (a buffer that cannot reach its own flush
threshold is not a configuration, it is a typo).
"""

from __future__ import annotations

from queen.buffer.message_buffer import MessageBuffer, resolve_buffer_options
from queen.utils.defaults import BUFFER_DEFAULTS


def test_defaults_carry_a_bound_and_a_retry_delay():
    assert BUFFER_DEFAULTS["message_count"] == 100
    assert BUFFER_DEFAULTS["time_millis"] == 1000
    assert BUFFER_DEFAULTS["max_size"] == 400
    assert BUFFER_DEFAULTS["max_size"] == 4 * BUFFER_DEFAULTS["message_count"]
    assert BUFFER_DEFAULTS["retry_delay_millis"] == 250


def test_absent_options_resolve_to_the_bounded_defaults():
    for options in (None, {}):
        resolved = resolve_buffer_options(options)
        assert resolved["message_count"] == 100
        assert resolved["time_millis"] == 1000
        assert resolved["max_size"] == 400
        assert resolved["retry_delay_millis"] == 250


def test_zero_max_size_is_the_default_bound_not_unbounded():
    assert resolve_buffer_options({"max_size": 0})["max_size"] == 400
    assert resolve_buffer_options({"max_size": None})["max_size"] == 400
    assert resolve_buffer_options({"max_size": -1})["max_size"] == 400


def test_default_bound_follows_message_count():
    assert resolve_buffer_options({"message_count": 50})["max_size"] == 200
    assert resolve_buffer_options({"message_count": 1000, "max_size": 0})["max_size"] == 4000


def test_max_size_is_floored_at_message_count():
    resolved = resolve_buffer_options({"message_count": 100, "max_size": 10})
    assert resolved["max_size"] == 100
    assert resolve_buffer_options({"message_count": 1000, "max_size": 999})["max_size"] == 1000


def test_zero_retry_delay_resolves_to_the_default():
    assert resolve_buffer_options({"retry_delay_millis": 0})["retry_delay_millis"] == 250
    assert resolve_buffer_options({"retry_delay_millis": 5})["retry_delay_millis"] == 5


def test_other_options_survive_resolution():
    resolved = resolve_buffer_options({"message_count": 7, "time_millis": 25})
    assert resolved["message_count"] == 7
    assert resolved["time_millis"] == 25


def test_buffer_applies_the_resolved_options():
    buffer = MessageBuffer("test-buffer/Default", {"message_count": 25}, lambda address: None)
    assert buffer.options["max_size"] == 100
    assert buffer.max_size == 100
    assert buffer.retry_delay_seconds == 0.25
    assert buffer.message_count == 0  # occupancy, not the option
