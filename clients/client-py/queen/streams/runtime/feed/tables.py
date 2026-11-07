"""
``.join_table()`` on the tenant feed: the latest row per key of a table queue,
looked up for every message that reaches the step. Port of the JS SDK's
runtime/feed/tables.js.

A table queue holds one message per change: its key (``key(row)``, the
partition by default) and value (``value(row)``, the payload by default); a
value of None deletes the row. Rows come from the same feed as the stream's
messages, in one order, so a message sees the table exactly as of its own
position: every change committed before it, none after (a change in the same
transaction counts as before).

State key ``t\\u001f<key>``; a value is ``{v, p}``: the row and its position.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List

from ...util.js import UNDEFINED, call_user, js_get, js_str, nullish
from .patterns import clone, message_of
from .state import FeedState

SEP = "\u001f"


class TableStep:
    def __init__(self, *, op: Any, state: FeedState) -> None:
        self._op = op
        self._state = state
        self.kind = "table"

    @property
    def queues(self) -> List[str]:
        return self._op.queues

    @property
    def initial(self) -> str:
        return self._op.initial

    @property
    def open(self) -> int:
        return self._state.size

    async def apply_row(self, entry: Dict[str, Any]) -> None:
        """A message of the table's queue: the row it sets, or deletes."""
        op = self._op
        key = await call_user(op.key_fn, entry) if op.key_fn else js_get(entry, "partition")
        if nullish(key):
            return
        value = await call_user(op.value_fn, entry) if op.value_fn else js_get(entry, "data")
        sk = f"t{SEP}{js_str(key)}"
        if nullish(value):
            self._state.delete(sk)
        else:
            self._state.set(sk, {"v": value, "p": js_get(entry, "position")})

    async def apply(self, env: Dict[str, Any], t: Any) -> List[Dict[str, Any]]:
        """A stream message: the envelope with the row joined in, or none when the row is missing and missing is 'drop'."""
        op = self._op
        msg = message_of(env, t)
        value = env.get("value", UNDEFINED)
        key = await call_user(op.on_fn, msg, value)
        found = UNDEFINED if nullish(key) else self._state.get(f"t{SEP}{js_str(key)}")
        row = clone(found.get("v", UNDEFINED)) if found is not UNDEFINED else UNDEFINED
        if row is UNDEFINED and op.missing == "drop":
            return []
        out = await call_user(op.select_fn, value, row, msg) if op.select_fn else {"value": value, "row": row}
        if nullish(out):
            return []
        return [{**env, "value": out}]

    def inspect(self, key: Any) -> Any:
        found = self._state.get(f"t{SEP}{js_str(key)}")
        return {"key": js_str(key), "row": clone(found.get("v", UNDEFINED)), "position": found.get("p", UNDEFINED)} if found is not UNDEFINED else None

    def list(self) -> Iterator[Dict[str, Any]]:
        for k, v in self._state.items_view():
            yield {"key": k[2:], "row": clone(v.get("v", UNDEFINED)), "position": v.get("p", UNDEFINED)}
