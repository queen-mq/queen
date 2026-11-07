"""
The state of one stateful step of a feed stream: its open windows, sessions,
waiting messages or table rows, by state key, in memory. Port of the JS SDK's
runtime/feed/state.js.

It is not written to KV at every commit. A commit writes the position; now
and then the runner writes a snapshot of every step's state (snapshot.py), and
a restart loads the last snapshot and reads the feed again from the
snapshot's position to the committed one, sending nothing it already sent.

Every change is recorded so that it can be undone: a function that throws
leaves no trace (its message is retried, skipped or dead-lettered against the
state from before it), and a commit that did not happen rolls back to the
last one that did.
"""

from __future__ import annotations

import heapq
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from ...util.js import UNDEFINED, js_sort_key, js_str

__all__ = [
    "FeedState",
    "DueHeap",
    "BUFFER_PREFIX",
    "REBUILD_BATCH",
    "CLOSE_BATCH",
    "is_buffer_key",
    "fnv1a32",
    "shard_of",
]

# The messages an event-time step holds until its watermark reaches them
# (clock.py) are entries of the step's state under this prefix; the engines
# leave them alone.
BUFFER_PREFIX = "b\u001f"
# Entries indexed between two turns of the event loop when a loaded state is indexed again.
# JS indexes 50,000 between turns; Python takes about ten times as long per entry, so it turns
# every 10,000 to keep each pause as short (the order, and so the result, is the same).
REBUILD_BATCH = 10_000
# Windows, deadlines or runs closed in one go; then the results run, then the next batch.
CLOSE_BATCH = 2_000

_MISSING = object()


class FeedState:
    __slots__ = ("_map", "_undo", "_changed", "_touched", "_classify", "_counts")

    def __init__(self) -> None:
        self._map: Dict[str, Any] = {}
        self._undo: List[Tuple[str, bool, Any]] = []
        # Anything set or deleted since the last snapshot: the next one has work.
        self._changed = False
        # The keys set or deleted since the last snapshot: what a delta snapshot writes.
        self._touched: Dict[str, None] = {}
        # How many entries of each class the state holds (`classify`), kept as it changes.
        self._classify: Optional[Callable[[str, Any], Any]] = None
        self._counts: Dict[Any, int] = {}

    @property
    def size(self) -> int:
        return len(self._map)

    @property
    def dirty(self) -> bool:
        """Something changed since the last commit."""
        return len(self._undo) > 0

    @property
    def changed_since_snapshot(self) -> bool:
        return self._changed

    @property
    def touched_since_snapshot(self) -> int:
        """How many keys were set or deleted since the last snapshot (some may be back as they were)."""
        return len(self._touched)

    def snapshot_taken(self) -> None:
        self._changed = False
        self._touched.clear()

    def classify_with(self, fn: Optional[Callable[[str, Any], Any]]) -> None:
        """Count the entries by class: ``fn(key, value)`` answers a class name or None."""
        self._classify = fn if callable(fn) else None
        self._recount()

    def count(self, name: Any) -> int:
        return self._counts.get(name, 0)

    def get(self, key: str) -> Any:
        """The value, or UNDEFINED."""
        return self._map.get(key, UNDEFINED)

    def has(self, key: str) -> bool:
        return key in self._map

    def entries(self) -> Iterator[Tuple[str, Any]]:
        return iter(list(self._map.items()))

    def items_view(self) -> Any:
        return self._map.items()

    def set(self, key: str, value: Any) -> None:
        """Store ``value`` (a fresh object: stored values are never changed in place)."""
        prev = self._map.get(key, _MISSING)
        had = prev is not _MISSING
        self._undo.append((key, had, prev if had else None))
        if had:
            self._bump(key, prev, -1)
        self._map[key] = value
        self._bump(key, value, 1)
        self._changed = True
        self._touched[key] = None

    def delete(self, key: str) -> None:
        prev = self._map.get(key, _MISSING)
        if prev is _MISSING:
            return
        self._undo.append((key, True, prev))
        self._bump(key, prev, -1)
        del self._map[key]
        self._changed = True
        self._touched[key] = None

    def mark(self) -> int:
        """A point to roll back to."""
        return len(self._undo)

    def rollback_to(self, mark: int) -> None:
        undo = self._undo
        m = self._map
        while len(undo) > mark:
            key, had, prev = undo.pop()
            cur = m.get(key, _MISSING)
            if cur is not _MISSING:
                self._bump(key, cur, -1)
            if had:
                m[key] = prev
                self._bump(key, prev, 1)
            elif cur is not _MISSING:
                del m[key]
            # The key stays touched: a delta writes it as it is now.

    def discard(self) -> None:
        """Back to the state of the last commit."""
        self.rollback_to(0)

    def committed(self) -> None:
        """The changes are committed: they are the new starting point."""
        self._undo = []

    def dump(self) -> List[List[Any]]:
        """Every entry, for a snapshot: ``[[key, value], ...]``."""
        return [[k, v] for k, v in self._map.items()]

    def touched(self) -> Iterator[Tuple[str, Any]]:
        """What a delta snapshot writes: each key touched since the last snapshot,
        as ``(key, value)`` when it is there and ``(key, UNDEFINED)`` when it is gone."""
        for key in list(self._touched):
            yield key, self._map.get(key, UNDEFINED)

    def load(self, entries: Any) -> None:
        """Replace everything with a snapshot's entries; nothing to undo."""
        self._map = {}
        if isinstance(entries, list):
            for kv in entries:
                if isinstance(kv, (list, tuple)) and len(kv) >= 1:
                    self._map[kv[0] if isinstance(kv[0], str) else js_str(kv[0])] = kv[1] if len(kv) > 1 else UNDEFINED
        self._undo = []
        self._changed = False
        self._touched.clear()
        self._recount()

    def load_begin(self) -> None:
        """A load in pieces: start empty, then load_set / load_delete each piece, then loaded()."""
        self._map = {}
        self._undo = []
        self._counts = {}

    def load_set(self, key: str, value: Any) -> None:
        prev = self._map.get(key, _MISSING)
        if prev is not _MISSING:
            self._bump(key, prev, -1)
        self._map[key] = value
        self._bump(key, value, 1)

    def load_delete(self, key: str) -> None:
        prev = self._map.get(key, _MISSING)
        if prev is _MISSING:
            return
        self._bump(key, prev, -1)
        del self._map[key]

    def loaded(self) -> None:
        """The pieces are in: the state is as the snapshot holds it, nothing to undo or to write."""
        self._undo = []
        self._changed = False
        self._touched.clear()

    def wipe(self) -> None:
        """Start empty (a reset): the next snapshot writes the empty state."""
        self._map = {}
        self._undo = []
        self._changed = True
        self._touched.clear()
        self._counts = {}

    def _bump(self, key: str, value: Any, d: int) -> None:
        if self._classify is None:
            return
        c = self._classify(key, value)
        if c is None or c is UNDEFINED:
            return
        self._counts[c] = self._counts.get(c, 0) + d

    def _recount(self) -> None:
        self._counts = {}
        if self._classify is None:
            return
        for k, v in self._map.items():
            self._bump(k, v, 1)


def is_buffer_key(key: Any) -> bool:
    return isinstance(key, str) and key.startswith(BUFFER_PREFIX)


class DueHeap:
    """A min-heap of ``(due, key)``: the next window or deadline to close. Ties
    close in key order (UTF-16 code units, as JS compares strings)."""

    __slots__ = ("_a",)

    def __init__(self) -> None:
        self._a: List[Tuple[Any, str, str]] = []

    @property
    def size(self) -> int:
        return len(self._a)

    def __len__(self) -> int:
        return len(self._a)

    def clear(self) -> None:
        self._a = []

    def peek(self) -> Optional[Tuple[Any, str]]:
        if not self._a:
            return None
        due, _, key = self._a[0]
        return due, key

    def push(self, due: Any, key: str) -> None:
        heapq.heappush(self._a, (due, js_sort_key(key), key))

    def pop(self) -> Optional[Tuple[Any, str]]:
        if not self._a:
            return None
        due, _, key = heapq.heappop(self._a)
        return due, key


def fnv1a32(text: Any) -> int:
    """FNV-1a, 32 bit, over the UTF-8 bytes of ``text``: the hash the broker
    picks a shard by (feed.rs ``fnv1a32``), so both sides agree on where a
    partition is."""
    s = text if isinstance(text, str) else js_str(text)
    try:
        data = s.encode("utf-8")
    except UnicodeEncodeError:
        data = "".join("�" if 0xD800 <= ord(ch) <= 0xDFFF else ch for ch in s).encode("utf-8")
    h = 0x811C9DC5
    for b in data:
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def shard_of(key: Any, count: int) -> int:
    """The shard of ``count`` that a key (a partition name, by default) belongs to."""
    return fnv1a32(key) % max(1, count)
