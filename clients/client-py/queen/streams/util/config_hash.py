"""
config_hash — stable hash of an operator chain's structural shape.

Used to detect that the user redeployed a stream under the same query_id but
with a different operator chain. The server refuses such re-registrations
unless the user passes ``reset=True``.

Hash inputs (intentional):
  - operator kinds in order
  - structural config of each operator (window size, aggregate fields,
    sink queue name, ...)

Hash inputs (intentionally NOT included):
  - user-supplied callables (.map fn, .filter predicate, .reduce fn). We
    cannot stably serialise functions, and they often vary across deploys
    for trivial reasons. Hashing only the structural shape is a safety net
    for "you reordered the chain or changed window size", which is what
    actually corrupts state.

CROSS-LANGUAGE COMPATIBILITY: this implementation must produce the same
SHA-256 hex digest as the JS ``configHashOf`` and the Go ``ConfigHashOf`` for
the same logical operator chain. The wire format is:

    json.dumps([{"kind": op.kind, **op.config}, ...], separators=(',', ':'))

with stable key insertion order matching JS object literal order. See
tests/streams_unit/test_config_hash.py for cross-validation cases.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


def config_hash_of(operators: Iterable[Any]) -> str:
    """Return the SHA-256 hex digest of the operator chain's structural shape."""
    parts = [_describe(op) for op in operators]
    # NOTE: separators=(',', ':') matches JS JSON.stringify default (no spaces).
    payload = json.dumps(parts, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _describe(op: Any) -> dict:
    cfg = getattr(op, "config", None)
    if not isinstance(cfg, dict):
        cfg = {}
    return {"kind": op.kind, **cfg}


def _describe_feed(op: Any) -> dict:
    # The JS shape: `{kind, ...config}`. An operator whose feed reading of its
    # config differs from the queue runner's keeps it in `feed_config`.
    cfg = getattr(op, "feed_config", None)
    if not isinstance(cfg, dict):
        cfg = getattr(op, "config", None)
    if not isinstance(cfg, dict):
        cfg = {}
    return {"kind": op.kind, **cfg}


def js_config_hash_of(operators: Iterable[Any]) -> str:
    """``configHashOf`` of the JS SDK, byte for byte: sha256 over the
    ``JSON.stringify`` text of ``[{kind, ...config}, ...]`` (JS key order, JS
    number text: ``60`` never ``60.0``). The feed's chain hash is its first 16
    hex characters."""
    from .js import js_json_dumps

    payload = js_json_dumps([_describe_feed(op) for op in operators])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
