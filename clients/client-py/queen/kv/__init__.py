"""Key/value surface (PLAN_KV_TIMERS.md §5, §8.1)."""

from .kv import KV, KvResult
from . import ops

__all__ = ["KV", "KvResult", "ops"]
