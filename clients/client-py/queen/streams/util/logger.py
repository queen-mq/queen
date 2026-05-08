"""
Tiny logger. Off by default; enable via QUEEN_STREAMS_LOG=1 or by passing
a custom logger to Stream.run().
"""

from __future__ import annotations

import os
import sys
from typing import Any, Optional


_ENABLED = os.environ.get("QUEEN_STREAMS_LOG", "").lower() in ("1", "true", "yes")


class _DefaultLogger:
    @staticmethod
    def info(msg: str, ctx: Optional[Any] = None) -> None:
        if _ENABLED:
            if ctx is not None:
                print(f"[queen-streams] {msg}", ctx)
            else:
                print(f"[queen-streams] {msg}")

    @staticmethod
    def warn(msg: str, ctx: Optional[Any] = None) -> None:
        if ctx is not None:
            print(f"[queen-streams] WARN {msg}", ctx, file=sys.stderr)
        else:
            print(f"[queen-streams] WARN {msg}", file=sys.stderr)

    @staticmethod
    def error(msg: str, ctx: Optional[Any] = None) -> None:
        if ctx is not None:
            print(f"[queen-streams] ERROR {msg}", ctx, file=sys.stderr)
        else:
            print(f"[queen-streams] ERROR {msg}", file=sys.stderr)


def make_logger(custom: Optional[Any] = None) -> Any:
    if custom is not None and hasattr(custom, "info") and callable(custom.info):
        return custom
    return _DefaultLogger
