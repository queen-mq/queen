"""
Queen MQ — async message-queue client + fluent streaming SDK for Python.

Single import for everything:

    from queen import Queen, Stream, token_bucket_gate, sliding_window_gate

The streaming SDK lives under ``queen.streams.*``; broker-only users can
keep using ``Queen``/``Admin`` and ignore the rest.
"""

from .admin import Admin
from .builders.transaction_builder import TransactionResult
from .client import Queen
from .errors import KvError, QueenError, QueenHttpError, TimerError
from .kv import KV, KvResult
from .streams.stream import Stream
from .streams.helpers.rate_limiter import token_bucket_gate, sliding_window_gate
from .timers import TimerBuilder, TimerResult, Timers
from .types import Message, AckResponse, BufferStats, DLQResponse, TransactionResponse
from .utils.defaults import (
    CLIENT_DEFAULTS,
    QUEUE_DEFAULTS,
    CONSUME_DEFAULTS,
    POP_DEFAULTS,
    BUFFER_DEFAULTS,
)

__version__ = "1.0.3"

__all__ = [
    "Queen",
    "Admin",
    "KV",
    "KvResult",
    "Timers",
    "TimerBuilder",
    "TimerResult",
    "TransactionResult",
    "QueenError",
    "QueenHttpError",
    "KvError",
    "TimerError",
    "Stream",
    "token_bucket_gate",
    "sliding_window_gate",
    "Message",
    "AckResponse",
    "BufferStats",
    "DLQResponse",
    "TransactionResponse",
    "CLIENT_DEFAULTS",
    "QUEUE_DEFAULTS",
    "CONSUME_DEFAULTS",
    "POP_DEFAULTS",
    "BUFFER_DEFAULTS",
]

