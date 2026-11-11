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
from .ephemeral import (
    EPHEMERAL_QUEUE_NOT_FOUND,
    EPHEMERAL_UNSUPPORTED,
    EPHEMERAL_UNSUPPORTED_MESSAGE,
    Ephemeral,
)
from .errors import (
    ConflationUnsupportedError,
    EphemeralError,
    EphemeralQueueNotFoundError,
    KvError,
    LockError,
    LockNotHeldError,
    QueenError,
    QueenHttpError,
    TimerError,
    TokenSourceError,
    TransactionError,
)
from .feed import FeedReader
from .kv import KV, KvResult
from .locks import Lock, LockResult, Locks
from .reads import ChangedPartition, ChangedPartitions, FetchedPartition, FetchedRecord
from .streams.stream import Stream
from .streams.helpers.rate_limiter import token_bucket_gate, sliding_window_gate
from .timers import TimerBuilder, TimerResult, Timers
from .tokens import Grant, MintTokenResponse, TokenSource, Tokens
from .types import Message, AckResponse, BufferStats, DLQResponse, TransactionResponse
from .utils.defaults import (
    CLIENT_DEFAULTS,
    QUEUE_DEFAULTS,
    CONSUME_DEFAULTS,
    POP_DEFAULTS,
    BUFFER_DEFAULTS,
)

__version__ = "2.8.1"

__all__ = [
    "Queen",
    "Admin",
    "KV",
    "KvResult",
    "Lock",
    "Locks",
    "LockResult",
    "Ephemeral",
    "EPHEMERAL_UNSUPPORTED",
    "EPHEMERAL_UNSUPPORTED_MESSAGE",
    "EPHEMERAL_QUEUE_NOT_FOUND",
    "Timers",
    "TimerBuilder",
    "TimerResult",
    "TransactionResult",
    "Tokens",
    "TokenSource",
    "MintTokenResponse",
    "Grant",
    "QueenError",
    "QueenHttpError",
    "KvError",
    "LockError",
    "LockNotHeldError",
    "TimerError",
    "TokenSourceError",
    "TransactionError",
    "FeedReader",
    "EphemeralError",
    "EphemeralQueueNotFoundError",
    "ConflationUnsupportedError",
    "Stream",
    "token_bucket_gate",
    "sliding_window_gate",
    "Message",
    "AckResponse",
    "BufferStats",
    "DLQResponse",
    "TransactionResponse",
    "FetchedRecord",
    "FetchedPartition",
    "ChangedPartition",
    "ChangedPartitions",
    "CLIENT_DEFAULTS",
    "QUEUE_DEFAULTS",
    "CONSUME_DEFAULTS",
    "POP_DEFAULTS",
    "BUFFER_DEFAULTS",
]

