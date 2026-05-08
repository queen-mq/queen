"""
Queen MQ — async message-queue client + fluent streaming SDK for Python.

Single import for everything:

    from queen import Queen, Stream, token_bucket_gate, sliding_window_gate

The streaming SDK lives under ``queen.streams.*``; broker-only users can
keep using ``Queen``/``Admin`` and ignore the rest.
"""

from .admin import Admin
from .client import Queen
from .streams.stream import Stream
from .streams.helpers.rate_limiter import token_bucket_gate, sliding_window_gate
from .types import Message, AckResponse, BufferStats, DLQResponse, TransactionResponse
from .utils.defaults import (
    CLIENT_DEFAULTS,
    QUEUE_DEFAULTS,
    CONSUME_DEFAULTS,
    POP_DEFAULTS,
    BUFFER_DEFAULTS,
)

__version__ = "0.15.0"

__all__ = [
    "Queen",
    "Admin",
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

