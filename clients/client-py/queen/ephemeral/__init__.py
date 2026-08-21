"""Ephemeral (RAM-class) queues (EPHEMERAL_QUEUES.md §1, §3.1, §4)."""

from .ephemeral import (
    EPHEMERAL_QUEUE_NOT_FOUND,
    EPHEMERAL_UNSUPPORTED,
    EPHEMERAL_UNSUPPORTED_MESSAGE,
    Ephemeral,
)

__all__ = [
    "Ephemeral",
    "EPHEMERAL_UNSUPPORTED",
    "EPHEMERAL_UNSUPPORTED_MESSAGE",
    "EPHEMERAL_QUEUE_NOT_FOUND",
]
