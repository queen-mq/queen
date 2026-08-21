"""Buffer module for Queen client"""

from .buffer_manager import BufferManager
from .message_buffer import MessageBuffer, resolve_buffer_options
from .sinks import (
    DURABLE_DESTINATION,
    DURABLE_SINK,
    EPHEMERAL_SINK,
    Destination,
    Sink,
    durable_address,
    ephemeral_address,
    ephemeral_destination,
)

__all__ = [
    "BufferManager",
    "MessageBuffer",
    "resolve_buffer_options",
    "Sink",
    "Destination",
    "DURABLE_SINK",
    "EPHEMERAL_SINK",
    "DURABLE_DESTINATION",
    "durable_address",
    "ephemeral_address",
    "ephemeral_destination",
]
