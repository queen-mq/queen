"""
Type definitions for Queen client
"""

from typing import Any, Dict, List, Optional, Union, Callable, Awaitable
from typing_extensions import TypedDict, Protocol


class Message(TypedDict, total=False):
    """Message from Queen.

    ``producerSub`` is the authenticated producer identity stamped by the server
    from the JWT ``sub`` claim at push time. It is present only when JWT
    authentication is enabled on the server. Clients CANNOT set this field on
    push - the server always derives it from the validated JWT, which is how
    impersonation is prevented.
    """

    transactionId: str
    partitionId: str
    leaseId: Optional[str]
    queue: str
    partition: str
    data: Dict[str, Any]
    createdAt: str
    errorMessage: Optional[str]
    retryCount: int
    producerSub: Optional[str]


class AckResponse(TypedDict, total=False):
    """Acknowledgment response"""

    success: bool
    error: Optional[str]


class BufferStats(TypedDict):
    """Buffer statistics"""

    activeBuffers: int
    totalBufferedMessages: int
    oldestBufferAge: float
    flushesPerformed: int


class DLQResponse(TypedDict):
    """Dead Letter Queue response"""

    messages: List[Message]
    total: int


class TransactionResponse(TypedDict, total=False):
    """Transaction response"""

    success: bool
    error: Optional[str]


# Type aliases for handlers
MessageHandler = Callable[[Message], Awaitable[Any]]
BatchMessageHandler = Callable[[List[Message]], Awaitable[Any]]
SuccessCallback = Callable[[Union[Message, List[Message]], Any], Awaitable[None]]
ErrorCallback = Callable[[Union[Message, List[Message]], Exception], Awaitable[None]]
DuplicateCallback = Callable[[List[Any], Exception], Awaitable[None]]


class TraceConfig(TypedDict, total=False):
    """Trace configuration"""

    traceName: Optional[Union[str, List[str]]]
    eventType: Optional[str]
    data: Dict[str, Any]


class TraceMethod(Protocol):
    """Protocol for message.trace() method"""

    async def __call__(self, trace_config: TraceConfig) -> Dict[str, Any]:
        ...


class MessageWithTrace(Message):
    """Message enhanced with trace method"""

    trace: TraceMethod


class Retry429Config(TypedDict, total=False):
    """Backoff policy for HTTP 429 (rate limited) responses from a
    queen_proxy deployment, separate from ``retry_attempts``/
    ``retry_delay_millis`` (which cover 5xx/network failures). Pass a plain
    dict with any subset of these keys as ``retry_429=`` -- omitted keys fall
    back to the defaults below.

    max_attempts: bounded retry cap. Defaults to 10 for ordinary requests
        (push, admin calls, non-waiting pop); a long-poll pop (wait=True)
        retries unboundedly (paced by backoff) unless max_attempts is set
        here, in which case it applies uniformly to both.
    base_ms: initial backoff (ms) used when the response has no Retry-After
        header. Default 500.
    cap_ms: ceiling (ms) for the exponential backoff delay. Default 30000.

    When the server does send a Retry-After header (seconds), it is honored
    (with +-20% jitter) instead of the computed exponential delay.
    """

    max_attempts: int
    base_ms: int
    cap_ms: int

