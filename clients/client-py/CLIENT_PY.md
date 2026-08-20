# Queen Python Client - Implementation Plan

**Goal:** Build a Python 3 client with 100% feature parity, correctness, and behavioral equivalence to the Node.js client.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Module Structure](#module-structure)
3. [Core Components](#core-components)
4. [Configuration & Defaults](#configuration--defaults)
5. [Implementation Details](#implementation-details)
6. [API Surface](#api-surface)
7. [Testing Strategy](#testing-strategy)
8. [Edge Cases & Gotchas](#edge-cases--gotchas)
9. [Python-Specific Considerations](#python-specific-considerations)
10. [Implementation Checklist](#implementation-checklist)

---

## Architecture Overview

### Design Principles

1. **Fluent API with Method Chaining** - Use builder pattern for ergonomic API
2. **Async/Await Throughout** - Use `asyncio` for all I/O operations
3. **Type Hints** - Full type annotations for IDE support and correctness
4. **Context Managers** - Support `async with` for graceful shutdown
5. **Thread-Safe** - All operations must be thread-safe for concurrent usage

### Python Version Requirements

- **Minimum:** Python 3.8 (for `typing.Protocol`, `typing.Literal`)
- **Recommended:** Python 3.11+ (better async performance)

### Key Dependencies

```python
# pyproject.toml or requirements.txt
httpx >= 0.27.0        # Modern async HTTP client (replaces requests/aiohttp)
uuid >= 1.30           # UUID generation (use uuid7 if available)
typing-extensions      # Backport typing features for 3.8-3.10
```

---

## Module Structure

```
clients/client-py/
├── queen/
│   ├── __init__.py                  # Public API exports
│   ├── client.py                    # Queen main class
│   ├── http/
│   │   ├── __init__.py
│   │   ├── http_client.py           # HttpClient with retry/failover
│   │   └── load_balancer.py         # LoadBalancer with affinity
│   ├── buffer/
│   │   ├── __init__.py
│   │   ├── buffer_manager.py        # BufferManager
│   │   └── message_buffer.py        # MessageBuffer
│   ├── builders/
│   │   ├── __init__.py
│   │   ├── queue_builder.py         # QueueBuilder
│   │   ├── transaction_builder.py   # TransactionBuilder
│   │   ├── push_builder.py          # PushBuilder (for callbacks)
│   │   ├── consume_builder.py       # ConsumeBuilder (for callbacks)
│   │   ├── operation_builder.py     # OperationBuilder (create/delete)
│   │   └── dlq_builder.py           # DLQBuilder
│   ├── consumer/
│   │   ├── __init__.py
│   │   └── consumer_manager.py      # ConsumerManager
│   ├── stream/
│   │   ├── __init__.py
│   │   ├── stream_builder.py        # StreamBuilder
│   │   ├── stream_consumer.py       # StreamConsumer
│   │   └── window.py                # Window utility class
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── defaults.py              # All default constants
│   │   ├── logger.py                # Logging utility
│   │   └── validation.py            # Validation utilities
│   └── types.py                     # Type definitions (TypedDict, Protocol, etc.)
├── tests/
│   └── (mirror structure of queen/)
├── examples/
│   └── (port all Node.js examples)
├── pyproject.toml                   # Modern Python packaging
├── README.md
└── LICENSE

```

---

## Core Components

### 1. Queen (Main Entry Point)

**File:** `queen/client.py`

```python
from typing import Union, List, Optional, Dict, Any
from .builders.queue_builder import QueueBuilder
from .builders.transaction_builder import TransactionBuilder
from .stream.stream_builder import StreamBuilder
from .stream.stream_consumer import StreamConsumer
from .http.http_client import HttpClient
from .http.load_balancer import LoadBalancer
from .buffer.buffer_manager import BufferManager
from .utils.defaults import CLIENT_DEFAULTS
from .utils.validation import validate_url, validate_urls
import signal
import asyncio

class Queen:
    """Queen Message Queue Client - Version 2"""
    
    def __init__(
        self,
        config: Union[str, List[str], Dict[str, Any]] = None,
        *,
        # Allow keyword-only args for explicit configuration
        urls: Optional[List[str]] = None,
        url: Optional[str] = None,
        timeout_millis: int = CLIENT_DEFAULTS["timeout_millis"],
        retry_attempts: int = CLIENT_DEFAULTS["retry_attempts"],
        retry_delay_millis: int = CLIENT_DEFAULTS["retry_delay_millis"],
        load_balancing_strategy: str = CLIENT_DEFAULTS["load_balancing_strategy"],
        affinity_hash_ring: int = CLIENT_DEFAULTS["affinity_hash_ring"],
        enable_failover: bool = CLIENT_DEFAULTS["enable_failover"],
        health_retry_after_millis: int = CLIENT_DEFAULTS["health_retry_after_millis"]
    ):
        """
        Initialize Queen client.
        
        Args:
            config: Single URL, list of URLs, or config dict
            urls: List of server URLs (keyword-only)
            url: Single server URL (keyword-only)
            timeout_millis: Request timeout in milliseconds
            retry_attempts: Number of retry attempts
            retry_delay_millis: Initial retry delay (exponential backoff)
            load_balancing_strategy: 'affinity', 'round-robin', or 'session'
            affinity_hash_ring: Virtual nodes per server for affinity
            enable_failover: Enable automatic failover
            health_retry_after_millis: Retry unhealthy backends after N ms
        """
        # Implementation details...
        
    def queue(self, name: Optional[str] = None) -> QueueBuilder:
        """Get a queue builder for fluent API"""
        
    def transaction(self) -> TransactionBuilder:
        """Start a new transaction builder"""
        
    async def ack(
        self, 
        message: Union[Any, List[Any]], 
        status: Union[bool, str] = True,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Acknowledge one or more messages"""
        
    async def renew(self, message_or_lease_id: Union[str, Any, List[Any]]) -> Dict[str, Any]:
        """Renew lease for one or more messages"""
        
    async def flush_all_buffers(self) -> None:
        """Flush all buffered messages"""
        
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        
    async def delete_consumer_group(
        self, 
        consumer_group: str, 
        delete_metadata: bool = True
    ) -> Dict[str, Any]:
        """Delete a consumer group"""
        
    async def update_consumer_group_timestamp(
        self,
        consumer_group: str,
        timestamp: str
    ) -> Dict[str, Any]:
        """Update subscription timestamp for a consumer group"""
        
    def stream(self, name: str, namespace: str) -> StreamBuilder:
        """Define a stream for windowed processing"""
        
    def consumer(self, stream_name: str, consumer_group: str) -> StreamConsumer:
        """Create a consumer for a stream"""
        
    async def close(self) -> None:
        """Gracefully shutdown the client"""
        
    async def __aenter__(self):
        """Support async context manager"""
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup on context exit"""
        await self.close()
```

**Key Implementation Notes:**
- Private attributes use single underscore (Python convention)
- Setup signal handlers for SIGINT/SIGTERM
- Store signal handler references for cleanup
- Normalize config in `_normalize_config()` method
- Create HttpClient with load balancer if multiple URLs
- Initialize BufferManager with HttpClient reference

---

### 2. HttpClient

**File:** `queen/http/http_client.py`

```python
import httpx
import asyncio
from typing import Optional, Any, Dict
from ..utils.logger import log, error as log_error, warn as log_warn

class HttpClient:
    """HTTP client with retry, load balancing, and failover support"""
    
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        load_balancer: Optional['LoadBalancer'] = None,
        timeout_millis: int = 30000,
        retry_attempts: int = 3,
        retry_delay_millis: int = 1000,
        enable_failover: bool = True
    ):
        """Initialize HTTP client"""
        self._base_url = base_url
        self._load_balancer = load_balancer
        self._timeout_millis = timeout_millis
        self._retry_attempts = retry_attempts
        self._retry_delay_millis = retry_delay_millis
        self._enable_failover = enable_failover
        
        # Create httpx.AsyncClient (persistent connection pool)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_millis / 1000.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=100)
        )
        
    async def get(
        self, 
        path: str, 
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None
    ) -> Any:
        """GET request"""
        
    async def post(
        self, 
        path: str, 
        body: Optional[Dict[str, Any]] = None,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None
    ) -> Any:
        """POST request"""
        
    async def put(
        self, 
        path: str, 
        body: Optional[Dict[str, Any]] = None,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None
    ) -> Any:
        """PUT request"""
        
    async def delete(
        self, 
        path: str,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None
    ) -> Any:
        """DELETE request"""
        
    async def _request_with_failover(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int],
        affinity_key: Optional[str]
    ) -> Any:
        """Execute request with failover logic"""
        
    async def _request_with_retry(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int]
    ) -> Any:
        """Execute request with retry logic"""
        
    async def _execute_request(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int]
    ) -> Any:
        """Execute single HTTP request"""
        
    def _get_url(self) -> str:
        """Get URL from load balancer or base URL"""
        
    def get_load_balancer(self) -> Optional['LoadBalancer']:
        """Get load balancer instance"""
        
    async def close(self) -> None:
        """Close HTTP client and connection pool"""
        await self._client.aclose()
```

**Key Implementation Notes:**
- Use `httpx.AsyncClient` for connection pooling
- Handle 204 No Content → return `None`
- Parse JSON responses, handle empty responses
- Don't retry on 4xx errors
- Exponential backoff: `delay = retry_delay_millis * (2 ** attempt)`
- Timeout errors: catch `httpx.TimeoutException`
- Network errors: catch `httpx.NetworkError`, `httpx.ConnectError`
- Mark backends unhealthy on 5xx/network errors
- Use `httpx.Timeout` for per-request timeouts

---

### 3. LoadBalancer

**File:** `queen/http/load_balancer.py`

```python
from typing import List, Dict, Optional, Tuple
import time

class LoadBalancer:
    """Load balancer with round-robin, session, and affinity strategies"""
    
    def __init__(
        self,
        urls: List[str],
        strategy: str = 'round-robin',
        *,
        affinity_hash_ring: int = 128,
        health_retry_after_millis: int = 30000
    ):
        """Initialize load balancer"""
        self._urls = [url.rstrip('/') for url in urls]
        self._strategy = strategy
        self._current_index = 0
        self._session_map: Dict[str, int] = {}
        self._session_id = f"session_{int(time.time())}_{id(self)}"
        self._affinity_hash_ring = affinity_hash_ring
        self._health_retry_after_millis = health_retry_after_millis
        
        # Health status: url -> {'healthy': bool, 'failures': int, 'last_failure': float}
        self._health_status = {
            url: {'healthy': True, 'failures': 0, 'last_failure': None}
            for url in self._urls
        }
        
        # Virtual nodes for affinity strategy
        self._virtual_nodes: Optional[List[Dict[str, Any]]] = None
        
        if strategy == 'affinity':
            self._build_virtual_node_ring()
    
    def get_next_url(self, session_key: Optional[str] = None) -> str:
        """Get next URL based on strategy"""
        
    def mark_unhealthy(self, url: str) -> None:
        """Mark a backend as unhealthy"""
        
    def mark_healthy(self, url: str) -> None:
        """Mark a backend as healthy"""
        
    def get_health_status(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all backends"""
        
    def get_all_urls(self) -> List[str]:
        """Get all URLs"""
        
    def get_strategy(self) -> str:
        """Get load balancing strategy"""
        
    def get_virtual_node_count(self) -> int:
        """Get number of virtual nodes in the ring"""
        
    def reset(self) -> None:
        """Reset load balancer state"""
        
    def _build_virtual_node_ring(self) -> None:
        """Build virtual node ring for consistent hashing"""
        
    def _rebuild_virtual_node_ring(self, urls: Optional[List[str]] = None) -> None:
        """Rebuild virtual node ring with specified URLs"""
        
    def _get_affinity_url(self, key: str, healthy_urls: List[str]) -> str:
        """Get URL using affinity-based routing"""
        
    def _hash_string(self, s: str) -> int:
        """FNV-1a hash function for consistent hashing"""
```

**Key Implementation Notes:**
- FNV-1a hash: Start with `2166136261`, XOR char, multiply by factors, return unsigned 32-bit
- Virtual nodes sorted by hash value (ascending)
- Binary search to find first vnode >= keyHash
- Walk forward on ring to find first healthy server
- Rebuild ring when backends become unhealthy/healthy
- Health retry: Allow retry after `health_retry_after_millis` passes
- Use `time.time()` for last_failure timestamps (seconds since epoch)

---

### 4. BufferManager & MessageBuffer

**Files:** `queen/buffer/buffer_manager.py`, `queen/buffer/message_buffer.py`

The original plan sketched class skeletons here; the implementation has since moved past them
(the 2026-08-20 backpressure rework changed the API surface), so the source files and their
docstrings are the reference now. What the plan still owes you is the contract:

- `MessageBuffer.add()` is **async** and applies backpressure: at `max_size` waiting messages
  (default `4 * message_count`, unbounded not expressible) it parks on an `asyncio.Condition`
  until the flusher drains below the bound. Cancellation propagates as `CancelledError`.
- A batch whose POST fails is restored to the **front** of the buffer, in order, and retried
  every `retry_delay_millis` (default 250 ms); it is never dropped.
- Flushes are single-flight per buffer, and flush tasks are strongly referenced (asyncio keeps
  only weak refs to fire-and-forget tasks, so an unreferenced flush could be collected mid-POST).
- `close()` flushes with a 30 s deadline (`CLOSE_FLUSH_TIMEOUT_SECONDS` in `client.py`) and
  reports what was left unsent.

**Key Implementation Notes:**
- Use `asyncio.Condition` for the backpressure bound (the direct `sync.Cond` analogue)
- Timer callback: use `loop.call_later(seconds, callback)`; cancel with `timer_handle.cancel()`
- Use `time.time()` for timestamps (seconds), convert to ms when needed

---

### 5. QueueBuilder

**File:** `queen/builders/queue_builder.py`

```python
from typing import Optional, Dict, Any, List, Callable, Awaitable
import uuid
from ..utils.defaults import QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS
from ..utils.validation import validate_queue_name, is_valid_uuid
from ..utils.logger import log, error as log_error

class QueueBuilder:
    """Queue builder for fluent API"""
    
    def __init__(
        self,
        queen: 'Queen',
        http_client: 'HttpClient',
        buffer_manager: 'BufferManager',
        queue_name: Optional[str] = None
    ):
        self._queen = queen
        self._http_client = http_client
        self._buffer_manager = buffer_manager
        self._queue_name = queue_name
        self._partition = 'Default'
        self._namespace: Optional[str] = None
        self._task: Optional[str] = None
        self._group: Optional[str] = None
        self._config: Dict[str, Any] = {}
        
        # Consume options
        self._concurrency = CONSUME_DEFAULTS["concurrency"]
        self._batch = CONSUME_DEFAULTS["batch"]
        self._limit = CONSUME_DEFAULTS["limit"]
        self._idle_millis = CONSUME_DEFAULTS["idle_millis"]
        self._auto_ack = CONSUME_DEFAULTS["auto_ack"]
        self._wait = CONSUME_DEFAULTS["wait"]
        self._timeout_millis = CONSUME_DEFAULTS["timeout_millis"]
        self._renew_lease = CONSUME_DEFAULTS["renew_lease"]
        self._renew_lease_interval_millis = CONSUME_DEFAULTS["renew_lease_interval_millis"]
        self._subscription_mode = CONSUME_DEFAULTS["subscription_mode"]
        self._subscription_from = CONSUME_DEFAULTS["subscription_from"]
        self._each = False
        
        # Buffer options
        self._buffer_options: Optional[Dict[str, Any]] = None
        
    def namespace(self, name: str) -> 'QueueBuilder':
        """Set namespace"""
        
    def task(self, name: str) -> 'QueueBuilder':
        """Set task"""
        
    def config(self, options: Dict[str, Any]) -> 'QueueBuilder':
        """Set queue configuration"""
        
    def create(self) -> 'OperationBuilder':
        """Create queue"""
        
    def delete(self) -> 'OperationBuilder':
        """Delete queue"""
        
    def partition(self, name: str) -> 'QueueBuilder':
        """Set partition"""
        
    def buffer(self, options: Dict[str, Any]) -> 'QueueBuilder':
        """Enable client-side buffering"""
        
    def push(self, payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> 'PushBuilder':
        """Push messages"""
        
    def group(self, name: str) -> 'QueueBuilder':
        """Set consumer group"""
        
    def concurrency(self, count: int) -> 'QueueBuilder':
        """Set concurrency"""
        
    def batch(self, size: int) -> 'QueueBuilder':
        """Set batch size"""
        
    def limit(self, count: int) -> 'QueueBuilder':
        """Set message limit"""
        
    def idle_millis(self, millis: int) -> 'QueueBuilder':
        """Set idle timeout"""
        
    def auto_ack(self, enabled: bool) -> 'QueueBuilder':
        """Set auto-ack"""
        
    def renew_lease(self, enabled: bool, interval_millis: Optional[int] = None) -> 'QueueBuilder':
        """Enable lease renewal"""
        
    def subscription_mode(self, mode: str) -> 'QueueBuilder':
        """Set subscription mode"""
        
    def subscription_from(self, from_: str) -> 'QueueBuilder':
        """Set subscription start point"""
        
    def each(self) -> 'QueueBuilder':
        """Process messages one at a time"""
        
    def consume(
        self,
        handler: Callable[[Any], Awaitable[Any]],
        *,
        signal: Optional[asyncio.Event] = None
    ) -> 'ConsumeBuilder':
        """Start consuming messages"""
        
    def wait(self, enabled: bool) -> 'QueueBuilder':
        """Enable/disable long polling"""
        
    async def pop(self) -> List[Dict[str, Any]]:
        """Pop messages"""
        
    async def flush_buffer(self) -> None:
        """Flush buffer for this queue"""
        
    def dlq(self, consumer_group: Optional[str] = None) -> 'DLQBuilder':
        """Query dead letter queue"""
        
    def _get_affinity_key(self) -> Optional[str]:
        """Generate affinity key for consistent routing"""
        
    def _build_pop_path(self) -> str:
        """Build pop path"""
        
    def _build_pop_params(self) -> Dict[str, str]:
        """Build pop query parameters"""
```

**Key Implementation Notes:**
- UUID generation: Use `uuid.uuid4()` or try to import `uuid7` library
- Format messages: Check for `data` or `payload` properties using `in` operator
- Auto-generate `transactionId` if not provided
- Affinity key format: `queue:partition:consumerGroup` or `namespace:task:consumerGroup`
- For pop(): override autoAck to POP_DEFAULTS if not explicitly set
- Use `urllib.parse.urlencode()` for query params
- Return empty list on pop() error (don't throw)

---

### 6. ConsumerManager

**File:** `queen/consumer/consumer_manager.py`

```python
import asyncio
from typing import Optional, Dict, Any, List, Callable, Awaitable
from ..utils.logger import log, warn as log_warn, error as log_error

class ConsumerManager:
    """Consumer manager for handling concurrent workers"""
    
    def __init__(self, http_client: 'HttpClient', queen: 'Queen'):
        self._http_client = http_client
        self._queen = queen
        
    async def start(
        self,
        handler: Callable[[Any], Awaitable[Any]],
        options: Dict[str, Any]
    ) -> None:
        """Start consumer workers"""
        
    async def _worker(
        self,
        worker_id: int,
        handler: Callable[[Any], Awaitable[Any]],
        path: str,
        base_params: Dict[str, str],
        options: Dict[str, Any]
    ) -> None:
        """Worker loop"""
        
    async def _process_message(
        self,
        message: Dict[str, Any],
        handler: Callable[[Any], Awaitable[Any]],
        auto_ack: bool,
        group: Optional[str]
    ) -> None:
        """Process single message"""
        
    async def _process_batch(
        self,
        messages: List[Dict[str, Any]],
        handler: Callable[[Any], Awaitable[Any]],
        auto_ack: bool,
        group: Optional[str]
    ) -> None:
        """Process batch of messages"""
        
    def _setup_lease_renewal(
        self,
        messages: List[Dict[str, Any]],
        interval_millis: int
    ) -> asyncio.Task:
        """Setup lease renewal task"""
        
    def _enhance_messages_with_trace(
        self,
        messages: List[Dict[str, Any]],
        group: Optional[str]
    ) -> None:
        """Add trace() method to messages"""
        
    def _get_affinity_key(
        self,
        queue: Optional[str],
        partition: Optional[str],
        namespace: Optional[str],
        task: Optional[str],
        group: Optional[str]
    ) -> Optional[str]:
        """Generate affinity key"""
        
    def _build_path(
        self,
        queue: Optional[str],
        partition: Optional[str],
        namespace: Optional[str],
        task: Optional[str]
    ) -> str:
        """Build pop path"""
        
    def _build_params(
        self,
        batch: int,
        wait: bool,
        timeout_millis: int,
        group: Optional[str],
        subscription_mode: Optional[str],
        subscription_from: Optional[str],
        namespace: Optional[str],
        task: Optional[str],
        auto_ack: bool
    ) -> Dict[str, str]:
        """Build query parameters"""
```

**Key Implementation Notes:**
- Use `asyncio.create_task()` for worker tasks
- Check `signal.is_set()` for abort (use `asyncio.Event`)
- Use `time.time()` for idle timeout tracking
- Enhance messages: Add `trace` as a closure that captures message context
- Trace function: NEVER raise exceptions, always return dict with success status
- Lease renewal: Use `asyncio.create_task()` with periodic sleep loop
- Auto-ack errors: Don't rethrow (allows consumer to continue)
- Handle timeout errors gracefully for long polling

---

### 7. TransactionBuilder

**File:** `queen/builders/transaction_builder.py`

```python
from typing import List, Dict, Any, Union
from ..utils.logger import log, error as log_error

class TransactionBuilder:
    """Transaction builder for atomic operations"""
    
    def __init__(self, http_client: 'HttpClient'):
        self._http_client = http_client
        self._operations: List[Dict[str, Any]] = []
        self._required_leases: List[str] = []
        
    def ack(
        self,
        messages: Union[Any, List[Any]],
        status: str = 'completed'
    ) -> 'TransactionBuilder':
        """Add ack operation"""
        
    def queue(self, queue_name: str) -> 'TransactionQueueBuilder':
        """Add push operation to queue"""
        
    async def commit(self) -> Dict[str, Any]:
        """Commit transaction"""


class TransactionQueueBuilder:
    """Sub-builder for push operations in transaction"""
    
    def __init__(
        self,
        transaction_builder: 'TransactionBuilder',
        queue_name: str
    ):
        self._transaction_builder = transaction_builder
        self._queue_name = queue_name
        self._partition: Optional[str] = None
        
    def partition(self, partition_key: str) -> 'TransactionQueueBuilder':
        """Set partition for push"""
        
    def push(self, items: Union[Dict[str, Any], List[Dict[str, Any]]]) -> 'TransactionBuilder':
        """Add messages to transaction"""
```

**Key Implementation Notes:**
- Validate `partitionId` is present (raise ValueError if missing)
- Check for `transactionId` or `id` property
- Check for `data` or `payload` using `in` operator
- Deduplicate required_leases using set
- Commit endpoint: `POST /api/v1/transaction`
- Raise exception if `result.get('success')` is False

---

### 8. Stream Components

**File:** `queen/stream/stream_builder.py`

```python
from typing import List, Dict, Any

class StreamBuilder:
    """StreamBuilder - Fluent API for defining streams"""
    
    def __init__(
        self,
        http_client: 'HttpClient',
        queen: 'Queen',
        name: str,
        namespace: str
    ):
        self._http_client = http_client
        self._queen = queen
        self._config = {
            'name': name,
            'namespace': namespace,
            'source_queue_names': [],
            'partitioned': False,
            'window_type': 'tumbling',
            'window_duration_ms': 60000,
            'window_grace_period_ms': 30000,
            'window_lease_timeout_ms': 60000
        }
        
    def sources(self, queue_names: List[str]) -> 'StreamBuilder':
        """Set source queues"""
        
    def partitioned(self) -> 'StreamBuilder':
        """Enable partitioned processing"""
        
    def tumbling_time(self, seconds: int) -> 'StreamBuilder':
        """Configure tumbling time window"""
        
    def grace_period(self, seconds: int) -> 'StreamBuilder':
        """Configure grace period"""
        
    def lease_timeout(self, seconds: int) -> 'StreamBuilder':
        """Configure lease timeout"""
        
    async def define(self) -> Dict[str, Any]:
        """Define/create the stream on the server"""
```

**File:** `queen/stream/stream_consumer.py`

```python
import asyncio
from typing import Optional, Callable, Awaitable, Dict, Any
from .window import Window

class StreamConsumer:
    """StreamConsumer - Manages consuming windows from a stream"""
    
    def __init__(
        self,
        http_client: 'HttpClient',
        queen: 'Queen',
        stream_name: str,
        consumer_group: str
    ):
        self._http_client = http_client
        self._queen = queen
        self._stream_name = stream_name
        self._consumer_group = consumer_group
        self._poll_timeout = 30000
        self._lease_renew_interval = 20000
        self._running = False
        
    async def process(self, callback: Callable[[Window], Awaitable[None]]) -> None:
        """Start processing windows"""
        
    def stop(self) -> None:
        """Stop the processing loop"""
        
    async def poll_window(self) -> Optional[Window]:
        """Poll for a window"""
        
    async def execute_callback(
        self,
        window: Window,
        callback: Callable[[Window], Awaitable[None]]
    ) -> None:
        """Execute callback with lease renewal"""
        
    async def seek(self, timestamp: str) -> Dict[str, Any]:
        """Seek to a specific timestamp"""
```

**File:** `queen/stream/window.py`

```python
from typing import List, Dict, Any, Callable, Optional

class Window:
    """Window - Represents a time window of messages with utility methods"""
    
    def __init__(self, raw_window: Dict[str, Any]):
        # Copy all properties
        self.__dict__.update(raw_window)
        
        # Store immutable original messages
        self.all_messages = tuple(raw_window.get('messages', []))
        
        # Working copy
        self.messages = list(self.all_messages)
        
    def filter(self, filter_fn: Callable[[Dict[str, Any]], bool]) -> 'Window':
        """Filter messages"""
        
    def group_by(self, key_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """Group messages by key path"""
        
    def aggregate(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate messages"""
        
    def reset(self) -> 'Window':
        """Reset to original messages"""
        
    def size(self) -> int:
        """Get working set size"""
        
    def original_size(self) -> int:
        """Get original size"""
```

**Key Implementation Notes:**
- Use `asyncio.create_task()` for lease renewal in background
- Lease renewal: POST to `/api/v1/stream/renew-lease`
- ACK/NACK: POST to `/api/v1/stream/ack`
- Window aggregation: Use `functools.reduce()` for sum/avg
- Group by: Use `collections.defaultdict(list)`
- Get nested property: Implement `_get_path()` helper with split/reduce logic

---

## Configuration & Defaults

**File:** `queen/utils/defaults.py`

```python
"""Default configuration values for Queen Client"""

CLIENT_DEFAULTS = {
    "timeout_millis": 30000,
    "retry_attempts": 3,
    "retry_delay_millis": 1000,
    "load_balancing_strategy": "affinity",
    "affinity_hash_ring": 128,
    "enable_failover": True,
    "health_retry_after_millis": 5000
}

QUEUE_DEFAULTS = {
    "lease_time": 300,
    "retry_limit": 3,
    "priority": 0,
    "delayed_processing": 0,
    "window_buffer": 0,
    "max_size": 0,
    "retention_seconds": 0,
    "completed_retention_seconds": 0,
    "encryption_enabled": False
}

CONSUME_DEFAULTS = {
    "concurrency": 1,
    "batch": 1,
    "auto_ack": True,
    "wait": True,
    "timeout_millis": 30000,
    "limit": None,
    "idle_millis": None,
    "renew_lease": False,
    "renew_lease_interval_millis": None,
    "subscription_mode": None,
    "subscription_from": None
}

POP_DEFAULTS = {
    "batch": 1,
    "wait": False,
    "timeout_millis": 30000,
    "auto_ack": False
}

BUFFER_DEFAULTS = {
    "message_count": 100,
    "time_millis": 100
}
```

---

## Implementation Details

### UUID Generation

```python
# Try to use uuid7 if available, fallback to uuid4
try:
    from uuid_extensions import uuid7
    def generate_uuid() -> str:
        return str(uuid7())
except ImportError:
    from uuid import uuid4
    def generate_uuid() -> str:
        return str(uuid4())
```

### Logging

```python
import os
import sys
import json
from datetime import datetime
from typing import Any, Dict

LOG_ENABLED = os.environ.get('QUEEN_CLIENT_LOG', '').lower() == 'true'

def _get_timestamp() -> str:
    return datetime.utcnow().isoformat() + 'Z'

def _format_log(operation: str, details: Any, level: str = 'INFO') -> str:
    timestamp = _get_timestamp()
    details_str = json.dumps(details) if isinstance(details, dict) else str(details)
    return f"[{timestamp}] [{level}] [{operation}] {details_str}"

def log(operation: str, details: Any) -> None:
    if not LOG_ENABLED:
        return
    print(_format_log(operation, details), file=sys.stdout)

def warn(operation: str, details: Any) -> None:
    if not LOG_ENABLED:
        return
    print(_format_log(operation, details, 'WARN'), file=sys.stderr)

def error(operation: str, details: Any) -> None:
    if not LOG_ENABLED:
        return
    print(_format_log(operation, details, 'ERROR'), file=sys.stderr)

def is_enabled() -> bool:
    return LOG_ENABLED
```

### Validation

```python
import re
from typing import List

UUID_V4_REGEX = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE
)

def is_valid_uuid(s: str) -> bool:
    return isinstance(s, str) and UUID_V4_REGEX.match(s) is not None

def validate_queue_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError('Queue name must be a non-empty string')
    return name.strip()

def validate_url(url: str) -> str:
    if not isinstance(url, str) or not url.startswith('http'):
        raise ValueError(f'Invalid URL: {url}')
    return url

def validate_urls(urls: List[str]) -> List[str]:
    if not isinstance(urls, list) or not urls:
        raise ValueError('URLs must be a non-empty list')
    return [validate_url(url) for url in urls]
```

### Signal Handlers

```python
import signal
import asyncio

class Queen:
    def _setup_graceful_shutdown(self):
        self._signal_count = 0
        
        def shutdown_handler(signum, frame):
            self._signal_count += 1
            signal_name = signal.Signals(signum).name
            print(f"\nReceived {signal_name}, shutting down gracefully...")
            
            if self._signal_count > 1:
                print('Received multiple shutdown signals, exiting immediately')
                sys.exit(1)
            
            # Schedule coroutine in event loop
            asyncio.create_task(self._async_shutdown())
        
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
    async def _async_shutdown(self):
        try:
            await self.close()
            sys.exit(0)
        except Exception as e:
            print(f'Error during shutdown: {e}')
            sys.exit(1)
```

---

## API Surface

### Top-Level API

```python
from queen import Queen

# Initialize
queen = Queen('http://localhost:6632')
queen = Queen(['http://server1:6632', 'http://server2:6632'])
queen = Queen({
    'urls': ['http://server1:6632', 'http://server2:6632'],
    'timeout_millis': 30000,
    'load_balancing_strategy': 'affinity'
})

# Context manager
async with Queen('http://localhost:6632') as queen:
    await queen.queue('tasks').push([{'data': {'value': 1}}])
```

### Queue Operations

```python
# Create
await queen.queue('my-queue').create()
await queen.queue('my-queue').config({'priority': 5}).create()

# Delete
await queen.queue('my-queue').delete()

# Push
await queen.queue('q').push([{'data': {'value': 1}}])
await queen.queue('q').partition('p1').push([{'data': {'value': 1}}])
await queen.queue('q').buffer({'message_count': 100}).push([{'data': {'value': 1}}])

# Pop
messages = await queen.queue('q').pop()
messages = await queen.queue('q').batch(10).wait(True).pop()

# Consume
await queen.queue('q').consume(async def handler(msg): ...)
await queen.queue('q').limit(10).consume(handler)
await queen.queue('q').concurrency(5).batch(10).consume(handler)
await queen.queue('q').group('my-group').consume(handler)

# With callbacks
await (queen.queue('tasks')
    .consume(handler)
    .on_success(async def on_success(msg, result): ...)
    .on_error(async def on_error(msg, error): ...))
```

### Subscription Modes

```python
# Default (all messages)
await queen.queue('events').group('analytics').consume(handler)

# New messages only
await queen.queue('events').group('realtime').subscription_mode('new').consume(handler)

# From timestamp
await queen.queue('events').group('replay').subscription_from('2025-10-28T10:00:00.000Z').consume(handler)
```

### Acknowledgment

```python
await queen.ack(message, True)   # Success
await queen.ack(message, False)  # Retry
await queen.ack(message, False, {'error': 'reason'})
await queen.ack([msg1, msg2], True)  # Batch
```

### Transactions

```python
await (queen.transaction()
    .ack(message)
    .queue('output')
    .push([{'data': {'result': 'processed'}}])
    .commit())
```

### Lease Renewal

```python
await queen.renew(message)
await queen.renew([msg1, msg2])
await queen.queue('q').renew_lease(True, 60000).consume(handler)
```

### Buffering

```python
await queen.flush_all_buffers()
await queen.queue('q').flush_buffer()
stats = queen.get_buffer_stats()
```

### Dead Letter Queue

```python
dlq = await queen.queue('q').dlq().limit(10).get()
dlq = await queen.queue('q').dlq('consumer-group').limit(10).get()
```

### Streaming

```python
# Define stream
await (queen.stream('user-events', 'analytics')
    .sources(['queue1', 'queue2'])
    .partitioned()
    .tumbling_time(60)
    .define())

# Consume windows
consumer = queen.consumer('user-events', 'my-group')
await consumer.process(async def callback(window):
    stats = window.aggregate({'count': True, 'sum': ['data.amount']})
    print(f"Window had {stats['count']} messages")
)
```

---

## Testing Strategy

### Unit Tests

```python
# tests/test_defaults.py
def test_client_defaults():
    assert CLIENT_DEFAULTS['timeout_millis'] == 30000
    assert CLIENT_DEFAULTS['load_balancing_strategy'] == 'affinity'

# tests/test_validation.py
def test_validate_url():
    assert validate_url('http://localhost:6632') == 'http://localhost:6632'
    with pytest.raises(ValueError):
        validate_url('not-a-url')

# tests/test_load_balancer.py
def test_affinity_routing():
    lb = LoadBalancer(['http://s1', 'http://s2'], 'affinity')
    # Same key should route to same server
    url1 = lb.get_next_url('key1')
    url2 = lb.get_next_url('key1')
    assert url1 == url2
```

### Integration Tests

```python
# tests/integration/test_push_pop.py
@pytest.mark.asyncio
async def test_push_and_pop():
    async with Queen('http://localhost:6632') as queen:
        await queen.queue('test-queue').create()
        
        await queen.queue('test-queue').push([
            {'data': {'id': 1}},
            {'data': {'id': 2}}
        ])
        
        messages = await queen.queue('test-queue').batch(10).pop()
        assert len(messages) == 2
        assert messages[0]['data']['id'] == 1
```

### Parity Tests

Port all Node.js tests from `clients/client-js/test-v2/` to ensure 100% behavioral parity.

---

## Edge Cases & Gotchas

### 1. Time Units Inconsistency
```python
# Properties with "millis" suffix → milliseconds
timeout_millis = 30000  # 30 seconds

# Properties with "seconds" suffix → seconds
lease_time = 300  # 5 minutes

# Properties without suffix (time-related) → seconds
delayed_processing = 60  # 60 seconds
```

### 2. autoAck Defaults
```python
# Pop: autoAck defaults to False
messages = await queen.queue('q').pop()  # Manual ack required

# Consume: autoAck defaults to True
await queen.queue('q').consume(handler)  # Auto-ack enabled
```

### 3. partitionId is Mandatory for Ack
```python
# Must include partitionId to prevent acking wrong message
await queen.ack(message, True)  # message must have partitionId

# Validation in ack() method:
if not partition_id:
    raise ValueError('Message must have partitionId property')
```

### 4. Callbacks Disable autoAck
```python
# If on_success or on_error defined, autoAck forced to False
await (queen.queue('q')
    .consume(handler)
    .on_success(on_success_handler))  # autoAck = False automatically
```

### 5. Affinity Key Format
```python
# Queue mode
affinity_key = f"{queue}:{partition or '*'}:{group or '__QUEUE_MODE__'}"

# Namespace/task mode
affinity_key = f"{namespace or '*'}:{task or '*'}:{group or '__QUEUE_MODE__'}"
```

### 6. Trace Never Crashes
```python
# message.trace() should NEVER raise exceptions
async def trace(trace_config):
    try:
        # ... implementation
        return {'success': True}
    except Exception as e:
        # Log but don't raise
        log_error('trace', {'error': str(e)})
        return {'success': False, 'error': str(e)}
```

### 7. Health Status Retry Timing
```python
# Allow retry of unhealthy backends after configured interval
now = time.time()
if not status['healthy']:
    if status['last_failure'] and (now - status['last_failure']) >= (health_retry_after_millis / 1000):
        # Include in healthy_urls for retry
        return True
return False
```

### 8. Virtual Node Ring Rebuild
```python
# Rebuild ring when backends become unhealthy/healthy
def mark_unhealthy(url):
    self._health_status[url]['healthy'] = False
    if self._virtual_nodes:
        self._rebuild_virtual_node_ring()  # Exclude unhealthy

def mark_healthy(url):
    was_unhealthy = not self._health_status[url]['healthy']
    self._health_status[url]['healthy'] = True
    if self._virtual_nodes and was_unhealthy:
        self._rebuild_virtual_node_ring()  # Include recovered
```

---

## Python-Specific Considerations

### 1. Async/Await
- All I/O operations must be async
- Use `asyncio.create_task()` for concurrent tasks
- Use `asyncio.gather()` for parallel operations
- Use `asyncio.Event` for abort signals

### 2. Type Hints
```python
from typing import Union, List, Optional, Dict, Any, Callable, Awaitable, TypedDict

class Message(TypedDict):
    transactionId: str
    partitionId: str
    leaseId: Optional[str]
    data: Dict[str, Any]

MessageHandler = Callable[[Union[Message, List[Message]]], Awaitable[Any]]
```

### 3. Context Managers
```python
class Queen:
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
```

### 4. Method Chaining
```python
# Return self for fluent API
def partition(self, name: str) -> 'QueueBuilder':
    self._partition = name
    return self
```

### 5. Property Accessors
```python
@property
def message_count(self) -> int:
    return len(self._messages)
```

### 6. Private Attributes
```python
# Use single underscore for "private" (Python convention)
self._http_client = http_client
self._buffers = {}
```

### 7. String Formatting
```python
# Use f-strings for readability
affinity_key = f"{queue}:{partition}:{group}"
```

### 8. Exception Handling
```python
# Use specific exception types
raise ValueError('Queue name must be non-empty')
raise TypeError('Expected list, got str')
```

### 9. Async Timers
```python
# Use asyncio.TimerHandle for deferred execution
loop = asyncio.get_event_loop()
timer = loop.call_later(seconds, callback)

# Cancel timer
timer.cancel()
```

### 10. Async Locks
```python
# Use asyncio.Lock for thread-safe operations
self._lock = asyncio.Lock()

async with self._lock:
    # Critical section
    self._buffers[key] = buffer
```

---

## Implementation Checklist

### Phase 1: Core Infrastructure
- [ ] Project setup (pyproject.toml, directory structure)
- [ ] Utils module (defaults, logger, validation)
- [ ] Type definitions (types.py)
- [ ] HttpClient with retry logic
- [ ] LoadBalancer with all 3 strategies
- [ ] Health tracking and failover
- [ ] Virtual node consistent hashing (FNV-1a)

### Phase 2: Buffering
- [ ] MessageBuffer class
- [ ] BufferManager class
- [ ] Time-based flush (asyncio.TimerHandle)
- [ ] Count-based flush
- [ ] Manual flush API
- [ ] Pending flush tracking

### Phase 3: Queue Operations
- [ ] QueueBuilder class
- [ ] Queue create/delete
- [ ] Push operation
- [ ] Pop operation
- [ ] PushBuilder with callbacks
- [ ] OperationBuilder with callbacks
- [ ] UUID generation

### Phase 4: Consumption
- [ ] ConsumerManager class
- [ ] Worker loop with concurrency
- [ ] Auto-ack logic
- [ ] Lease renewal
- [ ] Idle timeout
- [ ] Limit handling
- [ ] ConsumeBuilder with callbacks

### Phase 5: Advanced Features
- [ ] TransactionBuilder
- [ ] Ack API (single and batch)
- [ ] Renew API
- [ ] Consumer group management
- [ ] Subscription modes
- [ ] DLQBuilder

### Phase 6: Tracing
- [ ] Message enhancement with trace()
- [ ] Trace endpoint integration
- [ ] Error-safe trace implementation
- [ ] Multi-dimensional trace names

### Phase 7: Streaming
- [ ] StreamBuilder
- [ ] StreamConsumer
- [ ] Window class with utilities
- [ ] Aggregation methods
- [ ] Grouping and filtering

### Phase 8: Client Finalization
- [ ] Queen main class
- [ ] Signal handlers (SIGINT/SIGTERM)
- [ ] Graceful shutdown
- [ ] Context manager support
- [ ] Buffer stats API
- [ ] Close/cleanup

### Phase 9: Testing
- [ ] Unit tests for all components
- [ ] Integration tests
- [ ] Port all Node.js tests
- [ ] Parity validation
- [ ] Performance benchmarks

### Phase 10: Documentation
- [ ] API documentation (docstrings)
- [ ] README with examples
- [ ] Migration guide from Node.js
- [ ] Type stubs (py.typed)
- [ ] Examples directory

---

## Notes on Implementation Order

1. **Start with infrastructure** (HttpClient, LoadBalancer) - foundation for everything
2. **Build buffering next** - needed for push operations
3. **Implement QueueBuilder** - core API surface
4. **Add ConsumerManager** - complete the cycle
5. **Layer in advanced features** - transactions, streaming
6. **Polish and test** - ensure 100% parity

---

## Success Criteria

✅ **Functional Parity:**
- All API methods from Node.js client exist
- All defaults match exactly
- All behaviors match (retry, failover, buffering, etc.)

✅ **Correctness:**
- All edge cases handled
- Thread-safe operations
- Graceful error handling
- No data loss scenarios

✅ **Testing:**
- 100% of Node.js tests pass in Python
- Additional Python-specific tests
- Performance benchmarks meet expectations

✅ **Documentation:**
- Complete API documentation
- Working examples for all features
- Migration guide for Node.js users

✅ **Developer Experience:**
- Type hints everywhere
- Pythonic API design
- Clear error messages
- IDE auto-completion support

---

## End of Plan

This plan provides a complete roadmap for building a Python client with 100% feature parity to the Node.js client. Follow the implementation checklist and ensure all success criteria are met.

