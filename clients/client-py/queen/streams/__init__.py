"""
queen.streams — fluent streaming SDK on top of the Queen broker client.

Public surface (mirrors @queenmq/streams in JS):

    from queen import Queen, Stream, token_bucket_gate, sliding_window_gate

    async with Queen(url='http://localhost:6632') as q:
        await Stream.from_(q.queue('source')) \\
            .gate(token_bucket_gate(capacity=100, refill_per_sec=100)) \\
            .to(q.queue('approved')) \\
            .run(query_id='rate-limiter', url='http://localhost:6632')

The streaming runtime commits state, sink emissions, and source acks in a
single PostgreSQL transaction via /streams/v1/cycle, so end-to-end semantics
are exactly-once even across retries.
"""

from .stream import Stream
from .helpers.rate_limiter import token_bucket_gate, sliding_window_gate

__all__ = ["Stream", "token_bucket_gate", "sliding_window_gate"]
