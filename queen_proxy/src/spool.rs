//! Disk spool for meter samples when pxdb is down. OWNER: Agent D.
//! Adapt the broker's file_buffer.rs pattern (append-only rotating files,
//! *.tmp active -> *.buf drainable, startup recovery, circuit breaker) —
//! copy what you need from server/src/file_buffer.rs and strip the broker
//! specifics; samples serialize as JSON lines.

#[allow(dead_code)]
pub struct Spool;
