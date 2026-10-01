//! One worker's socket: newline-delimited JSON over a non-blocking stream.

use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;

const MAX_LINE_BYTES: usize = 64 * 1024;
/// A worker reads its events at once; one that does not is not waited for.
const WRITE_TIMEOUT_MILLIS: i32 = 100;

pub(super) struct Connection {
    stream: UnixStream,
    buffer: Vec<u8>,
}

impl Connection {
    pub(super) fn new(stream: UnixStream) -> std::io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            buffer: Vec::new(),
        })
    }

    /// The next complete line, if one is available without waiting.
    pub(super) fn next_line(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
                return Ok(Some(self.buffer.drain(..=end).collect()));
            }
            if self.buffer.len() > MAX_LINE_BYTES {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "command line too long",
                ));
            }
            let mut chunk = [0_u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "the worker closed the connection",
                    ))
                }
                Ok(read) => self.buffer.extend_from_slice(&chunk[..read]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Wait until the worker sends something, `also` becomes readable, or
    /// `millis` pass; None waits without a limit.
    pub(super) fn wait(&self, millis: Option<i64>, also: Option<RawFd>) {
        let mut fds = [
            libc::pollfd {
                fd: self.stream.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                // poll ignores a negative descriptor.
                fd: also.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let timeout = millis.map_or(-1, |millis| millis.clamp(0, i64::from(i32::MAX)) as i32);
        // SAFETY: two valid pollfd entries for the duration of the call. An
        // interrupted or failed poll only ends this wait early.
        unsafe {
            libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout);
        }
    }

    pub(super) fn emit(&mut self, event: &serde_json::Value) -> std::io::Result<()> {
        let mut line = event.to_string().into_bytes();
        line.push(b'\n');
        let mut written = 0;
        while written < line.len() {
            match self.stream.write(&line[written..]) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(count) => written += count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if !writable(self.stream.as_raw_fd()) {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            "the worker does not read its events",
                        ));
                    }
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// A diagnostic that never delays fencing: one write attempt, and a
    /// worker that misses it still sees the signal.
    pub(super) fn emit_best_effort(&mut self, event: &serde_json::Value) {
        let mut line = event.to_string().into_bytes();
        line.push(b'\n');
        let _ = self.stream.write(&line);
    }
}

fn writable(fd: RawFd) -> bool {
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: one valid pollfd for the duration of the call.
    unsafe { libc::poll(&mut poll, 1, WRITE_TIMEOUT_MILLIS) > 0 }
}
