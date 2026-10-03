//! The one error type of the crate.
//!
//! What a caller needs from an error is what to do next, so every variant says
//! it: [`Error::is_retryable`] (back off and try the same thing again) and
//! [`Error::code`] (the stable name the status block and the API report). The
//! engines never decide by matching message text.

use std::fmt;

use crate::queen::QueenError;

/// A stable error code plus a human sentence that names the fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The connector document is invalid. Never retried: the document must
    /// change.
    Config(String),
    /// PostgreSQL refused or failed. `sqlstate` is the five-character code when
    /// the server sent one; `transient` is the classification of
    /// [`crate::pg::connect::classify`] (connection loss, admin shutdown,
    /// serialization failure, deadlock, too many connections: true).
    Pg {
        sqlstate: Option<String>,
        message: String,
        transient: bool,
    },
    /// The broker answered an error (or could not be reached in-process).
    Queen(QueenError),
    /// Another owner moved the state this node was writing (the source
    /// pointer, a lease): give everything up and start again from the top.
    Fenced(String),
    /// A condition only an operator can fix (`slot_lost`, `system_changed`,
    /// `slot_ahead`, `version`, `wal_level`, `replica_identity`, ...). The
    /// connector stops with an error status naming the fix; the supervisor
    /// retries slowly in case the operator fixed it on the database side.
    Fatal { code: &'static str, message: String },
    /// A protocol violation or an I/O failure on a socket.
    Io(String),
    /// The stop signal arrived.
    Stopped,
}

impl Error {
    pub fn config(msg: impl Into<String>) -> Error {
        Error::Config(msg.into())
    }

    pub fn fatal(code: &'static str, msg: impl Into<String>) -> Error {
        Error::Fatal {
            code,
            message: msg.into(),
        }
    }

    pub fn io(msg: impl Into<String>) -> Error {
        Error::Io(msg.into())
    }

    /// Whether backing off and trying the same operation again can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Config(_) | Error::Fatal { .. } | Error::Stopped => false,
            Error::Pg { transient, .. } => *transient,
            Error::Queen(q) => q.is_retryable(),
            Error::Fenced(_) => true,
            Error::Io(_) => true,
        }
    }

    /// The stable code the status block reports.
    pub fn code(&self) -> &'static str {
        match self {
            Error::Config(_) => "config",
            Error::Pg { .. } => "postgres",
            Error::Queen(_) => "queen",
            Error::Fenced(_) => "fenced",
            Error::Fatal { code, .. } => code,
            Error::Io(_) => "io",
            Error::Stopped => "stopped",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(m) => write!(f, "invalid connector: {m}"),
            Error::Pg {
                sqlstate: Some(s),
                message,
                ..
            } => write!(f, "postgres {s}: {message}"),
            Error::Pg { message, .. } => write!(f, "postgres: {message}"),
            Error::Queen(q) => write!(f, "queen: {q}"),
            Error::Fenced(m) => write!(f, "fenced: {m}"),
            Error::Fatal { code, message } => write!(f, "{code}: {message}"),
            Error::Io(m) => write!(f, "io: {m}"),
            Error::Stopped => write!(f, "stopped"),
        }
    }
}

impl std::error::Error for Error {}

impl From<QueenError> for Error {
    fn from(e: QueenError) -> Error {
        Error::Queen(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
