//! The `pgoutput` logical decoding messages (protocol version 1), decoded from
//! the payload of one XLogData frame. OWNER: agent R.
//!
//! Times on the wire are microseconds since 2000-01-01; this module converts
//! them to Unix microseconds ([`super::PG_EPOCH_OFFSET_US`]).
//!
//! The decoder is strict: a message must be consumed exactly (trailing bytes
//! are an error), and every count is checked against the bytes that are left
//! BEFORE anything is allocated, so a corrupt frame is an [`Error::Io`] and
//! never a panic or a multi-gigabyte allocation. Protocol version 1 without
//! `streaming` never carries the in-progress xid field that streamed
//! transactions put in front of Relation/Insert/... messages; this crate never
//! asks for streaming, and the stream and two-phase tags decode as
//! [`Message::Other`].

use bytes::Bytes;

use super::{Lsn, PG_EPOCH_OFFSET_US};
use crate::error::{Error, Result};

/// One column of a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    /// `'n'`.
    Null,
    /// `'u'`: an unchanged TOASTed value — the value is NOT sent.
    Unchanged,
    /// `'t'`: the type's text output.
    Text(Bytes),
    /// `'b'`: binary (never requested by this crate).
    Binary(Bytes),
}

/// A tuple, one datum per column of the relation, in column order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tuple(pub Vec<Datum>);

/// The old row of an UPDATE/DELETE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OldTuple {
    /// `'K'`: the replica identity key columns only (other columns Null).
    Key(Tuple),
    /// `'O'`: the whole old row (`REPLICA IDENTITY FULL`).
    Full(Tuple),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelColumn {
    /// Part of the replica identity (flags & 1).
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_mod: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    /// Never empty: the wire's empty namespace (`pg_catalog`) is spelled out.
    pub namespace: String,
    pub name: String,
    /// `d` default, `n` nothing, `f` full, `i` index.
    pub replica_identity: u8,
    pub columns: Vec<RelColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Begin {
        /// The commit record's LSN (known up front: a non-streamed transaction
        /// is sent only after its commit was decoded).
        final_lsn: Lsn,
        commit_time_us: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: Lsn,
        /// The END of the commit record: what the source confirms and stores.
        end_lsn: Lsn,
        commit_time_us: i64,
    },
    Origin {
        lsn: Lsn,
        name: String,
    },
    Relation(Relation),
    /// Sent before a Relation for each column whose type is not built in
    /// (OID >= 10000). For a domain the OID is the domain's while the
    /// namespace and name are its BASE type's (pgoutput's
    /// `logicalrep_write_typ`); the namespace is never empty (`pg_catalog`
    /// spelled out).
    Type {
        oid: u32,
        namespace: String,
        name: String,
    },
    Insert {
        relid: u32,
        new: Tuple,
    },
    Update {
        relid: u32,
        old: Option<OldTuple>,
        new: Tuple,
    },
    Delete {
        relid: u32,
        old: OldTuple,
    },
    Truncate {
        /// 1 = CASCADE, 2 = RESTART IDENTITY.
        options: u8,
        relids: Vec<u32>,
    },
    /// `pg_logical_emit_message` (with `messages 'true'`).
    LogicalMessage {
        transactional: bool,
        lsn: Lsn,
        prefix: String,
        content: Bytes,
    },
    /// A tag this crate never asks for (stream / two-phase messages).
    Other {
        tag: u8,
    },
}

/// Decode one pgoutput message (the XLogData payload).
pub fn decode(data: &[u8]) -> Result<Message> {
    decode_bytes(Bytes::copy_from_slice(data))
}

/// [`decode`] without the copy: text datums and message contents are slices
/// of `data` (the replication client hands over the frame it read).
pub fn decode_bytes(data: Bytes) -> Result<Message> {
    let Some(&tag) = data.first() else {
        return Err(Error::io("pgoutput: empty message"));
    };
    let mut r = Reader {
        buf: data,
        pos: 1,
        kind: kind_name(tag),
    };
    let message = match tag {
        b'B' => Message::Begin {
            final_lsn: r.lsn("final LSN")?,
            commit_time_us: r.time("commit time")?,
            xid: r.u32("xid")?,
        },
        b'C' => Message::Commit {
            flags: r.u8("flags")?,
            commit_lsn: r.lsn("commit LSN")?,
            end_lsn: r.lsn("end LSN")?,
            commit_time_us: r.time("commit time")?,
        },
        b'O' => Message::Origin {
            lsn: r.lsn("origin LSN")?,
            name: r.cstr("origin name")?,
        },
        b'R' => Message::Relation(r.relation()?),
        b'Y' => Message::Type {
            oid: r.u32("type OID")?,
            namespace: r.namespace()?,
            name: r.cstr("type name")?,
        },
        b'I' => {
            let relid = r.u32("relation id")?;
            r.expect_new_tuple_tag()?;
            Message::Insert {
                relid,
                new: r.tuple()?,
            }
        }
        b'U' => {
            let relid = r.u32("relation id")?;
            let (old, tag) = match r.u8("tuple tag")? {
                b'K' => (Some(OldTuple::Key(r.tuple()?)), r.u8("tuple tag")?),
                b'O' => (Some(OldTuple::Full(r.tuple()?)), r.u8("tuple tag")?),
                t => (None, t),
            };
            if tag != b'N' {
                return Err(r.err(&format!("tuple tag {} where 'N' belongs", show(tag))));
            }
            Message::Update {
                relid,
                old,
                new: r.tuple()?,
            }
        }
        b'D' => {
            let relid = r.u32("relation id")?;
            let old = match r.u8("tuple tag")? {
                b'K' => OldTuple::Key(r.tuple()?),
                b'O' => OldTuple::Full(r.tuple()?),
                t => return Err(r.err(&format!("tuple tag {} where 'K' or 'O' belongs", show(t)))),
            };
            Message::Delete { relid, old }
        }
        b'T' => {
            let n = r.i32("relation count")?;
            let options = r.u8("options")?;
            let n = usize::try_from(n).map_err(|_| r.err("negative relation count"))?;
            r.need(n.saturating_mul(4), "relation ids")?;
            let mut relids = Vec::with_capacity(n);
            for _ in 0..n {
                relids.push(r.u32("relation id")?);
            }
            Message::Truncate { options, relids }
        }
        b'M' => {
            let flags = r.u8("flags")?;
            Message::LogicalMessage {
                transactional: flags & 1 != 0,
                lsn: r.lsn("message LSN")?,
                prefix: r.cstr("prefix")?,
                content: r.counted("content")?,
            }
        }
        other => return Ok(Message::Other { tag: other }),
    };
    r.finish()?;
    Ok(message)
}

fn kind_name(tag: u8) -> &'static str {
    match tag {
        b'B' => "Begin",
        b'C' => "Commit",
        b'O' => "Origin",
        b'R' => "Relation",
        b'Y' => "Type",
        b'I' => "Insert",
        b'U' => "Update",
        b'D' => "Delete",
        b'T' => "Truncate",
        b'M' => "Message",
        _ => "message",
    }
}

fn show(b: u8) -> String {
    if b.is_ascii_graphic() {
        format!("'{}'", b as char)
    } else {
        format!("0x{b:02x}")
    }
}

struct Reader {
    buf: Bytes,
    pos: usize,
    kind: &'static str,
}

impl Reader {
    fn err(&self, what: &str) -> Error {
        Error::io(format!("pgoutput {}: {what}", self.kind))
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn need(&self, n: usize, what: &str) -> Result<()> {
        if self.remaining() < n {
            Err(self.err(&format!("truncated at {what}")))
        } else {
            Ok(())
        }
    }

    fn take<const N: usize>(&mut self, what: &str) -> Result<[u8; N]> {
        self.need(N, what)?;
        let mut a = [0u8; N];
        a.copy_from_slice(&self.buf[self.pos..self.pos + N]);
        self.pos += N;
        Ok(a)
    }

    fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.take::<1>(what)?[0])
    }

    fn u16(&mut self, what: &str) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(what)?))
    }

    fn u32(&mut self, what: &str) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(what)?))
    }

    fn i32(&mut self, what: &str) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(what)?))
    }

    fn i64(&mut self, what: &str) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(what)?))
    }

    fn lsn(&mut self, what: &str) -> Result<Lsn> {
        Ok(Lsn(u64::from_be_bytes(self.take(what)?)))
    }

    /// A timestamp: microseconds since 2000-01-01 on the wire, Unix
    /// microseconds out. Saturating: a garbage value must not panic.
    fn time(&mut self, what: &str) -> Result<i64> {
        Ok(self.i64(what)?.saturating_add(PG_EPOCH_OFFSET_US))
    }

    /// A NUL-terminated string. The replication connection runs with
    /// `client_encoding=UTF8`, so anything else is a corrupt frame.
    fn cstr(&mut self, what: &str) -> Result<String> {
        let rest = &self.buf[self.pos..];
        let Some(end) = rest.iter().position(|&b| b == 0) else {
            return Err(self.err(&format!("unterminated {what}")));
        };
        let s = std::str::from_utf8(&rest[..end])
            .map_err(|_| self.err(&format!("{what} is not UTF-8")))?
            .to_string();
        self.pos += end + 1;
        Ok(s)
    }

    /// pgoutput writes `pg_catalog` as the empty string.
    fn namespace(&mut self) -> Result<String> {
        let s = self.cstr("namespace")?;
        Ok(if s.is_empty() {
            "pg_catalog".to_string()
        } else {
            s
        })
    }

    /// Int32 length + that many bytes, as a slice of the frame.
    fn counted(&mut self, what: &str) -> Result<Bytes> {
        let len = self.i32(what)?;
        let len =
            usize::try_from(len).map_err(|_| self.err(&format!("negative length of {what}")))?;
        self.need(len, what)?;
        let b = self.buf.slice(self.pos..self.pos + len);
        self.pos += len;
        Ok(b)
    }

    fn expect_new_tuple_tag(&mut self) -> Result<()> {
        match self.u8("tuple tag")? {
            b'N' => Ok(()),
            t => Err(self.err(&format!("tuple tag {} where 'N' belongs", show(t)))),
        }
    }

    fn tuple(&mut self) -> Result<Tuple> {
        let n = usize::from(self.u16("column count")?);
        // At least one kind byte per column: checked before allocating.
        self.need(n, "tuple columns")?;
        let mut cols = Vec::with_capacity(n);
        for _ in 0..n {
            cols.push(match self.u8("column kind")? {
                b'n' => Datum::Null,
                b'u' => Datum::Unchanged,
                b't' => Datum::Text(self.counted("text value")?),
                b'b' => Datum::Binary(self.counted("binary value")?),
                k => return Err(self.err(&format!("unknown column kind {}", show(k)))),
            });
        }
        Ok(Tuple(cols))
    }

    fn relation(&mut self) -> Result<Relation> {
        let id = self.u32("relation id")?;
        let namespace = self.namespace()?;
        let name = self.cstr("relation name")?;
        let replica_identity = self.u8("replica identity")?;
        let n = usize::from(self.u16("column count")?);
        // flags + NUL + type OID + typmod: 10 bytes at the least per column.
        self.need(n.saturating_mul(10), "columns")?;
        let mut columns = Vec::with_capacity(n);
        for _ in 0..n {
            let flags = self.u8("column flags")?;
            columns.push(RelColumn {
                key: flags & 1 != 0,
                name: self.cstr("column name")?,
                type_oid: self.u32("column type")?,
                type_mod: self.i32("column typmod")?,
            });
        }
        Ok(Relation {
            id,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }

    fn finish(&self) -> Result<()> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(self.err(&format!("{n} trailing bytes"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a message byte by byte, the way the server writes it.
    #[derive(Default)]
    struct W(Vec<u8>);

    impl W {
        fn tag(t: u8) -> W {
            W(vec![t])
        }
        fn u8(mut self, v: u8) -> W {
            self.0.push(v);
            self
        }
        fn i16(mut self, v: i16) -> W {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i32(mut self, v: i32) -> W {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn u32(mut self, v: u32) -> W {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i64(mut self, v: i64) -> W {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn cstr(mut self, s: &str) -> W {
            self.0.extend_from_slice(s.as_bytes());
            self.0.push(0);
            self
        }
        fn raw(mut self, b: &[u8]) -> W {
            self.0.extend_from_slice(b);
            self
        }
        fn text(self, s: &str) -> W {
            self.u8(b't').i32(s.len() as i32).raw(s.as_bytes())
        }
    }

    const T2000: i64 = 812_345_678_901_234; // µs since 2000-01-01

    fn fixtures() -> Vec<(Vec<u8>, Message)> {
        let unix = T2000 + PG_EPOCH_OFFSET_US;
        vec![
            (
                W::tag(b'B').i64(0x16B_3748).i64(T2000).u32(7781).0,
                Message::Begin {
                    final_lsn: Lsn(0x16B_3748),
                    commit_time_us: unix,
                    xid: 7781,
                },
            ),
            (
                W::tag(b'C')
                    .u8(0)
                    .i64(0x16B_3748)
                    .i64(0x16B_3778)
                    .i64(T2000)
                    .0,
                Message::Commit {
                    flags: 0,
                    commit_lsn: Lsn(0x16B_3748),
                    end_lsn: Lsn(0x16B_3778),
                    commit_time_us: unix,
                },
            ),
            (
                W::tag(b'O').i64(0x1_0000_0010).cstr("pg_16385").0,
                Message::Origin {
                    lsn: Lsn(0x1_0000_0010),
                    name: "pg_16385".into(),
                },
            ),
            (
                W::tag(b'R')
                    .u32(16_390)
                    .cstr("public")
                    .cstr("orders")
                    .u8(b'd')
                    .i16(3)
                    .u8(1)
                    .cstr("id")
                    .u32(23)
                    .i32(-1)
                    .u8(0)
                    .cstr("status")
                    .u32(1043)
                    .i32(24)
                    .u8(0)
                    .cstr("notes")
                    .u32(25)
                    .i32(-1)
                    .0,
                Message::Relation(Relation {
                    id: 16_390,
                    namespace: "public".into(),
                    name: "orders".into(),
                    replica_identity: b'd',
                    columns: vec![
                        RelColumn {
                            key: true,
                            name: "id".into(),
                            type_oid: 23,
                            type_mod: -1,
                        },
                        RelColumn {
                            key: false,
                            name: "status".into(),
                            type_oid: 1043,
                            type_mod: 24,
                        },
                        RelColumn {
                            key: false,
                            name: "notes".into(),
                            type_oid: 25,
                            type_mod: -1,
                        },
                    ],
                }),
            ),
            (
                W::tag(b'R').u32(1).cstr("").cstr("weird").u8(b'n').i16(0).0,
                Message::Relation(Relation {
                    id: 1,
                    namespace: "pg_catalog".into(),
                    name: "weird".into(),
                    replica_identity: b'n',
                    columns: vec![],
                }),
            ),
            (
                W::tag(b'Y').u32(16_400).cstr("public").cstr("mood").0,
                Message::Type {
                    oid: 16_400,
                    namespace: "public".into(),
                    name: "mood".into(),
                },
            ),
            (
                W::tag(b'Y').u32(16_401).cstr("").cstr("int4").0,
                Message::Type {
                    oid: 16_401,
                    namespace: "pg_catalog".into(),
                    name: "int4".into(),
                },
            ),
            (
                W::tag(b'I')
                    .u32(16_390)
                    .u8(b'N')
                    .i16(4)
                    .text("42")
                    .u8(b'n')
                    .u8(b'u')
                    .u8(b'b')
                    .i32(2)
                    .raw(&[0, 0xff])
                    .0,
                Message::Insert {
                    relid: 16_390,
                    new: Tuple(vec![
                        Datum::Text(Bytes::from_static(b"42")),
                        Datum::Null,
                        Datum::Unchanged,
                        Datum::Binary(Bytes::from_static(&[0, 0xff])),
                    ]),
                },
            ),
            (
                W::tag(b'U')
                    .u32(16_390)
                    .u8(b'N')
                    .i16(2)
                    .text("42")
                    .u8(b'u')
                    .0,
                Message::Update {
                    relid: 16_390,
                    old: None,
                    new: Tuple(vec![
                        Datum::Text(Bytes::from_static(b"42")),
                        Datum::Unchanged,
                    ]),
                },
            ),
            (
                W::tag(b'U')
                    .u32(16_390)
                    .u8(b'K')
                    .i16(2)
                    .text("41")
                    .u8(b'n')
                    .u8(b'N')
                    .i16(2)
                    .text("42")
                    .text("")
                    .0,
                Message::Update {
                    relid: 16_390,
                    old: Some(OldTuple::Key(Tuple(vec![
                        Datum::Text(Bytes::from_static(b"41")),
                        Datum::Null,
                    ]))),
                    new: Tuple(vec![
                        Datum::Text(Bytes::from_static(b"42")),
                        Datum::Text(Bytes::new()),
                    ]),
                },
            ),
            (
                W::tag(b'U')
                    .u32(16_390)
                    .u8(b'O')
                    .i16(1)
                    .text("old")
                    .u8(b'N')
                    .i16(1)
                    .text("new")
                    .0,
                Message::Update {
                    relid: 16_390,
                    old: Some(OldTuple::Full(Tuple(vec![Datum::Text(
                        Bytes::from_static(b"old"),
                    )]))),
                    new: Tuple(vec![Datum::Text(Bytes::from_static(b"new"))]),
                },
            ),
            (
                W::tag(b'D').u32(16_390).u8(b'K').i16(1).text("42").0,
                Message::Delete {
                    relid: 16_390,
                    old: OldTuple::Key(Tuple(vec![Datum::Text(Bytes::from_static(b"42"))])),
                },
            ),
            (
                W::tag(b'D').u32(16_390).u8(b'O').i16(1).u8(b'n').0,
                Message::Delete {
                    relid: 16_390,
                    old: OldTuple::Full(Tuple(vec![Datum::Null])),
                },
            ),
            (
                W::tag(b'T').i32(2).u8(3).u32(16_390).u32(16_391).0,
                Message::Truncate {
                    options: 3,
                    relids: vec![16_390, 16_391],
                },
            ),
            (
                W::tag(b'T').i32(0).u8(0).0,
                Message::Truncate {
                    options: 0,
                    relids: vec![],
                },
            ),
            (
                W::tag(b'M')
                    .u8(1)
                    .i64(0x170_A2C8)
                    .cstr("queen:4f1c2a9b")
                    .i32(4)
                    .raw(b"lw:7")
                    .0,
                Message::LogicalMessage {
                    transactional: true,
                    lsn: Lsn(0x170_A2C8),
                    prefix: "queen:4f1c2a9b".into(),
                    content: Bytes::from_static(b"lw:7"),
                },
            ),
            (
                W::tag(b'M').u8(0).i64(5).cstr("").i32(0).0,
                Message::LogicalMessage {
                    transactional: false,
                    lsn: Lsn(5),
                    prefix: String::new(),
                    content: Bytes::new(),
                },
            ),
        ]
    }

    #[test]
    fn every_message_decodes_exactly() {
        for (bytes, want) in fixtures() {
            assert_eq!(decode(&bytes).unwrap(), want, "{bytes:?}");
            assert_eq!(decode_bytes(Bytes::from(bytes.clone())).unwrap(), want);
        }
    }

    #[test]
    fn times_are_unix_microseconds() {
        let m = decode(&W::tag(b'B').i64(1).i64(0).u32(1).0).unwrap();
        let Message::Begin { commit_time_us, .. } = m else {
            panic!()
        };
        // 2000-01-01T00:00:00Z
        assert_eq!(commit_time_us, 946_684_800_000_000);
        // Garbage saturates instead of overflowing.
        let m = decode(&W::tag(b'B').i64(1).i64(i64::MAX).u32(1).0).unwrap();
        assert!(matches!(
            m,
            Message::Begin {
                commit_time_us: i64::MAX,
                ..
            }
        ));
    }

    #[test]
    fn unknown_tags_are_other_and_empty_is_an_error() {
        for tag in [
            b'S', b'E', b'c', b'A', b'b', b'P', b'K', b'r', b'p', b'Z', 0, 0xff,
        ] {
            assert_eq!(decode(&[tag, 1, 2, 3]).unwrap(), Message::Other { tag });
            assert_eq!(decode(&[tag]).unwrap(), Message::Other { tag });
        }
        assert!(matches!(decode(&[]), Err(Error::Io(_))));
    }

    #[test]
    fn every_truncation_is_an_io_error() {
        for (bytes, _) in fixtures() {
            for cut in 0..bytes.len() {
                match decode(&bytes[..cut]) {
                    Err(Error::Io(_)) => {}
                    other => panic!("prefix {cut} of {bytes:?}: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn trailing_bytes_are_an_error() {
        for (mut bytes, _) in fixtures() {
            bytes.push(0);
            assert!(matches!(decode(&bytes), Err(Error::Io(_))), "{bytes:?}");
        }
    }

    #[test]
    fn hostile_counts_fail_before_allocating() {
        let cases = [
            W::tag(b'T').i32(i32::MAX).u8(0).0,
            W::tag(b'T').i32(-1).u8(0).0,
            W::tag(b'R').u32(1).cstr("s").cstr("t").u8(b'd').i16(-1).0,
            W::tag(b'I').u32(1).u8(b'N').i16(-1).0,
            W::tag(b'I').u32(1).u8(b'N').i16(1).u8(b't').i32(i32::MAX).0,
            W::tag(b'I').u32(1).u8(b'N').i16(1).u8(b't').i32(-1).0,
            W::tag(b'I').u32(1).u8(b'N').i16(1).u8(b'x').0,
            W::tag(b'I').u32(1).u8(b'K').i16(0).0,
            W::tag(b'U').u32(1).u8(b'K').i16(0).u8(b'K').i16(0).0,
            W::tag(b'D').u32(1).u8(b'N').i16(0).0,
            W::tag(b'M').u8(0).i64(0).cstr("p").i32(i32::MAX).0,
            W::tag(b'O').i64(0).raw(b"no terminator").0,
            W::tag(b'Y').u32(1).raw(&[0xff, 0xfe, 0]).cstr("t").0,
        ];
        for c in cases {
            assert!(matches!(decode(&c), Err(Error::Io(_))), "{c:?}");
        }
    }

    /// Random garbage behind every tag, and every single-byte corruption of
    /// every fixture: an error or a message, never a panic.
    #[test]
    fn garbage_never_panics() {
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let tags = b"BCORYIUDTMX";
        for i in 0..20_000 {
            let len = (next() % 64) as usize;
            let mut buf = vec![tags[i % tags.len()]];
            buf.extend((0..len).map(|_| next() as u8));
            let _ = decode(&buf);
        }
        for (bytes, _) in fixtures() {
            for i in 0..bytes.len() {
                for v in [0u8, 1, 0x7f, 0x80, 0xff, b'N', b'K', b't'] {
                    let mut b = bytes.clone();
                    b[i] = v;
                    let _ = decode(&b);
                }
            }
        }
    }
}
