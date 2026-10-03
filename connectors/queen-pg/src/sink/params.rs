//! sql mode's parameters (PLAN §3.2): each `params` entry names what one `$n`
//! of the statement receives — a path into the payload or a field of the
//! message — and every one is passed as TEXT, or NULL.
//!
//! Why TEXT for everything: the statement is prepared with every parameter
//! typed `text` ([`tokio_postgres::Client::prepare_typed`]), so
//! `$1::numeric` is a cast the SERVER performs on the exact characters of the
//! payload. Left to infer, the server would type `$1` numeric, and
//! tokio-postgres binds a `&str` only to text-like parameters; a JSON number
//! would also have to pass through an `f64` on the way, losing digits. Text in,
//! the statement casts: a 30-digit amount arrives with all 30 digits.
//!
//! Paths are read straight from the raw payload ([`RawValue`]), one level at a
//! time, so a value passed through is the text the producer wrote: a number is
//! its digits, an object or array its JSON text.

use std::collections::HashMap;

use serde_json::value::RawValue;

/// One `params` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Param {
    /// `$` (no steps: the whole payload) or `$.a.b[2]`.
    Path(Vec<Step>),
    /// `@partition`, `@partitionId`, `@offset`, `@transactionId`, `@createdAt`.
    Meta(Meta),
}

/// One step of a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// `.name` or `["name"]` (the bracket form for names with `.`, `[` or `]`).
    Key(String),
    /// `[n]`.
    Index(usize),
}

/// A field of the delivered message rather than of its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Meta {
    Partition,
    PartitionId,
    Offset,
    TransactionId,
    CreatedAt,
}

impl Meta {
    /// The `@name` (and `metadata` field) spelling.
    pub fn name(self) -> &'static str {
        match self {
            Meta::Partition => "partition",
            Meta::PartitionId => "partitionId",
            Meta::Offset => "offset",
            Meta::TransactionId => "transactionId",
            Meta::CreatedAt => "createdAt",
        }
    }

    pub fn parse(name: &str) -> Option<Meta> {
        [
            Meta::Partition,
            Meta::PartitionId,
            Meta::Offset,
            Meta::TransactionId,
            Meta::CreatedAt,
        ]
        .into_iter()
        .find(|m| m.name() == name)
    }
}

impl Param {
    /// Parse one `params` entry; the error names what is wrong with it.
    pub fn parse(s: &str) -> Result<Param, String> {
        if let Some(name) = s.strip_prefix('@') {
            return Meta::parse(name).map(Param::Meta).ok_or_else(|| {
                format!(
                    "unknown parameter {s:?}: the message fields are @partition, @partitionId, \
                     @offset, @transactionId, @createdAt"
                )
            });
        }
        parse_path(s).map(Param::Path)
    }
}

fn parse_path(s: &str) -> Result<Vec<Step>, String> {
    let bad = |why: &str| {
        format!("bad parameter path {s:?}: {why} (paths look like $, $.a.b, $.items[2], $[\"odd.key\"])")
    };
    let mut rest = s
        .strip_prefix('$')
        .ok_or_else(|| bad("must start with $ or @"))?;
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            let end = r.find(['.', '[', ']']).unwrap_or(r.len());
            if end == 0 {
                return Err(bad("empty key after '.'"));
            }
            steps.push(Step::Key(r[..end].to_string()));
            rest = &r[end..];
        } else if let Some(r) = rest.strip_prefix('[') {
            if r.starts_with('"') {
                // A JSON string literal: find its closing quote, honouring
                // escapes, and let serde_json decode it.
                let bytes = r.as_bytes();
                let mut i = 1;
                let mut closed = None;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'"' => {
                            closed = Some(i);
                            break;
                        }
                        _ => i += 1,
                    }
                }
                let close = closed.ok_or_else(|| bad("unterminated string"))?;
                let key: String = serde_json::from_str(&r[..=close])
                    .map_err(|e| bad(&format!("bad string ({e})")))?;
                rest = r[close + 1..]
                    .strip_prefix(']')
                    .ok_or_else(|| bad("expected ']' after the string"))?;
                steps.push(Step::Key(key));
            } else {
                let close = r.find(']').ok_or_else(|| bad("unterminated '['"))?;
                let digits = &r[..close];
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(bad("an index is digits only"));
                }
                let n: usize = digits.parse().map_err(|_| bad("index too large"))?;
                steps.push(Step::Index(n));
                rest = &r[close + 1..];
            }
        } else {
            return Err(bad("expected '.' or '['"));
        }
    }
    Ok(steps)
}

/// What a parameter can read from one delivered message.
#[derive(Debug, Clone, Copy)]
pub struct MessageRef<'a> {
    pub payload: &'a RawValue,
    pub partition: &'a str,
    pub partition_id: &'a str,
    pub offset: i64,
    pub transaction_id: &'a str,
    pub created_at: &'a str,
}

impl MessageRef<'_> {
    /// A message field as TEXT (`None` = NULL: an empty `createdAt`).
    pub fn meta_text(&self, m: Meta) -> Option<String> {
        match m {
            Meta::Partition => Some(self.partition.to_string()),
            Meta::PartitionId => Some(self.partition_id.to_string()),
            Meta::Offset => Some(self.offset.to_string()),
            Meta::TransactionId => Some(self.transaction_id.to_string()),
            Meta::CreatedAt => (!self.created_at.is_empty()).then(|| self.created_at.to_string()),
        }
    }
}

/// The TEXT a parameter passes for `msg` (`None` = NULL).
pub fn eval(p: &Param, msg: &MessageRef<'_>) -> Option<String> {
    match p {
        Param::Meta(m) => msg.meta_text(*m),
        Param::Path(steps) => lookup(msg.payload, steps).and_then(as_text),
    }
}

/// Follow `steps` into `root`; `None` when a step finds no such key or index,
/// or the value at that point is not an object (key) or an array (index).
pub fn lookup<'a>(root: &'a RawValue, steps: &[Step]) -> Option<&'a RawValue> {
    let mut cur = root;
    for step in steps {
        cur = match step {
            Step::Key(k) => {
                if !first_byte_is(cur, b'{') {
                    return None;
                }
                // A repeated key: the last one wins, as in jsonb.
                let m: HashMap<String, &'a RawValue> = serde_json::from_str(cur.get()).ok()?;
                *m.get(k.as_str())?
            }
            Step::Index(i) => {
                if !first_byte_is(cur, b'[') {
                    return None;
                }
                let v: Vec<&'a RawValue> = serde_json::from_str(cur.get()).ok()?;
                *v.get(*i)?
            }
        };
    }
    Some(cur)
}

fn first_byte_is(v: &RawValue, b: u8) -> bool {
    v.get().trim_start().as_bytes().first() == Some(&b)
}

/// A JSON value as the TEXT a parameter passes: a string's contents, a
/// number's digits as written, `true`/`false`, an object's or array's JSON
/// text; `null` is NULL.
pub fn as_text(v: &RawValue) -> Option<String> {
    let t = v.get().trim();
    match t.as_bytes().first()? {
        b'"' => serde_json::from_str::<String>(t).ok(),
        b'n' => None,
        _ => Some(t.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_string()).unwrap()
    }

    fn msg(payload: &RawValue) -> MessageRef<'_> {
        MessageRef {
            payload,
            partition: "acct-7",
            partition_id: "1234",
            offset: 42,
            transaction_id: "tx-1",
            created_at: "2026-10-02T10:00:00.000Z",
        }
    }

    fn ev(p: &str, payload: &str) -> Option<String> {
        let r = raw(payload);
        eval(&Param::parse(p).unwrap(), &msg(&r))
    }

    #[test]
    fn paths_parse_into_steps() {
        assert_eq!(Param::parse("$").unwrap(), Param::Path(vec![]));
        assert_eq!(
            Param::parse("$.a.b[2]").unwrap(),
            Param::Path(vec![
                Step::Key("a".into()),
                Step::Key("b".into()),
                Step::Index(2)
            ])
        );
        assert_eq!(
            Param::parse(r#"$["odd.key"][0]["q\"uote"]"#).unwrap(),
            Param::Path(vec![
                Step::Key("odd.key".into()),
                Step::Index(0),
                Step::Key("q\"uote".into())
            ])
        );
        assert_eq!(
            Param::parse("$.Mixed_Case$1").unwrap(),
            Param::Path(vec![Step::Key("Mixed_Case$1".into())])
        );
        assert_eq!(Param::parse("@offset").unwrap(), Param::Meta(Meta::Offset));
        assert_eq!(
            Param::parse("@transactionId").unwrap(),
            Param::Meta(Meta::TransactionId)
        );
        for bad in [
            "",
            "a",
            "$.",
            "$..a",
            "$[",
            "$[x]",
            "$[-1]",
            "$[1",
            "$a",
            "$.a]",
            "@nope",
            "@",
            r#"$["open"#,
            r#"$["k"x"#,
        ] {
            assert!(Param::parse(bad).is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn nested_paths_and_arrays_read_the_value() {
        let p = r#"{"a":{"b":[10,{"c":"deep"},30]},"n":1}"#;
        assert_eq!(ev("$.a.b[0]", p).as_deref(), Some("10"));
        assert_eq!(ev("$.a.b[1].c", p).as_deref(), Some("deep"));
        assert_eq!(ev("$.a.b[2]", p).as_deref(), Some("30"));
        assert_eq!(ev("$.n", p).as_deref(), Some("1"));
        // A top-level array payload.
        assert_eq!(ev("$[1]", "[true, false]").as_deref(), Some("false"));
    }

    #[test]
    fn missing_or_mistyped_steps_are_null() {
        let p = r#"{"a":{"b":[1]},"s":"x","z":null}"#;
        assert_eq!(ev("$.nope", p), None);
        assert_eq!(ev("$.a.b[5]", p), None);
        assert_eq!(ev("$.a[0]", p), None, "an index into an object");
        assert_eq!(ev("$.a.b.c", p), None, "a key into an array");
        assert_eq!(ev("$.s.t", p), None, "a key into a string");
        assert_eq!(ev("$.z", p), None, "JSON null is NULL");
        assert_eq!(ev("$.z.y", p), None);
        assert_eq!(ev("$", "null"), None);
    }

    #[test]
    fn values_pass_as_their_text() {
        let p = r#"{"big":123456789012345678901234567890.000000000001,"neg":-1e-7,
                    "s":"hé \"q\" \n","t":true,"f":false,
                    "o":{"k":[1, 2]},"arr":[{"x":1}],"empty":""}"#;
        assert_eq!(
            ev("$.big", p).as_deref(),
            Some("123456789012345678901234567890.000000000001"),
            "every digit survives"
        );
        assert_eq!(ev("$.neg", p).as_deref(), Some("-1e-7"));
        assert_eq!(
            ev("$.s", p).as_deref(),
            Some("hé \"q\" \n"),
            "strings decoded"
        );
        assert_eq!(ev("$.t", p).as_deref(), Some("true"));
        assert_eq!(ev("$.f", p).as_deref(), Some("false"));
        assert_eq!(
            ev("$.o", p).as_deref(),
            Some(r#"{"k":[1, 2]}"#),
            "objects as JSON text"
        );
        assert_eq!(
            ev("$.arr", p).as_deref(),
            Some(r#"[{"x":1}]"#),
            "arrays as JSON text"
        );
        assert_eq!(
            ev("$.empty", p).as_deref(),
            Some(""),
            "an empty string is not NULL"
        );
        assert_eq!(ev("$", r#"{"a":1}"#).as_deref(), Some(r#"{"a":1}"#));
        assert_eq!(ev("$", r#""just text""#).as_deref(), Some("just text"));
        assert_eq!(ev("$", "17").as_deref(), Some("17"));
    }

    #[test]
    fn a_repeated_key_reads_the_last_one_like_jsonb() {
        assert_eq!(ev("$.a", r#"{"a":1,"a":2}"#).as_deref(), Some("2"));
    }

    #[test]
    fn message_fields_are_text() {
        let p = "{}";
        assert_eq!(ev("@partition", p).as_deref(), Some("acct-7"));
        assert_eq!(ev("@partitionId", p).as_deref(), Some("1234"));
        assert_eq!(ev("@offset", p).as_deref(), Some("42"));
        assert_eq!(ev("@transactionId", p).as_deref(), Some("tx-1"));
        assert_eq!(
            ev("@createdAt", p).as_deref(),
            Some("2026-10-02T10:00:00.000Z")
        );
        let r = raw(p);
        let m = MessageRef {
            created_at: "",
            ..msg(&r)
        };
        assert_eq!(m.meta_text(Meta::CreatedAt), None);
    }
}
