//! The `where` language (PLAN_ROUTING.md §9): conditions on one message,
//! ANDed. One parser and one evaluator for the tenant feed
//! (`POST /api/v1/feed`), filtered consumer groups and reading queues.
//!
//! Two forms say the same thing:
//!
//! - a list of `{field, op, value}`, as the feed and the SDKs' `.where()`
//!   send it;
//! - one string, `data.kind in ('order.paid', 'order.refunded') and
//!   data.total > 100`, as people write it.
//!
//! Both become the same [`Cond`]s. [`to_text`] writes a list back as the
//! string the console shows, and [`to_json`] as the list the catalogue keeps.
//!
//! A field the message does not have (a path its payload lacks, a payload that
//! is not JSON, a header it does not carry) holds for no operator, `!=`
//! included, as SQL's comparisons with NULL.

use serde_json::Value;

/// The deepest `data.a.b.c` path.
pub const MAX_FIELD_DEPTH: usize = 16;

/// What a condition reads.
#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Queue,
    Partition,
    TransactionId,
    /// The message's offset in its partition (the feed only).
    Offset,
    /// The message's position in the tenant feed (the feed only).
    Position,
    /// The payload, or a path into it (`data.customer.country`).
    Data(Vec<String>),
    /// A header (`headers.type`).
    Header(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Exists,
    Prefix,
    NotPrefix,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cond {
    pub field: Field,
    pub op: Op,
    pub value: Value,
}

/// What a caller allows: how many conditions, how many values in one `in`,
/// and whether the feed-only fields may be named.
#[derive(Clone, Copy, Debug)]
pub struct Rules {
    pub max_conditions: usize,
    pub max_in: usize,
    /// `offset` and `position` (the feed reads stored messages; a route
    /// decides before either is known to the writer).
    pub positions: bool,
}

impl Rules {
    /// The feed's: 16 conditions, 256 values in an `in`, every field.
    pub const FEED: Rules = Rules {
        max_conditions: 16,
        max_in: 256,
        positions: true,
    };

    /// A filtered group's or a reading queue's: `QUEEN_WHERE_MAX_CONDITIONS`
    /// (16) and `QUEEN_WHERE_MAX_IN` (64) by default, no feed-only field.
    pub fn route() -> Rules {
        Rules {
            max_conditions: env_usize("QUEEN_WHERE_MAX_CONDITIONS", 16),
            max_in: env_usize("QUEEN_WHERE_MAX_IN", 64),
            positions: false,
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// What a condition is checked against.
#[derive(Clone, Copy, Debug, Default)]
pub struct Subject<'a> {
    pub queue: &'a str,
    pub partition: &'a str,
    pub txn: &'a str,
    pub offset: Option<u64>,
    pub position: Option<u64>,
    /// The payload as JSON, when some condition reads it and it parses.
    pub doc: Option<&'a Value>,
    /// The message's headers block (`crate::frames::decode_headers`).
    pub headers: Option<&'a [u8]>,
}

/// A field by name.
pub fn parse_field(raw: &str, rules: &Rules) -> Result<Field, String> {
    match raw {
        "queue" => return Ok(Field::Queue),
        "partition" => return Ok(Field::Partition),
        "transactionId" => return Ok(Field::TransactionId),
        "offset" | "position" if !rules.positions => {
            return Err(format!(
                "where: {raw:?} is known only once a message is stored; use it in the feed"
            ))
        }
        "offset" => return Ok(Field::Offset),
        "position" => return Ok(Field::Position),
        "data" => return Ok(Field::Data(Vec::new())),
        _ => {}
    }
    if let Some(key) = raw.strip_prefix("headers.") {
        if !crate::frames::header_key_valid(key) {
            return Err(format!(
                "where: bad header name {key:?} (1 to 64 bytes of a-z, 0-9, '.', '_' or '-')"
            ));
        }
        return Ok(Field::Header(key.to_string()));
    }
    let Some(path) = raw.strip_prefix("data.") else {
        return Err(format!(
            "where: unknown field {raw:?} (queue, partition, transactionId, data, data.<path> or headers.<name>{})",
            if rules.positions { ", offset, position" } else { "" }
        ));
    };
    let parts: Vec<String> = path.split('.').map(str::to_string).collect();
    if parts.iter().any(String::is_empty) || parts.len() > MAX_FIELD_DEPTH {
        return Err(format!("where: bad field path {raw:?}"));
    }
    Ok(Field::Data(parts))
}

pub fn parse_op(raw: &str) -> Result<Op, String> {
    Ok(match raw.trim().to_ascii_lowercase().as_str() {
        "=" | "==" => Op::Eq,
        "!=" | "<>" => Op::Ne,
        "<" => Op::Lt,
        "<=" => Op::Le,
        ">" => Op::Gt,
        ">=" => Op::Ge,
        "in" => Op::In,
        "exists" => Op::Exists,
        "prefix" => Op::Prefix,
        "!prefix" => Op::NotPrefix,
        _ => {
            return Err(format!(
                "where: unknown op {raw:?} (=, !=, <, <=, >, >=, in, exists, prefix, !prefix)"
            ))
        }
    })
}

fn op_text(op: Op) -> &'static str {
    match op {
        Op::Eq => "=",
        Op::Ne => "!=",
        Op::Lt => "<",
        Op::Le => "<=",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::In => "in",
        Op::Exists => "exists",
        Op::Prefix => "prefix",
        Op::NotPrefix => "!prefix",
    }
}

fn field_text(f: &Field) -> String {
    match f {
        Field::Queue => "queue".into(),
        Field::Partition => "partition".into(),
        Field::TransactionId => "transactionId".into(),
        Field::Offset => "offset".into(),
        Field::Position => "position".into(),
        Field::Data(p) if p.is_empty() => "data".into(),
        Field::Data(p) => format!("data.{}", p.join(".")),
        Field::Header(k) => format!("headers.{k}"),
    }
}

/// One condition, its value checked against its operator.
pub fn check(field_raw: &str, op_raw: &str, value: Value, rules: &Rules) -> Result<Cond, String> {
    let field = parse_field(field_raw, rules)?;
    let op = parse_op(op_raw)?;
    match op {
        Op::In => match &value {
            Value::Array(a) if a.len() <= rules.max_in => {}
            Value::Array(_) => {
                return Err(format!("where: in takes at most {} values", rules.max_in))
            }
            _ => return Err(format!("where: in on {field_raw:?} needs an array")),
        },
        Op::Prefix | Op::NotPrefix if !value.is_string() => {
            return Err(format!("where: {op_raw} on {field_raw:?} needs a string"))
        }
        Op::Lt | Op::Le | Op::Gt | Op::Ge if !(value.is_number() || value.is_string()) => {
            return Err(format!(
                "where: {op_raw} on {field_raw:?} needs a number or a string"
            ))
        }
        _ => {}
    }
    Ok(Cond { field, op, value })
}

/// Conditions from either form: a list of `{field, op, value}` (each value
/// optional for `exists`), or one string ([`parse_text`]). `null` is none.
pub fn parse_value(v: &Value, rules: &Rules) -> Result<Vec<Cond>, String> {
    let conds = match v {
        Value::Null => Vec::new(),
        Value::String(s) => parse_text(s, rules)?,
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                // A triple as a list: `['data.kind', '=', 'order.paid']`.
                if let Value::Array(t) = it {
                    let field = t.first().and_then(Value::as_str);
                    let op = t.get(1).and_then(Value::as_str);
                    match (field, op) {
                        (Some(f), Some(o)) if t.len() <= 3 => out.push(check(
                            f,
                            o,
                            t.get(2).cloned().unwrap_or(Value::Null),
                            rules,
                        )?),
                        _ => return Err("where: a condition is [field, op, value]".into()),
                    }
                    continue;
                }
                let Value::Object(m) = it else {
                    return Err("where: a condition is {field, op, value}".into());
                };
                if let Some(k) = m
                    .keys()
                    .find(|k| !matches!(k.as_str(), "field" | "op" | "value"))
                {
                    return Err(format!("where: unknown key {k:?} in a condition"));
                }
                let field = m
                    .get("field")
                    .and_then(Value::as_str)
                    .ok_or("where: a condition needs a field")?;
                let op = m
                    .get("op")
                    .and_then(Value::as_str)
                    .ok_or("where: a condition needs an op")?;
                out.push(check(
                    field,
                    op,
                    m.get("value").cloned().unwrap_or(Value::Null),
                    rules,
                )?);
            }
            out
        }
        _ => return Err("where: a list of conditions or one string".into()),
    };
    if conds.len() > rules.max_conditions {
        return Err(format!(
            "where: at most {} conditions",
            rules.max_conditions
        ));
    }
    Ok(conds)
}

/// The conditions as the list the catalogue keeps (sorted keys, so the same
/// conditions are the same bytes).
pub fn to_json(conds: &[Cond]) -> Value {
    Value::Array(
        conds
            .iter()
            .map(|c| {
                let mut m = serde_json::Map::new();
                m.insert("field".into(), Value::String(field_text(&c.field)));
                m.insert("op".into(), Value::String(op_text(c.op).into()));
                if c.op != Op::Exists || !c.value.is_null() {
                    m.insert("value".into(), c.value.clone());
                }
                Value::Object(m)
            })
            .collect(),
    )
}

fn value_text(out: &mut String, v: &Value) {
    match v {
        Value::String(s) => {
            out.push('\'');
            out.push_str(&s.replace('\'', "''"));
            out.push('\'');
        }
        Value::Array(a) => {
            out.push('(');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                value_text(out, x);
            }
            out.push(')');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// The conditions as one string: `data.kind in ('a', 'b') and data.total > 100`.
pub fn to_text(conds: &[Cond]) -> String {
    let mut out = String::new();
    for (i, c) in conds.iter().enumerate() {
        if i > 0 {
            out.push_str(" and ");
        }
        out.push_str(&field_text(&c.field));
        out.push(' ');
        out.push_str(op_text(c.op));
        if c.op != Op::Exists {
            out.push(' ');
            value_text(&mut out, &c.value);
        }
    }
    out
}

/// Whether any condition reads the payload: only then is it parsed.
pub fn wants_payload(conds: &[Cond]) -> bool {
    conds.iter().any(|c| matches!(c.field, Field::Data(_)))
}

pub fn all_hold(conds: &[Cond], s: &Subject<'_>) -> bool {
    conds.iter().all(|c| holds(c, s))
}

fn json_eq(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) if a.is_number() && b.is_number() => x == y,
        _ => a == b,
    }
}

fn json_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => a.as_f64()?.partial_cmp(&b.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Whether condition `c` holds for `s`.
pub fn holds(c: &Cond, s: &Subject<'_>) -> bool {
    let owned;
    let v: Option<&Value> = match &c.field {
        Field::Queue => {
            owned = Value::String(s.queue.to_string());
            Some(&owned)
        }
        Field::Partition => {
            owned = Value::String(s.partition.to_string());
            Some(&owned)
        }
        Field::TransactionId => {
            owned = Value::String(s.txn.to_string());
            Some(&owned)
        }
        Field::Offset => match s.offset {
            Some(o) => {
                owned = Value::from(o);
                Some(&owned)
            }
            None => None,
        },
        Field::Position => match s.position {
            Some(p) => {
                owned = Value::from(p);
                Some(&owned)
            }
            None => None,
        },
        Field::Header(k) => match s.headers.and_then(|b| crate::frames::header_get(b, k)) {
            Some(h) => {
                owned = Value::String(h.to_string());
                Some(&owned)
            }
            None => None,
        },
        Field::Data(path) => s.doc.and_then(|d| {
            path.iter().try_fold(d, |cur, key| match cur {
                Value::Object(m) => m.get(key),
                Value::Array(a) => key.parse::<usize>().ok().and_then(|i| a.get(i)),
                _ => None,
            })
        }),
    };
    let Some(v) = v else {
        return false;
    };
    match c.op {
        Op::Exists => true,
        Op::Eq => json_eq(v, &c.value),
        Op::Ne => !json_eq(v, &c.value),
        Op::In => c
            .value
            .as_array()
            .is_some_and(|a| a.iter().any(|x| json_eq(v, x))),
        Op::Prefix => match (v.as_str(), c.value.as_str()) {
            (Some(s), Some(p)) => s.starts_with(p),
            _ => false,
        },
        // A string that does not start with it; like `!=`, a value that is
        // not a string (or no value) holds for neither.
        Op::NotPrefix => match (v.as_str(), c.value.as_str()) {
            (Some(s), Some(p)) => !s.starts_with(p),
            _ => false,
        },
        Op::Lt => json_cmp(v, &c.value).is_some_and(|o| o.is_lt()),
        Op::Le => json_cmp(v, &c.value).is_some_and(|o| o.is_le()),
        Op::Gt => json_cmp(v, &c.value).is_some_and(|o| o.is_gt()),
        Op::Ge => json_cmp(v, &c.value).is_some_and(|o| o.is_ge()),
    }
}

// ---------------------------------------------------------------- the string form

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Word(String),
    Str(String),
    Num(Value),
    Op(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
}

fn lex(s: &str) -> Result<Vec<Tok>, String> {
    let b: Vec<char> = s.chars().collect();
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            '[' => {
                out.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                out.push(Tok::RBracket);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '\'' | '"' => {
                let q = c;
                i += 1;
                let mut v = String::new();
                loop {
                    if i >= b.len() {
                        return Err("where: a string is not closed".into());
                    }
                    if b[i] == q {
                        // A doubled quote is the quote itself ('it''s').
                        if i + 1 < b.len() && b[i + 1] == q {
                            v.push(q);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    if b[i] == '\\' && i + 1 < b.len() {
                        v.push(b[i + 1]);
                        i += 2;
                        continue;
                    }
                    v.push(b[i]);
                    i += 1;
                }
                out.push(Tok::Str(v));
            }
            '=' | '<' | '>' | '!' => {
                let mut op = c.to_string();
                if i + 1 < b.len()
                    && matches!(b[i + 1], '=' | '>')
                    && !(c == '>' && b[i + 1] == '>')
                {
                    op.push(b[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
                // `!prefix`
                if op == "!" {
                    let start = i;
                    while i < b.len() && b[i].is_ascii_alphabetic() {
                        i += 1;
                    }
                    let w: String = b[start..i].iter().collect();
                    if w.eq_ignore_ascii_case("prefix") {
                        out.push(Tok::Op("!prefix".into()));
                        continue;
                    }
                    return Err(format!("where: unexpected '!' before {w:?}"));
                }
                out.push(Tok::Op(op));
            }
            c if c == '-' || c.is_ascii_digit() => {
                let start = i;
                i += 1;
                while i < b.len()
                    && (b[i].is_ascii_digit() || matches!(b[i], '.' | 'e' | 'E' | '+' | '-'))
                {
                    i += 1;
                }
                let t: String = b[start..i].iter().collect();
                let v: Value = serde_json::from_str(&t)
                    .map_err(|_| format!("where: {t:?} is not a number"))?;
                if !v.is_number() {
                    return Err(format!("where: {t:?} is not a number"));
                }
                out.push(Tok::Num(v));
            }
            c if c.is_alphanumeric() || c == '_' => {
                let start = i;
                while i < b.len() && (b[i].is_alphanumeric() || matches!(b[i], '_' | '.' | '-')) {
                    i += 1;
                }
                out.push(Tok::Word(b[start..i].iter().collect()));
            }
            other => return Err(format!("where: unexpected {other:?}")),
        }
    }
    Ok(out)
}

fn scalar(t: &Tok) -> Option<Value> {
    match t {
        Tok::Str(s) => Some(Value::String(s.clone())),
        Tok::Num(n) => Some(n.clone()),
        Tok::Word(w) if w.eq_ignore_ascii_case("true") => Some(Value::Bool(true)),
        Tok::Word(w) if w.eq_ignore_ascii_case("false") => Some(Value::Bool(false)),
        Tok::Word(w) if w.eq_ignore_ascii_case("null") => Some(Value::Null),
        _ => None,
    }
}

/// The string form: `cond (and cond)*`, where a `cond` is
/// `field op value` or `field exists`, a value is a quoted string, a number,
/// `true`, `false` or `null`, and `in` takes `(v, v, ...)`.
pub fn parse_text(s: &str, rules: &Rules) -> Result<Vec<Cond>, String> {
    let toks = lex(s)?;
    let mut out = Vec::new();
    let mut i = 0usize;
    if toks.is_empty() {
        return Ok(out);
    }
    loop {
        let Some(Tok::Word(field)) = toks.get(i) else {
            return Err(format!(
                "where: a condition starts with a field, at token {}",
                i + 1
            ));
        };
        i += 1;
        let op = match toks.get(i) {
            Some(Tok::Op(o)) => o.clone(),
            Some(Tok::Word(w)) => w.clone(),
            _ => return Err(format!("where: {field:?} needs an operator")),
        };
        i += 1;
        let opk = parse_op(&op)?;
        let value = match opk {
            Op::Exists => Value::Null,
            Op::In => {
                let close = match toks.get(i) {
                    Some(Tok::LParen) => Tok::RParen,
                    Some(Tok::LBracket) => Tok::RBracket,
                    _ => return Err(format!("where: in on {field:?} needs (value, value, ...)")),
                };
                i += 1;
                let mut vals = Vec::new();
                loop {
                    if toks.get(i) == Some(&close) && vals.is_empty() {
                        i += 1;
                        break;
                    }
                    let v = toks.get(i).and_then(scalar).ok_or_else(|| {
                        format!("where: in on {field:?}: a value, at token {}", i + 1)
                    })?;
                    vals.push(v);
                    i += 1;
                    match toks.get(i) {
                        Some(Tok::Comma) => i += 1,
                        Some(t) if *t == close => {
                            i += 1;
                            break;
                        }
                        _ => {
                            return Err(format!(
                                "where: in on {field:?}: ',' or the closing bracket"
                            ))
                        }
                    }
                }
                Value::Array(vals)
            }
            _ => {
                let v = toks
                    .get(i)
                    .and_then(scalar)
                    .ok_or_else(|| format!("where: {field:?} {op} needs a value"))?;
                i += 1;
                v
            }
        };
        out.push(check(field, &op, value, rules)?);
        match toks.get(i) {
            None => break,
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("and") => {
                i += 1;
            }
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("or") => {
                return Err(
                    "where: only AND joins conditions; use in (...) for alternatives".into(),
                )
            }
            Some(t) => return Err(format!("where: expected 'and', found {t:?}")),
        }
    }
    if out.len() > rules.max_conditions {
        return Err(format!(
            "where: at most {} conditions",
            rules.max_conditions
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn r() -> Rules {
        Rules {
            max_conditions: 16,
            max_in: 64,
            positions: false,
        }
    }

    #[test]
    fn the_string_and_the_list_say_the_same() {
        let a = parse_text(
            "data.kind in ('order.paid', 'order.refunded') and data.total > 100 and headers.type prefix 'order.'",
            &r(),
        )
        .unwrap();
        let b = parse_value(
            &json!([
                {"field": "data.kind", "op": "in", "value": ["order.paid", "order.refunded"]},
                {"field": "data.total", "op": ">", "value": 100},
                ["headers.type", "prefix", "order."]
            ]),
            &r(),
        )
        .unwrap();
        assert_eq!(a, b);
        assert_eq!(
            to_text(&a),
            "data.kind in ('order.paid', 'order.refunded') and data.total > 100 and headers.type prefix 'order.'"
        );
        assert_eq!(parse_value(&to_json(&a), &r()).unwrap(), a);
        assert_eq!(parse_text(&to_text(&a), &r()).unwrap(), a);
    }

    #[test]
    fn the_string_form_takes_every_operator() {
        for (s, op) in [
            ("data.x = 1", Op::Eq),
            ("data.x == 1", Op::Eq),
            ("data.x != 1", Op::Ne),
            ("data.x < 1", Op::Lt),
            ("data.x <= 1", Op::Le),
            ("data.x > 1", Op::Gt),
            ("data.x >= 1", Op::Ge),
            ("data.x exists", Op::Exists),
            ("data.x prefix 'a'", Op::Prefix),
            ("data.x !prefix 'a'", Op::NotPrefix),
            ("data.x IN (1, 2)", Op::In),
            ("data.x in [1]", Op::In),
        ] {
            let c = parse_text(s, &r()).unwrap_or_else(|e| panic!("{s}: {e}"));
            assert_eq!(c.len(), 1, "{s}");
            assert_eq!(c[0].op, op, "{s}");
        }
        let c = parse_text(
            "data.ok = true AND data.n = -2.5 and data.s = 'it''s' and data.z = null",
            &r(),
        )
        .unwrap();
        assert_eq!(c[0].value, json!(true));
        assert_eq!(c[1].value, json!(-2.5));
        assert_eq!(c[2].value, json!("it's"));
        assert_eq!(c[3].value, Value::Null);
    }

    #[test]
    fn bad_strings_say_why() {
        for (s, says) in [
            ("data.x = 1 or data.y = 2", "only AND"),
            ("data.x", "needs an operator"),
            ("data.x =", "needs a value"),
            ("data.x in 1", "needs (value"),
            ("data.x = 'open", "not closed"),
            ("offset > 3", "known only once"),
            ("data..x = 1", "bad field path"),
            ("headers.Type = 'a'", "bad header name"),
            ("nope = 1", "unknown field"),
            ("data.x like 'a'", "unknown op"),
        ] {
            let e = parse_text(s, &r()).unwrap_err();
            assert!(e.contains(says), "{s}: {e}");
        }
        let many = (0..17)
            .map(|i| format!("data.f{i} = 1"))
            .collect::<Vec<_>>()
            .join(" and ");
        assert!(parse_text(&many, &r()).unwrap_err().contains("at most 16"));
        let wide = format!(
            "data.x in ({})",
            (0..65)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        assert!(parse_text(&wide, &r()).unwrap_err().contains("at most 64"));
    }

    #[test]
    fn conditions_read_payload_headers_and_names() {
        let doc = json!({"kind": "order.paid", "total": 120, "tags": ["vip"]});
        let headers = crate::frames::encode_headers([("type", "order.paid"), ("source", "web")]);
        let s = Subject {
            queue: "orders",
            partition: "order-9137",
            txn: "order-9137-paid",
            doc: Some(&doc),
            headers: Some(&headers),
            ..Subject::default()
        };
        let yes = |t: &str| all_hold(&parse_text(t, &r()).unwrap(), &s);
        assert!(yes("data.kind = 'order.paid'"));
        assert!(yes("data.total >= 120 and data.total < 121"));
        assert!(yes("data.tags.0 = 'vip'"));
        assert!(yes(
            "headers.type = 'order.paid' and headers.source in ('web', 'app')"
        ));
        assert!(yes("partition prefix 'order-' and queue = 'orders'"));
        assert!(yes("transactionId !prefix 'refund-'"));
        assert!(!yes("headers.missing = 'x'"));
        assert!(
            !yes("headers.missing != 'x'"),
            "a missing field holds for no operator"
        );
        assert!(!yes("data.missing exists"));
        assert!(yes("data.kind exists"));
        assert!(!wants_payload(
            &parse_text("headers.type = 'a' and partition = 'p'", &r()).unwrap()
        ));
        assert!(wants_payload(&parse_text("data.kind = 'a'", &r()).unwrap()));
    }
}
