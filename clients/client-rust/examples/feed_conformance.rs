//! The Rust SDK's runner for the feed-stream conformance kit
//! (clients/conformance/feed/README.md, sections 1 and 3): builds a feed
//! stream from a case file with this SDK and runs it, speaking JSON lines on
//! stdout and taking commands on stdin.
//!
//! ```text
//! cargo build --example feed_conformance
//! target/debug/examples/feed_conformance run  --url <url> --case <case.json> [--run '<json>']
//! target/debug/examples/feed_conformance view --url <url> --query-id <id> --op get|list|status [--step s] [--key k] [--limit n] [--shards n] [--timeout-ms n]
//! ```

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex, OnceLock};

use queen_mq::feed::{FeedFrom, Since};
use queen_mq::streams::feed::{
    js_number, js_str_cmp, js_string, FeedAggregate, FeedDuration, FeedHandle, FeedRunOptions,
    FeedSink, FeedSource, FeedStream, FeedWindow, Gather, Join, Pattern, Poison, Reset, ShardBy,
    TableJoin, ViewOptions,
};
use queen_mq::streams::Stream;
use queen_mq::{Config, Queen};
use serde_json::{json, Map, Value};

// ------------------------------------------------------------- ordered JSON

/// JSON with its object keys in the order JavaScript gives them
/// (`Object.keys`: array-index keys ascending first, then the others as
/// written): aggregate fields, sides and steps depend on that order.
#[derive(Clone, Debug)]
enum J {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\n' | b'\r' | b'\t') {
            self.i += 1;
        }
    }
    fn value(&mut self) -> Result<J, String> {
        self.ws();
        let c = *self.s.get(self.i).ok_or("unexpected end")?;
        match c {
            b'{' => {
                self.i += 1;
                let mut out: Vec<(String, J)> = Vec::new();
                self.ws();
                if self.s.get(self.i) == Some(&b'}') {
                    self.i += 1;
                    return Ok(J::Obj(out));
                }
                loop {
                    self.ws();
                    let k = self.string()?;
                    self.ws();
                    if self.s.get(self.i) != Some(&b':') {
                        return Err("expected :".into());
                    }
                    self.i += 1;
                    let v = self.value()?;
                    if let Some(slot) = out.iter_mut().find(|(x, _)| *x == k) {
                        slot.1 = v;
                    } else {
                        out.push((k, v));
                    }
                    self.ws();
                    match self.s.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            break;
                        }
                        _ => return Err("expected , or }".into()),
                    }
                }
                // Object.keys: array indexes first, ascending.
                let is_index = |k: &str| {
                    !k.is_empty()
                        && k.bytes().all(|b| b.is_ascii_digit())
                        && (k == "0" || !k.starts_with('0'))
                        && k.parse::<u64>().is_ok_and(|n| n < u32::MAX as u64)
                };
                let mut idx: Vec<(String, J)> =
                    out.iter().filter(|(k, _)| is_index(k)).cloned().collect();
                idx.sort_by_key(|(k, _)| k.parse::<u64>().unwrap_or(0));
                let rest: Vec<(String, J)> =
                    out.into_iter().filter(|(k, _)| !is_index(k)).collect();
                idx.extend(rest);
                Ok(J::Obj(idx))
            }
            b'[' => {
                self.i += 1;
                let mut out = Vec::new();
                self.ws();
                if self.s.get(self.i) == Some(&b']') {
                    self.i += 1;
                    return Ok(J::Arr(out));
                }
                loop {
                    out.push(self.value()?);
                    self.ws();
                    match self.s.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            break;
                        }
                        _ => return Err("expected , or ]".into()),
                    }
                }
                Ok(J::Arr(out))
            }
            b'"' => Ok(J::Str(self.string()?)),
            b't' if self.s[self.i..].starts_with(b"true") => {
                self.i += 4;
                Ok(J::Bool(true))
            }
            b'f' if self.s[self.i..].starts_with(b"false") => {
                self.i += 5;
                Ok(J::Bool(false))
            }
            b'n' if self.s[self.i..].starts_with(b"null") => {
                self.i += 4;
                Ok(J::Null)
            }
            _ => {
                let start = self.i;
                while self.i < self.s.len()
                    && matches!(
                        self.s[self.i],
                        b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'
                    )
                {
                    self.i += 1;
                }
                let t = std::str::from_utf8(&self.s[start..self.i]).map_err(|e| e.to_string())?;
                t.parse::<f64>()
                    .map(J::Num)
                    .map_err(|_| format!("bad number {t}"))
            }
        }
    }
    fn string(&mut self) -> Result<String, String> {
        if self.s.get(self.i) != Some(&b'"') {
            return Err("expected a string".into());
        }
        // serde_json reads the string with its escapes.
        let start = self.i;
        self.i += 1;
        while self.i < self.s.len() {
            match self.s[self.i] {
                b'\\' => self.i += 2,
                b'"' => {
                    self.i += 1;
                    let raw =
                        std::str::from_utf8(&self.s[start..self.i]).map_err(|e| e.to_string())?;
                    return serde_json::from_str::<String>(raw).map_err(|e| e.to_string());
                }
                _ => self.i += 1,
            }
        }
        Err("unterminated string".into())
    }
}

fn parse(text: &str) -> Result<J, String> {
    let mut p = Parser {
        s: text.as_bytes(),
        i: 0,
    };
    p.value()
}

impl J {
    fn get(&self, k: &str) -> Option<&J> {
        match self {
            J::Obj(o) => o.iter().find(|(x, _)| x == k).map(|(_, v)| v),
            _ => None,
        }
    }
    fn str(&self) -> Option<&str> {
        match self {
            J::Str(s) => Some(s),
            _ => None,
        }
    }
    fn num(&self) -> Option<f64> {
        match self {
            J::Num(n) => Some(*n),
            _ => None,
        }
    }
    fn to_value(&self) -> Value {
        match self {
            J::Null => Value::Null,
            J::Bool(b) => Value::Bool(*b),
            J::Num(n) => js_number(*n),
            J::Str(s) => Value::String(s.clone()),
            J::Arr(a) => Value::Array(a.iter().map(J::to_value).collect()),
            J::Obj(o) => Value::Object(o.iter().map(|(k, v)| (k.clone(), v.to_value())).collect()),
        }
    }
}

// --------------------------------------------------------------- expressions

fn flaky() -> &'static Mutex<HashMap<String, u64>> {
    static F: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    F.get_or_init(|| Mutex::new(HashMap::new()))
}

fn truthy(v: &Option<Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|x| x != 0.0 && !x.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

fn deep_eq(a: &Option<Value>, b: &Option<Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (None, _) | (_, None) => false,
        (Some(x), Some(y)) => value_eq(x, y),
    }
}

fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| value_eq(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| value_eq(v, w)))
        }
        _ => a == b,
    }
}

fn get_path(args: &[Option<Value>], path: &str) -> Option<Value> {
    let segs: Vec<&str> = path.split('.').collect();
    let first: usize = segs[0].trim().parse().ok()?;
    let mut cur: Value = args.get(first)?.clone()?;
    for seg in &segs[1..] {
        cur = match cur {
            Value::Array(items) => {
                let i: f64 = if seg.trim().is_empty() {
                    0.0
                } else {
                    seg.trim().parse().ok()?
                };
                if i < 0.0 || i.fract() != 0.0 {
                    return None;
                }
                items.into_iter().nth(i as usize)?
            }
            Value::Object(mut m) => m.remove(*seg)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn cmp(a: &Option<Value>, b: &Option<Value>) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Some(Value::Number(x)), Some(Value::Number(y))) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Some(Value::String(x)), Some(Value::String(y))) => Some(js_str_cmp(x, y)),
        _ => None,
    }
}

/// An expression's value with the function's arguments; `None` is MISSING.
fn eval(e: &J, args: &[Option<Value>]) -> Result<Option<Value>, String> {
    let ev = |x: &J| eval(x, args);
    match e {
        J::Null | J::Bool(_) | J::Num(_) | J::Str(_) => Ok(Some(e.to_value())),
        J::Arr(items) => {
            let mut out = Vec::new();
            for x in items {
                out.push(ev(x)?.unwrap_or(Value::Null));
            }
            Ok(Some(Value::Array(out)))
        }
        J::Obj(fields) => {
            let op = fields.iter().find(|(k, _)| k.starts_with('$'));
            let Some((op, a)) = op else {
                let mut m = Map::new();
                for (k, x) in fields {
                    if let Some(v) = ev(x)? {
                        m.insert(k.clone(), v);
                    }
                }
                return Ok(Some(Value::Object(m)));
            };
            if fields.len() != 1 {
                return Err(format!("expression: {op} stands alone in its object"));
            }
            let list = |a: &J| -> Vec<J> {
                match a {
                    J::Arr(x) => x.clone(),
                    other => vec![other.clone()],
                }
            };
            let two = |a: &J| -> Result<(Option<Value>, Option<Value>), String> {
                let l = list(a);
                Ok((
                    ev(l.first().unwrap_or(&J::Null))?,
                    ev(l.get(1).unwrap_or(&J::Null))?,
                ))
            };
            let b = |x: bool| Ok(Some(Value::Bool(x)));
            match op.as_str() {
                "$get" => {
                    let path = match a {
                        J::Str(s) => s.clone(),
                        J::Num(n) => js_string(Some(&js_number(*n))),
                        other => js_string(Some(&other.to_value())),
                    };
                    Ok(get_path(args, &path))
                }
                "$eq" => {
                    let (x, y) = two(a)?;
                    b(deep_eq(&x, &y))
                }
                "$ne" => {
                    let (x, y) = two(a)?;
                    b(!deep_eq(&x, &y))
                }
                "$lt" => {
                    let (x, y) = two(a)?;
                    b(cmp(&x, &y) == Some(std::cmp::Ordering::Less))
                }
                "$lte" => {
                    let (x, y) = two(a)?;
                    b(matches!(
                        cmp(&x, &y),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    ))
                }
                "$gt" => {
                    let (x, y) = two(a)?;
                    b(cmp(&x, &y) == Some(std::cmp::Ordering::Greater))
                }
                "$gte" => {
                    let (x, y) = two(a)?;
                    b(matches!(
                        cmp(&x, &y),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ))
                }
                "$and" => {
                    for x in list(a) {
                        if !truthy(&ev(&x)?) {
                            return b(false);
                        }
                    }
                    b(true)
                }
                "$or" => {
                    for x in list(a) {
                        if truthy(&ev(&x)?) {
                            return b(true);
                        }
                    }
                    b(false)
                }
                "$not" => b(!truthy(&ev(a)?)),
                "$if" => {
                    let l = list(a);
                    if truthy(&ev(l.first().unwrap_or(&J::Null))?) {
                        ev(l.get(1).unwrap_or(&J::Null))
                    } else {
                        ev(l.get(2).unwrap_or(&J::Null))
                    }
                }
                "$add" => {
                    let mut s = 0f64;
                    for x in list(a) {
                        if let Some(Value::Number(n)) = ev(&x)? {
                            s += n.as_f64().unwrap_or(0.0);
                        }
                    }
                    Ok(Some(js_number(s)))
                }
                "$mod" => {
                    let (x, y) = two(a)?;
                    match (
                        x.as_ref().and_then(|v| v.as_f64()),
                        y.as_ref().and_then(|v| v.as_f64()),
                    ) {
                        (Some(x), Some(y)) => Ok(Some(js_number(x % y))),
                        _ => Ok(Some(Value::Null)),
                    }
                }
                "$str" => Ok(Some(Value::String(js_string(ev(a)?.as_ref())))),
                "$concat" => {
                    let mut out = String::new();
                    for x in list(a) {
                        if let Some(v) = ev(&x)? {
                            out.push_str(&js_string(Some(&v)));
                        }
                    }
                    Ok(Some(Value::String(out)))
                }
                "$len" => Ok(Some(match ev(a)? {
                    Some(Value::Array(x)) => json!(x.len()),
                    Some(Value::String(s)) => json!(s.encode_utf16().count()),
                    _ => json!(0),
                })),
                "$keys" => Ok(Some(match ev(a)? {
                    Some(Value::Object(m)) => {
                        let mut keys: Vec<String> = m.keys().cloned().collect();
                        keys.sort_by(|x, y| js_str_cmp(x, y));
                        json!(keys)
                    }
                    _ => json!([]),
                })),
                "$missing" => Ok(None),
                "$throw" => Err(match a {
                    J::Str(s) => s.clone(),
                    other => js_string(Some(&other.to_value())),
                }),
                "$flaky" => {
                    let key = js_string(ev(a.get("key").unwrap_or(&J::Null))?.as_ref());
                    let times = a.get("times").and_then(J::num).unwrap_or(0.0) as u64;
                    let n = {
                        let mut f = flaky().lock().unwrap();
                        let n = *f.get(&key).unwrap_or(&0);
                        if n < times {
                            f.insert(key.clone(), n + 1);
                        }
                        n
                    };
                    if n < times {
                        return Err(format!("flaky {key} {}", n + 1));
                    }
                    ev(a.get("then").unwrap_or(&J::Null))
                }
                other => Err(format!("expression: unknown operator {other}")),
            }
        }
    }
}

type Args = Vec<Option<Value>>;

fn f1(e: &J) -> impl Fn(&Value) -> Result<Option<Value>, String> + Send + Sync + 'static {
    let e = e.clone();
    move |a| eval(&e, &[Some(a.clone())] as &[Option<Value>])
}

fn f2(
    e: &J,
) -> impl Fn(&Value, Option<&Value>) -> Result<Option<Value>, String> + Send + Sync + 'static {
    let e = e.clone();
    move |a, b| eval(&e, &[Some(a.clone()), b.cloned()] as &[Option<Value>])
}

fn fpair(
    e: &J,
) -> impl Fn(&Value, &Value) -> Result<Option<Value>, String> + Send + Sync + 'static {
    let e = e.clone();
    move |a, b| eval(&e, &[Some(a.clone()), Some(b.clone())] as &[Option<Value>])
}

fn fgot(e: &J) -> impl Fn(&Value, &str) -> Result<Option<Value>, String> + Send + Sync + 'static {
    let e = e.clone();
    move |g, k| {
        eval(
            &e,
            &[Some(g.clone()), Some(Value::String(k.into()))] as &[Option<Value>],
        )
    }
}

fn fgot3(
    e: &J,
) -> impl Fn(&Value, &Value, &str) -> Result<Option<Value>, String> + Send + Sync + 'static {
    let e = e.clone();
    move |g, m, k| {
        eval(
            &e,
            &[
                Some(g.clone()),
                Some(m.clone()),
                Some(Value::String(k.into())),
            ] as &[Option<Value>],
        )
    }
}

// ----------------------------------------------------------------- the chain

fn say(v: Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", serde_json::to_string(&v).unwrap_or_default());
    let _ = out.flush();
}

fn duration(j: &J) -> Option<FeedDuration> {
    match j {
        J::Num(n) => Some(FeedDuration::Seconds(*n)),
        J::Str(s) => Some(FeedDuration::Text(s.clone())),
        _ => None,
    }
}

fn window(kind: &str, o: &J) -> FeedWindow {
    let n = |k: &str| o.get(k).and_then(J::num).unwrap_or(f64::NAN);
    let mut w = match kind {
        "windowTumbling" => FeedWindow::tumbling(n("seconds")),
        "windowSliding" => FeedWindow::sliding(n("size"), n("slide")),
        "windowSession" => FeedWindow::session(n("gap")),
        _ => FeedWindow::cron(o.get("every").and_then(J::str).unwrap_or("")),
    };
    if let Some(g) = o.get("gracePeriod").and_then(J::num) {
        w = w.grace_period(g);
    }
    if let Some(g) = o.get("idleFlushMs").and_then(J::num) {
        w = w.idle_flush_ms(g);
    }
    match o.get("eventTime") {
        Some(J::Bool(false)) => w = w.feed_clock(),
        Some(J::Null) | None => {}
        Some(e) => w = w.event_time(f1(e)),
    }
    if let Some(d) = o.get("allowedLateness").and_then(duration) {
        w = w.allowed_lateness(d);
    }
    if let Some(p) = o.get("onLate").and_then(J::str) {
        w = w.on_late(p);
    }
    if let Some(d) = o.get("idle").and_then(duration) {
        w = w.idle(d);
    }
    w
}

macro_rules! time_opts {
    ($b:expr, $o:expr) => {{
        let mut b = $b;
        match $o.get("eventTime") {
            Some(J::Bool(false)) => b = b.feed_clock(),
            Some(J::Null) | None => {}
            Some(e) => b = b.event_time(f1(e)),
        }
        if let Some(d) = $o.get("allowedLateness").and_then(duration) {
            b = b.allowed_lateness(d);
        }
        if let Some(p) = $o.get("onLate").and_then(J::str) {
            b = b.on_late(p);
        }
        if let Some(d) = $o.get("idle").and_then(duration) {
            b = b.idle(d);
        }
        if let Some(n) = $o.get("name").and_then(J::str) {
            b = b.name(n);
        }
        if let Some(r) = $o.get("onRepeat").and_then(J::str) {
            b = b.on_repeat(r);
        }
        if let Some(e) = $o.get("key") {
            b = b.key(f2(e));
        }
        if let Some(e) = $o.get("keep") {
            b = b.keep(f2(e));
        }
        b
    }};
}

fn within(o: &J) -> String {
    o.get("within").and_then(J::str).unwrap_or("").to_string()
}

fn build(queen: &Queen, c: &J) -> FeedStream {
    let src = c.get("source").cloned().unwrap_or(J::Obj(Vec::new()));
    let mut source = FeedSource::new();
    let names = |j: &J| -> Vec<String> {
        match j {
            J::Arr(a) => a
                .iter()
                .filter_map(|x| x.str().map(str::to_string))
                .collect(),
            J::Str(s) => vec![s.clone()],
            _ => Vec::new(),
        }
    };
    if let Some(q) = src.get("queues").or(src.get("queue")) {
        source = source.queues(names(q));
    }
    if let Some(p) = src.get("partitions").or(src.get("partition")) {
        source = source.partitions(names(p));
    }
    if let Some(f) = src
        .get("from")
        .and_then(|f| FeedFrom::from_value(&f.to_value()))
    {
        source = source.from(f);
    }
    if let Some(s) = src.get("since") {
        source = match s {
            J::Num(n) => source.since(Since::Millis(*n)),
            J::Str(t) => source.since(t.as_str()),
            _ => source,
        };
    }
    if let Some(f) = src.get("followerUrls") {
        source = source.follower_urls(names(f));
    }
    if let Some(n) = src.get("maxRecords").and_then(J::num) {
        source = source.max_records(n as u64);
    }
    let mut s = Stream::from_feed(queen, source);
    let empty = Vec::new();
    let chain = match c.get("chain") {
        Some(J::Arr(a)) => a,
        _ => &empty,
    };
    for st in chain {
        let op = st.get("op").and_then(J::str).unwrap_or("");
        let o = st.get("opts").cloned().unwrap_or(J::Obj(Vec::new()));
        let fnx = st.get("fn").cloned().unwrap_or(J::Null);
        s = match op {
            "where" => {
                let args = match st.get("args") {
                    Some(J::Arr(a)) => a.clone(),
                    _ => Vec::new(),
                };
                let field = args.first().and_then(J::str).unwrap_or("").to_string();
                let wop = args.get(1).and_then(J::str).unwrap_or("").to_string();
                let value = args.get(2).map(J::to_value);
                s.where_(&field, &wop, value)
            }
            "filter" => s.filter(f2(&fnx)),
            "map" => s.map(f2(&fnx)),
            "flatMap" => s.flat_map(f2(&fnx)),
            "keyBy" => s.key_by(f1(&fnx)),
            "windowTumbling" | "windowSliding" | "windowSession" | "windowCron" => {
                s.window(window(op, &o))
            }
            "reduce" => {
                let initial = st.get("initial").map(J::to_value);
                let e = fnx.clone();
                s.reduce(initial, move |acc, v| {
                    eval(&e, &[acc, Some(v.clone())] as &[Option<Value>])
                })
            }
            "aggregate" => {
                let mut a = FeedAggregate::new();
                if let Some(J::Obj(fields)) = st.get("fields") {
                    for (name, e) in fields {
                        a = a.field(name, f1(e));
                    }
                }
                s.aggregate(a)
            }
            "pattern" => {
                let mut p = Pattern::new(
                    f1(o.get("start").unwrap_or(&J::Null)),
                    f1(o.get("then").unwrap_or(&J::Null)),
                    &within(&o),
                );
                if let Some(e) = o.get("onMatch") {
                    p = p.on_match(fpair(e));
                }
                if let Some(e) = o.get("onTimeout") {
                    p = p.on_timeout(f1(e));
                }
                s.pattern(time_opts!(p, o))
            }
            "join" => {
                if let Some(sides) = o.get("sides") {
                    let mut j = Join::sides(&within(&o));
                    match sides {
                        J::Arr(list) => {
                            for q in list {
                                if let Some(q) = q.str() {
                                    j = j.side_queue(q, q);
                                }
                            }
                        }
                        J::Obj(map) => {
                            for (name, t) in map {
                                j = match t {
                                    J::Str(q) => j.side_queue(name, q),
                                    e => j.side(name, f2(e)),
                                };
                            }
                        }
                        _ => {}
                    }
                    if let Some(e) = o.get("select") {
                        j = j.select_all(fgot(e));
                    }
                    if let Some(e) = o.get("onTimeout") {
                        j = j.on_timeout_all(fgot3(e));
                    }
                    s.join(time_opts!(j, o))
                } else {
                    let mut j = Join::new(
                        f1(o.get("left").unwrap_or(&J::Null)),
                        f1(o.get("right").unwrap_or(&J::Null)),
                        &within(&o),
                    );
                    if let Some(e) = o.get("select") {
                        j = j.select(fpair(e));
                    }
                    if let Some(e) = o.get("onTimeout") {
                        let e = e.clone();
                        j = j.on_timeout(move |m, side| {
                            eval(
                                &e,
                                &[Some(m.clone()), Some(Value::String(side.into()))]
                                    as &[Option<Value>],
                            )
                        });
                    }
                    s.join(time_opts!(j, o))
                }
            }
            "gather" => {
                let mut g = match (o.get("steps"), o.get("count")) {
                    (Some(steps), count) => {
                        let mut g = Gather::steps(&within(&o));
                        match steps {
                            J::Arr(list) => {
                                for q in list {
                                    if let Some(q) = q.str() {
                                        g = g.step_queue(q);
                                    }
                                }
                            }
                            J::Obj(map) => {
                                for (name, t) in map {
                                    g = match t {
                                        J::Str(q) => {
                                            let q = q.clone();
                                            g.step(name, move |m: &Value, _v: Option<&Value>| {
                                                m.get("queue").and_then(|x| x.as_str())
                                                    == Some(q.as_str())
                                            })
                                        }
                                        e => g.step(name, f2(e)),
                                    };
                                }
                            }
                            _ => {}
                        }
                        if let Some(c) = count {
                            g = g.with_count(f2(c));
                        }
                        g
                    }
                    (None, Some(c)) => Gather::count(&within(&o), f2(c)),
                    (None, None) => Gather::steps(&within(&o)).without_steps(),
                };
                if let Some(e) = o.get("onComplete") {
                    g = g.on_complete(fgot(e));
                }
                if let Some(e) = o.get("onTimeout") {
                    g = g.on_timeout(fgot3(e));
                }
                s.gather(time_opts!(g, o))
            }
            "joinTable" => {
                let queues = match o.get("queue") {
                    Some(J::Arr(a)) => a
                        .iter()
                        .filter_map(|x| x.str().map(str::to_string))
                        .collect(),
                    Some(J::Str(q)) => vec![q.clone()],
                    _ => Vec::new(),
                };
                let mut t = TableJoin::queues(queues, f2(o.get("on").unwrap_or(&J::Null)));
                if let Some(e) = o.get("key") {
                    t = t.key(f1(e));
                }
                if let Some(e) = o.get("value") {
                    t = t.value(f1(e));
                }
                if let Some(e) = o.get("select") {
                    let e = e.clone();
                    t = t.select(move |v, row, m| {
                        eval(
                            &e,
                            &[v.cloned(), row.cloned(), Some(m.clone())] as &[Option<Value>],
                        )
                    });
                }
                if let Some(m) = o.get("missing").and_then(J::str) {
                    t = t.missing(m);
                }
                if let Some(i) = o.get("initial").and_then(J::str) {
                    t = t.initial(i);
                }
                if let Some(n) = o.get("name").and_then(J::str) {
                    t = t.name(n);
                }
                s.join_table(t)
            }
            "name" => s.name(st.get("name").and_then(J::str).unwrap_or("")),
            "to" => {
                let target = st.get("target").cloned().unwrap_or(J::Null);
                let mut sink = match &target {
                    J::Str(q) => FeedSink::queue(q.clone()),
                    J::Arr(list) => {
                        FeedSink::queues(list.iter().filter_map(|x| x.str().map(str::to_string)))
                    }
                    other => {
                        let e = other.get("fn").cloned().unwrap_or(J::Null);
                        FeedSink::route(move |v: Option<Value>, m: Value| {
                            let e = e.clone();
                            async move { eval(&e, &[v, Some(m)] as &[Option<Value>]) }
                        })
                    }
                };
                match st.get("partition") {
                    Some(J::Str(p)) => sink = sink.partition(p.clone()),
                    Some(p @ J::Obj(_)) => {
                        let e = p.get("fn").cloned().unwrap_or(J::Null);
                        sink =
                            sink.partition_by(move |v| eval(&e, &[v.cloned()] as &[Option<Value>]));
                    }
                    _ => {}
                }
                s.to(sink)
            }
            "foreach" => {
                let e = st.get("fn").cloned();
                s.foreach(move |value: Option<Value>, ctx: Value| {
                    let e = e.clone();
                    async move {
                        let mut line = Map::new();
                        line.insert("event".into(), json!("foreach"));
                        if let Some(v) = &value {
                            line.insert("value".into(), v.clone());
                        }
                        line.insert("ctx".into(), ctx.clone());
                        say(Value::Object(line));
                        match e {
                            None => Ok::<(), String>(()),
                            Some(e) => {
                                eval(&e, &[value, Some(ctx)] as &[Option<Value>]).map(|_| ())
                            }
                        }
                    }
                })
            }
            other => {
                say(
                    json!({ "event": "failed", "message": format!("case: unknown chain op {other}") }),
                );
                std::process::exit(2);
            }
        };
    }
    s
}

fn run_options(c: &J, overrides: &J) -> FeedRunOptions {
    let mut merged: Vec<(String, J)> = match c.get("run") {
        Some(J::Obj(o)) => o.clone(),
        _ => Vec::new(),
    };
    if let J::Obj(o) = overrides {
        for (k, v) in o {
            if let Some(slot) = merged.iter_mut().find(|(x, _)| x == k) {
                slot.1 = v.clone();
            } else {
                merged.push((k.clone(), v.clone()));
            }
        }
    }
    let r = J::Obj(merged);
    let mut o = FeedRunOptions::new(r.get("queryId").and_then(J::str).unwrap_or(""));
    let num = |k: &str| r.get(k).and_then(J::num);
    if let Some(f) = r
        .get("from")
        .and_then(|f| FeedFrom::from_value(&f.to_value()))
    {
        o = o.from(f);
    }
    if let Some(n) = num("waitMillis") {
        o = o.wait_millis(n as u64);
    }
    if let Some(n) = num("maxRecords") {
        o = o.max_records(n as u64);
    }
    if let Some(n) = num("maxBytes") {
        o.max_bytes = Some(n as u64);
    }
    match r.get("reset") {
        Some(J::Bool(true)) => o = o.reset(Reset::Always),
        Some(J::Str(t)) => o = o.reset(Reset::Token(t.clone())),
        _ => {}
    }
    if let Some(n) = num("positionCommitMillis") {
        o = o.position_commit_millis(n);
    }
    if let Some(n) = num("commitEveryMillis") {
        o = o.commit_every_millis(n);
    }
    if let Some(n) = num("maxPendingOutputs") {
        o = o.max_pending_outputs(n as usize);
    }
    if let Some(J::Bool(b)) = r.get("exclusive") {
        o = o.exclusive(*b);
    }
    match r.get("lockTtl") {
        Some(J::Str(t)) => o = o.lock_ttl(t.as_str()),
        Some(J::Num(s)) => o = o.lock_ttl(*s),
        _ => {}
    }
    match r.get("onPoison").and_then(J::str) {
        Some("stop") | None => {}
        Some("skip") => o = o.on_poison(Poison::Skip),
        Some(q) => o = o.on_poison(Poison::DeadLetter(q.to_string())),
    }
    if let Some(n) = num("retries") {
        o = o.retries(n as u32);
    }
    if let Some(n) = num("retryDelayMillis") {
        o = o.retry_delay_millis(n);
    }
    if let Some(n) = num("snapshotEveryMillis") {
        o = o.snapshot_every_millis(n);
    }
    if let Some(n) = num("snapshotMaxEntries") {
        o = o.snapshot_max_entries(n as usize);
    }
    if let Some(n) = num("snapshotChunkChars") {
        o = o.snapshot_chunk_chars(n as usize);
    }
    if let Some(n) = num("snapshotPieceBytes") {
        o = o.snapshot_piece_bytes(n as usize);
    }
    if let Some(sh) = r.get("shard") {
        let i = sh.get("index").and_then(J::num).unwrap_or(0.0) as u32;
        let n = sh.get("count").and_then(J::num).unwrap_or(1.0) as u32;
        o = o.shard(i, n);
    }
    if let Some(n) = num("shards") {
        o = o.shards(n as u32);
    }
    match r.get("shardBy") {
        None | Some(J::Null) => {}
        Some(J::Str(p)) if p == "partition" => {}
        Some(e) => o = o.shard_by(ShardBy::key(f1(e))),
    }
    if let Some(J::Bool(b)) = r.get("serveState") {
        o = o.serve_state(*b);
    }
    if let Some(n) = num("serveWaitMillis") {
        o = o.serve_wait_millis(n as u64);
    }
    o.on_error(|info| {
        let mut line = Map::new();
        line.insert("event".into(), json!("error"));
        line.insert("message".into(), json!(info.message));
        for k in ["phase", "action"] {
            if let Some(v) = info.ctx.get(k) {
                line.insert(k.into(), v.clone());
            }
        }
        if let Some(c) = &info.code {
            line.insert("code".into(), json!(c));
        }
        say(Value::Object(line));
    })
}

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let flag = format!("--{name}");
    args.iter()
        .position(|a| *a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}

fn failed(err: &queen_mq::Error) -> Value {
    let mut m = Map::new();
    m.insert("event".into(), json!("failed"));
    m.insert("message".into(), json!(err.to_string()));
    if let Some(c) = err.code() {
        m.insert("code".into(), json!(c.as_str()));
    }
    Value::Object(m)
}

#[tokio::main]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let Some(url) = arg("url") else {
        say(json!({ "event": "failed", "message": "--url is required" }));
        std::process::exit(2);
    };
    let queen = match Queen::connect(Config::new(url)) {
        Ok(q) => q,
        Err(e) => {
            say(failed(&e));
            std::process::exit(2);
        }
    };

    if mode == "view" {
        let opts = ViewOptions {
            shards: arg("shards").and_then(|s| s.parse().ok()),
            timeout_millis: arg("timeout-ms")
                .and_then(|s| s.parse().ok())
                .unwrap_or(3000),
        };
        let view = match Stream::view(&queen, &arg("query-id").unwrap_or_default(), opts) {
            Ok(v) => v,
            Err(e) => {
                say(failed(&e));
                std::process::exit(0);
            }
        };
        let op = arg("op").unwrap_or_default();
        let answer = match op.as_str() {
            "get" => {
                view.get(
                    &arg("step").unwrap_or_default(),
                    &arg("key").unwrap_or_default(),
                )
                .await
            }
            "list" => {
                let limit = arg("limit").and_then(|l| l.parse().ok()).unwrap_or(100);
                view.list(&arg("step").unwrap_or_default(), limit).await
            }
            _ => view.status().await.map(Value::Array),
        };
        match answer {
            Ok(a) => say(json!({ "event": "view", "answer": a })),
            Err(e) => say(failed(&e)),
        }
        std::process::exit(0);
    }

    if mode != "run" {
        say(json!({ "event": "failed", "message": "mode is run or view" }));
        std::process::exit(2);
    }
    let text = std::fs::read_to_string(arg("case").unwrap_or_default()).unwrap_or_default();
    let case = match parse(&text) {
        Ok(c) => c,
        Err(e) => {
            say(json!({ "event": "failed", "message": format!("case: {e}") }));
            std::process::exit(2);
        }
    };
    let overrides = arg("run")
        .and_then(|r| parse(&r).ok())
        .unwrap_or(J::Obj(Vec::new()));
    let stream = build(&queen, &case);
    let handle: FeedHandle = match stream.run(run_options(&case, &overrides)).await {
        Ok(h) => h,
        Err(e) => {
            say(failed(&e));
            std::process::exit(2);
        }
    };
    let handle = Arc::new(handle);
    let m = handle.metrics();
    let role = match m.get("role") {
        Some(Value::String(r)) => r.clone(),
        _ => {
            if m.get("running").and_then(|r| r.as_u64()).unwrap_or(0) > 0 {
                "active".into()
            } else {
                "standby".into()
            }
        }
    };
    say(json!({ "event": "started", "role": role }));

    let ending = Arc::new(Mutex::new(false));
    let end = {
        let handle = Arc::clone(&handle);
        let ending = Arc::clone(&ending);
        move |event: &'static str| {
            let handle = Arc::clone(&handle);
            let ending = Arc::clone(&ending);
            async move {
                {
                    let mut e = ending.lock().unwrap();
                    if *e {
                        return;
                    }
                    *e = true;
                }
                if event == "stopped" {
                    handle.stop().await;
                }
                say(json!({ "event": event, "metrics": handle.metrics() }));
                std::process::exit(0);
            }
        }
    };
    {
        let handle = Arc::clone(&handle);
        let end = end.clone();
        tokio::spawn(async move {
            handle.done().await;
            end("done").await;
        });
    }
    let stdin = tokio::io::stdin();
    let mut lines = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(stdin));
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            _ => break,
        };
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.first().copied() {
            Some("stop") => {
                end("stopped").await;
            }
            Some("metrics") => say(json!({ "event": "metrics", "metrics": handle.metrics() })),
            Some("state") => {
                let step = parts.get(1).copied().unwrap_or("");
                let key = parts.get(2..).map(|k| k.join(" ")).unwrap_or_default();
                say(json!({ "event": "state", "answer": handle.state(step).get(&key) }));
            }
            Some("list") => {
                let step = parts.get(1).copied().unwrap_or("");
                let limit = parts.get(2).and_then(|l| l.parse().ok()).unwrap_or(100);
                say(json!({ "event": "state", "answer": handle.state(step).list(limit) }));
            }
            _ => {}
        }
    }
    end("stopped").await;
}

#[allow(dead_code)]
fn unused(_: Args) {}
