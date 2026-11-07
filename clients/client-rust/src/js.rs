//! JavaScript's rules for the few things the feed streams write or compare
//! across SDKs: number text, `JSON.stringify`, `String(v)`, truthiness, string
//! order, `Date.parse` / `toISOString`, and the duration strings.
//!
//! A feed stream keeps its state as JSON in KV and names its outputs by
//! hashes, and a stream must move between SDKs: what one SDK writes, another
//! reads and continues. So where the JS SDK lets JavaScript decide (how `60`
//! prints, how a key becomes a string, which of two keys sorts first), this
//! module decides the same way.

use std::cmp::Ordering;
use std::fmt::Write as _;

use serde_json::Value;

/// Integers up to here are exact in a JS number (2^53).
const MAX_SAFE: f64 = 9_007_199_254_740_992.0;
/// The time range a JS `Date` holds: ±8.64e15 ms.
const MAX_DATE_MS: f64 = 8.64e15;

// ------------------------------------------------------------------ numbers

/// A number as JavaScript prints it (`Number.prototype.toString`, which
/// `JSON.stringify` and string concatenation use): `60`, never `60.0`;
/// `1e+21`; `1e-7`; `0.30000000000000004`.
pub(crate) fn number_text(x: f64) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if x == 0.0 {
        return "0".into();
    }
    if x < 0.0 {
        return format!("-{}", number_text(-x));
    }
    // Rust prints the shortest digits that read back as the same number, as
    // JavaScript does; only where the point and the exponent go differs.
    let e = format!("{x:e}");
    let (mantissa, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i64 = exp.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let k = digits.len() as i64;
    let n = exp + 1;
    let mut out = String::new();
    if k <= n && n <= 21 {
        out.push_str(&digits);
        for _ in 0..(n - k) {
            out.push('0');
        }
    } else if 0 < n && n <= 21 {
        out.push_str(&digits[..n as usize]);
        out.push('.');
        out.push_str(&digits[n as usize..]);
    } else if -6 < n && n <= 0 {
        out.push_str("0.");
        for _ in 0..(-n) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        out.push_str(&digits[..1]);
        if k > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if n > 0 { '+' } else { '-' });
        let _ = write!(out, "{}", (n - 1).abs());
    }
    out
}

/// A JSON number for `x`: an integer when it is one (so it prints as JS
/// prints it), else a float; `null` for NaN and the infinities, as
/// `JSON.stringify` writes them.
pub(crate) fn num(x: f64) -> Value {
    if !x.is_finite() {
        return Value::Null;
    }
    if x.fract() == 0.0 && x.abs() <= MAX_SAFE {
        if x == 0.0 {
            return Value::from(0i64);
        }
        return Value::from(x as i64);
    }
    serde_json::Number::from_f64(x)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// The number a JSON value is, if it is one.
pub(crate) fn as_num(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

/// `Number.isInteger(v)`.
pub(crate) fn is_integer(v: Option<&Value>) -> bool {
    as_num(v).is_some_and(|x| x.is_finite() && x.fract() == 0.0)
}

/// `Number.isFinite(v)`: a number, and finite.
pub(crate) fn finite(v: Option<&Value>) -> Option<f64> {
    as_num(v).filter(|x| x.is_finite())
}

/// An integer field as `i64` (`Number.isInteger` first).
pub(crate) fn int_of(v: Option<&Value>) -> Option<i64> {
    as_num(v)
        .filter(|x| x.is_finite() && x.fract() == 0.0)
        .map(|x| x as i64)
}

// --------------------------------------------------------------------- JSON

/// `JSON.stringify(v)` for a JSON value: no spaces, JS number text. Object
/// keys come in the map's order (serde_json sorts them; key order never
/// matters to a reader of this text, only to a hash, and hashed JSON is
/// written in insertion order by its own writer).
pub(crate) fn stringify(v: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, v);
    out
}

pub(crate) fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let _ = write!(out, "{i}");
            } else if let Some(u) = n.as_u64() {
                let _ = write!(out, "{u}");
            } else {
                match n.as_f64() {
                    Some(f) if f.is_finite() => out.push_str(&number_text(f)),
                    _ => out.push_str("null"),
                }
            }
        }
        Value::String(s) => write_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (k, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(out, k);
                out.push(':');
                write_value(out, item);
            }
            out.push('}');
        }
    }
}

/// `JSON.stringify` of a string: quotes, backslash and the control
/// characters escaped, nothing else (`/`, U+2028 and non-ASCII stay as they
/// are).
pub(crate) fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

pub(crate) fn string_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    write_string(&mut out, s);
    out
}

/// Deep equality of two JSON values, numbers by value (`2` equals `2.0`).
pub(crate) fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => x == y,
        },
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| json_eq(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        _ => a == b,
    }
}

// ---------------------------------------------------------- JS conversions

/// JavaScript truthiness: `false`, `0`, `""`, `null` and missing are false.
pub(crate) fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|x| x != 0.0 && !x.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_)) | Some(Value::Object(_)) => true,
    }
}

/// `String(v)`: a string as it is, a number as JS prints it, `null`,
/// `undefined` (missing), `true`; a list joined with `,` (its null items
/// empty); an object `[object Object]`.
pub(crate) fn string_of(v: Option<&Value>) -> String {
    match v {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i.to_string(),
            (_, Some(u)) => u.to_string(),
            _ => number_text(n.as_f64().unwrap_or(f64::NAN)),
        },
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|x| match x {
                Value::Null => String::new(),
                other => string_of(Some(other)),
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".into(),
    }
}

/// `a < b` on two JS strings: by UTF-16 code units.
pub(crate) fn str_cmp(a: &str, b: &str) -> Ordering {
    if a.is_ascii() && b.is_ascii() {
        return a.cmp(b);
    }
    a.encode_utf16().cmp(b.encode_utf16())
}

/// A JS string's `length` (UTF-16 code units).
pub(crate) fn utf16_len(s: &str) -> usize {
    if s.is_ascii() {
        return s.len();
    }
    s.encode_utf16().count()
}

// -------------------------------------------------------------------- dates

/// Days from 1970-01-01 to the civil date (proleptic Gregorian).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `new Date(ms).toISOString()`: `2026-11-06T09:00:00.000Z` (a fraction of a
/// millisecond is cut, as a `Date` does). `None` past the range a `Date`
/// holds, where JavaScript throws.
pub(crate) fn try_iso(ms: f64) -> Option<String> {
    if !ms.is_finite() || ms.abs() > MAX_DATE_MS {
        return None;
    }
    let t = ms.trunc() as i64;
    let days = t.div_euclid(86_400_000);
    let rem = t.rem_euclid(86_400_000);
    let (y, mo, d) = civil_from_days(days);
    let (h, mi, s, milli) = (
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000,
    );
    let year = if (0..=9999).contains(&y) {
        format!("{y:04}")
    } else if y < 0 {
        format!("-{:06}", -y)
    } else {
        format!("+{y:06}")
    };
    Some(format!(
        "{year}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z"
    ))
}

/// [`try_iso`], or the text of the number for a time out of range.
pub(crate) fn iso(ms: f64) -> String {
    try_iso(ms).unwrap_or_else(|| "Invalid Date".into())
}

/// `Date.parse(text)` for the forms a feed meets: ISO 8601 (`2026-11-06`,
/// `2026-11-06T09:00:00Z`, any number of fraction digits, `+01:00` offsets,
/// a space for the `T`) and the `toUTCString` form
/// (`Fri, 06 Nov 2026 09:00:00 GMT`). A date-time without an offset is read
/// as UTC (JavaScript reads it in the process's time zone). `None`: not a
/// time.
pub(crate) fn parse_date(text: &str) -> Option<f64> {
    let s = text.trim();
    parse_iso(s).or_else(|| parse_utc_string(s))
}

fn take_digits(s: &[u8], i: &mut usize, n: usize) -> Option<i64> {
    if *i + n > s.len() {
        return None;
    }
    let mut v = 0i64;
    for k in 0..n {
        let c = s[*i + k];
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + i64::from(c - b'0');
    }
    *i += n;
    Some(v)
}

fn parse_iso(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    let mut i = 0;
    let year = match b.first()? {
        b'+' | b'-' => {
            let neg = b[0] == b'-';
            i = 1;
            let y = take_digits(b, &mut i, 6)?;
            if neg && y == 0 {
                return None;
            }
            if neg {
                -y
            } else {
                y
            }
        }
        _ => take_digits(b, &mut i, 4)?,
    };
    let (mut month, mut day) = (1, 1);
    if i < b.len() && b[i] == b'-' {
        i += 1;
        month = take_digits(b, &mut i, 2)?;
        if i < b.len() && b[i] == b'-' {
            i += 1;
            day = take_digits(b, &mut i, 2)?;
        }
    }
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (mut h, mut mi, mut sec, mut ms) = (0i64, 0i64, 0i64, 0f64);
    let mut offset_min = 0i64;
    if i < b.len() {
        if !matches!(b[i], b'T' | b't' | b' ') {
            return None;
        }
        i += 1;
        h = take_digits(b, &mut i, 2)?;
        if i >= b.len() || b[i] != b':' {
            return None;
        }
        i += 1;
        mi = take_digits(b, &mut i, 2)?;
        if i < b.len() && b[i] == b':' {
            i += 1;
            sec = take_digits(b, &mut i, 2)?;
            if i < b.len() && b[i] == b'.' {
                i += 1;
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                if i == start {
                    return None;
                }
                // Milliseconds from the first three digits; the rest is cut.
                let frac = &s[start..i];
                let mut padded = frac.chars().take(3).collect::<String>();
                while padded.len() < 3 {
                    padded.push('0');
                }
                ms = padded.parse::<f64>().ok()?;
            }
        }
        if h > 24 || mi > 59 || sec > 59 || (h == 24 && (mi > 0 || sec > 0 || ms > 0.0)) {
            return None;
        }
        if i < b.len() {
            match b[i] {
                b'Z' | b'z' => i += 1,
                b'+' | b'-' => {
                    let sign = if b[i] == b'-' { -1 } else { 1 };
                    i += 1;
                    let oh = take_digits(b, &mut i, 2)?;
                    if i < b.len() && b[i] == b':' {
                        i += 1;
                    }
                    let om = take_digits(b, &mut i, 2)?;
                    if oh > 23 || om > 59 {
                        return None;
                    }
                    offset_min = sign * (oh * 60 + om);
                }
                _ => return None,
            }
        }
        if i != b.len() {
            return None;
        }
    }
    let days = days_from_civil(year, month, day);
    let t = (days * 86_400 + h * 3600 + mi * 60 + sec - offset_min * 60) as f64 * 1000.0 + ms;
    (t.abs() <= MAX_DATE_MS).then_some(t)
}

fn parse_utc_string(s: &str) -> Option<f64> {
    // `Fri, 06 Nov 2026 09:00:00 GMT` (the weekday optional).
    let rest = match s.split_once(',') {
        Some((_, r)) => r.trim(),
        None => s,
    };
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() < 4 {
        return None;
    }
    let day: i64 = parts[0].parse().ok()?;
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let mon = parts[1].get(..3)?.to_ascii_lowercase();
    let month = MONTHS.iter().position(|m| *m == mon)? as i64 + 1;
    let year: i64 = parts[2].parse().ok()?;
    let hms: Vec<&str> = parts[3].split(':').collect();
    if hms.len() < 2 {
        return None;
    }
    let h: i64 = hms[0].parse().ok()?;
    let mi: i64 = hms[1].parse().ok()?;
    let sec: i64 = hms.get(2).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
    if let Some(zone) = parts.get(4) {
        if !matches!(*zone, "GMT" | "UTC" | "Z" | "+0000") {
            return None;
        }
    }
    if !(1..=31).contains(&day) || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + h * 3600 + mi * 60 + sec) as f64 * 1000.0)
}

/// Milliseconds since the epoch, now.
pub(crate) fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

// ---------------------------------------------------------------- durations

/// `parseDurationMs` (kv/expiry.js): one or more `<number><unit>` parts,
/// `'250ms'`, `'30s'`, `'1h30m'`, units ms s m h d; milliseconds, above zero.
pub(crate) fn parse_duration_ms(value: &str) -> Result<f64, String> {
    let t = value.trim().to_lowercase();
    if t.is_empty() {
        return Err(format!(
            "duration must be a string with a unit, e.g. '250ms', '30s', '15m', '24h', '7d' — got {}",
            string_json(value)
        ));
    }
    let b = t.as_bytes();
    let mut i = 0;
    let mut ms = 0f64;
    while i < b.len() {
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return Err(format!(
                "invalid duration '{value}': expected <number><unit> parts, e.g. '30s' or '1h30m'"
            ));
        }
        if i < b.len() && b[i] == b'.' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            i += 1;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
        }
        let number: f64 = t[start..i].parse().map_err(|_| {
            format!(
                "invalid duration '{value}': expected <number><unit> parts, e.g. '30s' or '1h30m'"
            )
        })?;
        let ustart = i;
        while i < b.len() && b[i].is_ascii_lowercase() {
            i += 1;
        }
        if i == ustart {
            return Err(format!(
                "invalid duration '{value}': expected <number><unit> parts, e.g. '30s' or '1h30m'"
            ));
        }
        let unit = match &t[ustart..i] {
            "ms" => 1.0,
            "s" => 1000.0,
            "m" => 60_000.0,
            "h" => 3_600_000.0,
            "d" => 86_400_000.0,
            other => {
                return Err(format!(
                    "invalid duration '{value}': unknown unit in '{}{other}' (use ms, s, m, h, d)",
                    &t[start..ustart]
                ))
            }
        };
        ms += number * unit;
    }
    if ms <= 0.0 || ms.is_nan() {
        return Err(format!(
            "invalid duration '{value}': must be greater than zero"
        ));
    }
    Ok(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_print_as_javascript_prints_them() {
        // Each pair checked against node: String(x).
        let cases: &[(f64, &str)] = &[
            (60.0, "60"),
            (0.5, "0.5"),
            (-0.0, "0"),
            (1e21, "1e+21"),
            (1e20, "100000000000000000000"),
            (123.456, "123.456"),
            (0.000001, "0.000001"),
            (1e-7, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (0.1 + 0.2, "0.30000000000000004"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (1793955600000.0, "1793955600000"),
            (100.0 / 3.0, "33.333333333333336"),
            (1.23e-18, "1.23e-18"),
            (9007199254740994.0, "9007199254740994"),
            (-2.5, "-2.5"),
            (12345678.9, "12345678.9"),
        ];
        for (x, want) in cases {
            assert_eq!(number_text(*x), *want, "{x}");
        }
    }

    #[test]
    fn json_text_is_javascripts() {
        assert_eq!(
            stringify(&json!({"a": [1, 2.5, null], "b": "x\u{1f}\"y"})),
            r#"{"a":[1,2.5,null],"b":"x\u001f\"y"}"#
        );
        assert_eq!(stringify(&num(60.0)), "60");
        assert_eq!(stringify(&json!(60.0)), "60");
        assert_eq!(stringify(&num(f64::NAN)), "null");
    }

    #[test]
    fn string_of_is_javascripts_string() {
        assert_eq!(string_of(None), "undefined");
        assert_eq!(string_of(Some(&json!(null))), "null");
        assert_eq!(string_of(Some(&json!(3.0))), "3");
        assert_eq!(string_of(Some(&json!([1, null, [2, 3]]))), "1,,2,3");
        assert_eq!(string_of(Some(&json!({"a": 1}))), "[object Object]");
        assert_eq!(string_of(Some(&json!(true))), "true");
    }

    #[test]
    fn iso_times_round_trip() {
        let t = parse_date("2026-11-06T09:00:00.000Z").unwrap();
        assert_eq!(t, 1793955600000.0);
        assert_eq!(iso(t), "2026-11-06T09:00:00.000Z");
        assert_eq!(parse_date("2026-11-06T09:00:00.000000Z"), Some(t));
        assert_eq!(parse_date("2026-11-06T10:00:00+01:00"), Some(t));
        assert_eq!(parse_date("2026-11-06"), Some(t - 9.0 * 3_600_000.0));
        assert_eq!(parse_date("Fri, 06 Nov 2026 09:00:00 GMT"), Some(t));
        assert_eq!(parse_date("2026-11-06T09:00:00.1239Z"), Some(t + 123.0));
        assert_eq!(parse_date("2026-11-06t09:00z"), Some(t));
        assert_eq!(parse_date("2026-11-06T10:00:00+0100"), Some(t));
        assert_eq!(parse_date("2026-02-30"), Some(1772409600000.0));
        assert_eq!(parse_date("2026-11-06T24:00:00Z"), Some(1794009600000.0));
        assert_eq!(parse_date("2026-11-06T09:00:00,5Z"), None);
        assert_eq!(iso(-62198755200000.0), "-000001-01-01T00:00:00.000Z");
        assert_eq!(parse_date("yesterday"), None);
        assert_eq!(iso(-1.0), "1969-12-31T23:59:59.999Z");
        assert_eq!(iso(1.9), "1970-01-01T00:00:00.001Z");
        assert_eq!(iso(253402300800000.0), "+010000-01-01T00:00:00.000Z");
        assert_eq!(try_iso(9e15), None);
    }

    #[test]
    fn durations_parse_like_parse_duration_ms() {
        assert_eq!(parse_duration_ms("15m").unwrap(), 900_000.0);
        assert_eq!(parse_duration_ms("1h30m").unwrap(), 5_400_000.0);
        assert_eq!(parse_duration_ms(" 250MS ").unwrap(), 250.0);
        assert_eq!(parse_duration_ms("1.5s").unwrap(), 1500.0);
        assert!(parse_duration_ms("1hour").is_err());
        assert!(parse_duration_ms("0s").is_err());
        assert!(parse_duration_ms("").is_err());
        assert!(parse_duration_ms("5").is_err());
    }

    #[test]
    fn strings_sort_by_utf16_units() {
        assert_eq!(str_cmp("a", "b"), Ordering::Less);
        // U+FF5E sorts after U+1F600 in UTF-8, before it in UTF-16.
        assert_eq!(str_cmp("\u{1F600}", "\u{FF5E}"), Ordering::Less);
        assert_eq!(utf16_len("é\u{1F600}"), 3);
    }

    #[test]
    fn truthiness() {
        assert!(!truthy(None));
        assert!(!truthy(Some(&json!(0))));
        assert!(!truthy(Some(&json!(""))));
        assert!(truthy(Some(&json!([]))));
        assert!(truthy(Some(&json!("0"))));
    }
}
