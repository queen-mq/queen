//! PostgreSQL text output → JSON, by type OID (PLAN §4.6 "Values"). OWNER: agent R.
//!
//! One converter for both the stream (pgoutput text datums) and the snapshot
//! (`column::text` in a SELECT), so an `r` event and a `c` event for the same
//! row are byte-identical. Output is JSON TEXT appended to a `String`, never a
//! `serde_json::Value`: a bigint or a numeric stays the exact digits the
//! server printed.
//!
//! Both connections run with `TimeZone=UTC`, `DateStyle=ISO`,
//! `IntervalStyle=postgres`, `extra_float_digits=3`, `bytea_output=hex`.
//!
//! Two promises hold for EVERY input, including text no server would print:
//! the output is one valid JSON value, and nothing panics. A value that does
//! not have the shape its type promises (a number that is not a JSON number,
//! an array literal that does not parse, json that is not JSON) becomes a JSON
//! string of the text instead.
//!
//! The snapshot side must feed the type's OUTPUT FUNCTION text, which is what
//! pgoutput sends. `column::text` is not always that (tests/values_live.rs
//! pins the list on a real server): `bpchar::text` strips the padding,
//! `inet::text` adds `/32`, `xml::text` keeps an XML declaration the output
//! function drops, `bool::text` says `true` (the last is absorbed here; the
//! others cannot be). The simple query protocol returns output-function
//! text; so does `format('%s', column)`, which turns NULL into '' though.
//!
//! Numbers are copied as printed: a numeric beyond ±1e308 is valid JSON, but
//! a parser that maps numbers to f64 (`serde_json::Value`, JavaScript) cannot
//! hold it exactly or at all.

use std::borrow::Cow;
use std::fmt::Write as _;

/// Type OIDs this module treats specially (pg_type.dat; stable across
/// versions).
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const CHAR: u32 = 18;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const XML: u32 = 142;
    pub const BOX: u32 = 603;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
    pub const TIMETZ: u32 = 1266;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// Append the JSON rendering of one non-null column value (`text` = the
/// type's text output) to `out`.
pub fn append_json(type_oid: u32, text: &str, out: &mut String) {
    append_json_with_element(type_oid, None, text, out)
}

/// [`append_json`] for a type whose array-ness only the catalog knows:
/// `element_oid` is the element type of an array type that is not built in
/// (`enum[]`, `composite[]`, `domain[]`: `pg_type.typelem` where
/// `typelem <> 0 AND typlen = -1`). `None` falls back to the builtin table
/// ([`array_element`]). Elements of a type this module does not know become
/// strings; `box` elements are split on `;` (its `typdelim`), every other
/// type's on `,`.
///
/// `int2vector` and `oidvector` pass that catalog test but print `1 2 3`, not
/// an array literal: they come out as one string, which is what any text
/// without the array shape does.
pub fn append_json_with_element(
    type_oid: u32,
    element_oid: Option<u32>,
    text: &str,
    out: &mut String,
) {
    match element_oid
        .filter(|&e| e != 0)
        .or_else(|| array_element(type_oid))
    {
        Some(element) => append_array(element, text, out),
        None => append_scalar(type_oid, text, out),
    }
}

/// Append a JSON string literal (quotes and escapes) to `out`.
pub fn append_json_string(s: &str, out: &mut String) {
    out.reserve(s.len() + 2);
    out.push('"');
    let mut start = 0;
    for (i, &b) in s.as_bytes().iter().enumerate() {
        let esc = match b {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0..=0x1f => "",
            _ => continue,
        };
        // Every byte matched above is ASCII, so `i` is a char boundary.
        out.push_str(&s[start..i]);
        if esc.is_empty() {
            let _ = write!(out, "\\u{:04x}", b);
        } else {
            out.push_str(esc);
        }
        start = i + 1;
    }
    out.push_str(&s[start..]);
    out.push('"');
}

/// The element type of a builtin array type, `None` when `type_oid` is not a
/// builtin array. Every array type PostgreSQL 17/18 ships (pg_type rows with
/// `typlen = -1`, `typelem <> 0`, `typsubscript = array_subscript_handler`),
/// minus `int2vector` and `oidvector`, which print as space-separated lists.
pub fn array_element(type_oid: u32) -> Option<u32> {
    Some(match type_oid {
        143 => 142,   // xml
        199 => 114,   // json
        210 => 71,    // pg_type (row type)
        270 => 75,    // pg_attribute
        271 => 5069,  // xid8
        272 => 81,    // pg_proc
        273 => 83,    // pg_class
        629 => 628,   // line
        651 => 650,   // cidr
        719 => 718,   // circle
        775 => 774,   // macaddr8
        791 => 790,   // money
        1000 => 16,   // bool
        1001 => 17,   // bytea
        1002 => 18,   // "char"
        1003 => 19,   // name
        1005 => 21,   // int2
        1006 => 22,   // int2vector
        1007 => 23,   // int4
        1008 => 24,   // regproc
        1009 => 25,   // text
        1010 => 27,   // tid
        1011 => 28,   // xid
        1012 => 29,   // cid
        1013 => 30,   // oidvector
        1014 => 1042, // bpchar
        1015 => 1043, // varchar
        1016 => 20,   // int8
        1017 => 600,  // point
        1018 => 601,  // lseg
        1019 => 602,  // path
        1020 => 603,  // box (delimiter ';')
        1021 => 700,  // float4
        1022 => 701,  // float8
        1027 => 604,  // polygon
        1028 => 26,   // oid
        1034 => 1033, // aclitem
        1040 => 829,  // macaddr
        1041 => 869,  // inet
        1115 => 1114, // timestamp
        1182 => 1082, // date
        1183 => 1083, // time
        1185 => 1184, // timestamptz
        1187 => 1186, // interval
        1231 => 1700, // numeric
        1263 => 2275, // cstring
        1270 => 1266, // timetz
        1561 => 1560, // bit
        1563 => 1562, // varbit
        2201 => 1790, // refcursor
        2207 => 2202, // regprocedure
        2208 => 2203, // regoper
        2209 => 2204, // regoperator
        2210 => 2205, // regclass
        2211 => 2206, // regtype
        2287 => 2249, // record
        2949 => 2970, // txid_snapshot
        2951 => 2950, // uuid
        3221 => 3220, // pg_lsn
        3643 => 3614, // tsvector
        3644 => 3642, // gtsvector
        3645 => 3615, // tsquery
        3735 => 3734, // regconfig
        3770 => 3769, // regdictionary
        3807 => 3802, // jsonb
        3905 => 3904, // int4range
        3907 => 3906, // numrange
        3909 => 3908, // tsrange
        3911 => 3910, // tstzrange
        3913 => 3912, // daterange
        3927 => 3926, // int8range
        4073 => 4072, // jsonpath
        4090 => 4089, // regnamespace
        4097 => 4096, // regrole
        4192 => 4191, // regcollation
        5039 => 5038, // pg_snapshot
        6150 => 4451, // int4multirange
        6151 => 4532, // nummultirange
        6152 => 4533, // tsmultirange
        6153 => 4534, // tstzmultirange
        6155 => 4535, // datemultirange
        6157 => 4536, // int8multirange
        _ => return None,
    })
}

/// Unix microseconds → `2026-10-02T10:00:00.123456Z`.
pub fn iso_utc_micros(unix_us: i64) -> String {
    let secs = unix_us.div_euclid(1_000_000);
    let micros = unix_us.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let mut s = String::with_capacity(27);
    // ISO 8601 needs a sign beyond four digits; i64 microseconds reach
    // about ±292 000 years.
    if (0..=9999).contains(&y) {
        let _ = write!(s, "{y:04}");
    } else if y < 0 {
        let _ = write!(s, "-{:06}", -y);
    } else {
        let _ = write!(s, "+{y:06}");
    }
    let _ = write!(
        s,
        "-{m:02}-{d:02}T{:02}:{:02}:{:02}.{micros:06}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    );
    s
}

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian
/// (H. Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn append_scalar(type_oid: u32, text: &str, out: &mut String) {
    match type_oid {
        // `t`/`f` is the output function; `true`/`false` is `bool::text`.
        oid::BOOL => match text {
            "t" | "true" => out.push_str("true"),
            "f" | "false" => out.push_str("false"),
            _ => append_json_string(text, out),
        },
        oid::INT2 | oid::INT4 | oid::INT8 | oid::OID | oid::FLOAT4 | oid::FLOAT8 | oid::NUMERIC => {
            // NaN, Infinity and -Infinity are not JSON numbers: strings.
            if is_json_number(text) {
                out.push_str(text)
            } else {
                append_json_string(text, out)
            }
        }
        oid::JSON | oid::JSONB => append_raw_json(text, out),
        oid::TIMESTAMPTZ => append_timestamp(text, true, out),
        oid::TIMESTAMP => append_timestamp(text, false, out),
        _ => append_json_string(text, out),
    }
}

/// RFC 8259 `number`: `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
fn is_json_number(s: &str) -> bool {
    let b = s.as_bytes();
    let digits = |mut i: usize| {
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        i
    };
    let mut i = usize::from(b.first() == Some(&b'-'));
    match b.get(i) {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => i = digits(i),
        _ => return false,
    }
    if b.get(i) == Some(&b'.') {
        let j = digits(i + 1);
        if j == i + 1 {
            return false;
        }
        i = j;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let j = digits(i);
        if j == i {
            return false;
        }
        i = j;
    }
    i == b.len()
}

/// json/jsonb: embedded as is. The server validated it on input, but the
/// promise of this module is valid output for ANY text, so it is checked
/// (one linear scan, no allocation, no depth limit); leading and trailing
/// whitespace a `json` value kept from its input is dropped.
fn append_raw_json(text: &str, out: &mut String) {
    match serde_json::from_str::<&serde_json::value::RawValue>(text) {
        Ok(raw) => out.push_str(raw.get()),
        Err(_) => append_json_string(text, out),
    }
}

/// ISO style (`2026-10-02 10:00:00.123456+00`) → `"2026-10-02T10:00:00.123456+00:00"`;
/// `timestamp` has no zone. Anything else — `infinity`, `-infinity`, a BC
/// date (` BC` suffix), another DateStyle — is copied as a string.
fn append_timestamp(text: &str, with_zone: bool, out: &mut String) {
    match iso_shape(text.as_bytes(), with_zone) {
        Some((space, zone)) => {
            out.reserve(text.len() + 6);
            out.push('"');
            out.push_str(&text[..space]);
            out.push('T');
            out.push_str(&text[space + 1..]);
            // `+00` → `+00:00`; `+05:30` and `+05:30:15` stay.
            if zone.is_some_and(|z| text.len() - z == 3) {
                out.push_str(":00");
            }
            out.push('"');
        }
        None => append_json_string(text, out),
    }
}

/// `Y{4,}-MM-DD HH:MM:SS(.f+)?` then, with a zone, `[+-]HH(:MM(:SS)?)?`, and
/// nothing after. Returns the offset of the space and of the zone sign.
fn iso_shape(b: &[u8], with_zone: bool) -> Option<(usize, Option<usize>)> {
    let digits = |from: usize| {
        let mut i = from;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        i - from
    };
    let exact = |at: usize, n: usize| (digits(at) == n).then_some(at + n);
    let lit = |at: usize, c: u8| (b.get(at) == Some(&c)).then_some(at + 1);
    let year = digits(0);
    if year < 4 {
        return None;
    }
    let mut i = lit(year, b'-')?;
    i = lit(exact(i, 2)?, b'-')?;
    i = exact(i, 2)?;
    let space = i;
    i = lit(i, b' ')?;
    i = lit(exact(i, 2)?, b':')?;
    i = lit(exact(i, 2)?, b':')?;
    i = exact(i, 2)?;
    if b.get(i) == Some(&b'.') {
        let n = digits(i + 1);
        if n == 0 {
            return None;
        }
        i += 1 + n;
    }
    let zone = if with_zone {
        let z = i;
        if !matches!(b.get(i), Some(b'+' | b'-')) {
            return None;
        }
        i = exact(i + 1, 2)?;
        for _ in 0..2 {
            match lit(i, b':') {
                Some(j) => i = exact(j, 2)?,
                None => break,
            }
        }
        Some(z)
    } else {
        None
    };
    (i == b.len()).then_some((space, zone))
}

/// An array literal (`{1,2,NULL,"a\"b"}`, nested `{{1,2},{3,4}}`, `{}`, with
/// or without a `[0:1]=` bounds decoration, which is dropped) → a JSON array,
/// elements converted by `element`; anything that does not parse → a string.
fn append_array(element: u32, text: &str, out: &mut String) {
    let start = out.len();
    let delim = if element == oid::BOX { b';' } else { b',' };
    let mut p = ArrayParser {
        s: text,
        i: 0,
        delim,
        element,
    };
    if p.parse(out).is_none() {
        out.truncate(start);
        append_json_string(text, out);
    }
}

struct ArrayParser<'a> {
    s: &'a str,
    i: usize,
    delim: u8,
    element: u32,
}

/// Six is PostgreSQL's MAXDIM; deeper text is not an array the server
/// printed, and the bound keeps hostile input off the stack.
const MAX_DEPTH: usize = 6;

impl<'a> ArrayParser<'a> {
    fn peek(&self) -> Option<u8> {
        self.s.as_bytes().get(self.i).copied()
    }

    fn eat(&mut self, c: u8) -> Option<()> {
        (self.peek() == Some(c)).then(|| self.i += 1)
    }

    /// array_in's `scanner_isspace`.
    fn skip_space(&mut self) {
        while matches!(
            self.peek(),
            Some(b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
        ) {
            self.i += 1;
        }
    }

    fn bound(&mut self) -> Option<()> {
        let _ = self.eat(b'-');
        let from = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        (self.i > from).then_some(())
    }

    fn parse(&mut self, out: &mut String) -> Option<()> {
        self.skip_space();
        if self.peek() == Some(b'[') {
            while self.eat(b'[').is_some() {
                self.bound()?;
                self.eat(b':')?;
                self.bound()?;
                self.eat(b']')?;
            }
            self.eat(b'=')?;
            self.skip_space();
        }
        self.level(out, 1)?;
        self.skip_space();
        (self.i == self.s.len()).then_some(())
    }

    fn level(&mut self, out: &mut String, depth: usize) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.eat(b'{')?;
        out.push('[');
        self.skip_space();
        if self.eat(b'}').is_some() {
            out.push(']');
            return Some(());
        }
        loop {
            self.skip_space();
            match self.peek()? {
                b'{' => self.level(out, depth + 1)?,
                b'"' => {
                    let v = self.quoted()?;
                    append_scalar(self.element, &v, out);
                }
                _ => {
                    let v = self.unquoted()?;
                    if v.eq_ignore_ascii_case("NULL") {
                        out.push_str("null");
                    } else {
                        append_scalar(self.element, &v, out);
                    }
                }
            }
            self.skip_space();
            match self.peek()? {
                b'}' => {
                    self.i += 1;
                    out.push(']');
                    return Some(());
                }
                c if c == self.delim => {
                    self.i += 1;
                    out.push(',');
                }
                _ => return None,
            }
        }
    }

    /// `"…"` with `\x` → `x`. Only ASCII backslashes are removed, so the
    /// bytes stay UTF-8.
    fn quoted(&mut self) -> Option<Cow<'a, str>> {
        self.eat(b'"')?;
        let s = self.s;
        let b = s.as_bytes();
        let from = self.i;
        let mut owned: Option<Vec<u8>> = None;
        loop {
            let c = *b.get(self.i)?;
            match c {
                b'"' => {
                    let end = self.i;
                    self.i += 1;
                    return match owned {
                        None => Some(Cow::Borrowed(&s[from..end])),
                        Some(v) => String::from_utf8(v).ok().map(Cow::Owned),
                    };
                }
                b'\\' => {
                    let v = owned.get_or_insert_with(|| b[from..self.i].to_vec());
                    v.push(*b.get(self.i + 1)?);
                    self.i += 2;
                }
                _ => {
                    if let Some(v) = owned.as_mut() {
                        v.push(c);
                    }
                    self.i += 1;
                }
            }
        }
    }

    /// Up to the delimiter or `}`, trailing white space dropped, `\x` → `x`
    /// (array_out quotes anything with a backslash; array_in accepts both).
    fn unquoted(&mut self) -> Option<Cow<'a, str>> {
        let s = self.s;
        let b = s.as_bytes();
        let from = self.i;
        let mut owned: Option<Vec<u8>> = None;
        let mut end = self.i;
        while let Some(&c) = b.get(self.i) {
            match c {
                b'}' | b'{' | b'"' => break,
                c if c == self.delim => break,
                b'\\' => {
                    let v = owned.get_or_insert_with(|| b[from..self.i].to_vec());
                    v.push(*b.get(self.i + 1)?);
                    self.i += 2;
                    end = self.i;
                }
                _ => {
                    if let Some(v) = owned.as_mut() {
                        v.push(c);
                    }
                    self.i += 1;
                    if !matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c) {
                        end = self.i;
                    }
                }
            }
        }
        if end == from {
            return None;
        }
        match owned {
            None => Some(Cow::Borrowed(&s[from..end])),
            Some(mut v) => {
                // Drop the trailing white space that was copied.
                let keep = v.len() - (self.i - end);
                v.truncate(keep);
                String::from_utf8(v).ok().map(Cow::Owned)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn j(type_oid: u32, text: &str) -> String {
        let mut s = String::new();
        append_json(type_oid, text, &mut s);
        // Valid JSON, always (RawValue: syntax only, any number size).
        serde_json::from_str::<&serde_json::value::RawValue>(&s)
            .unwrap_or_else(|e| panic!("{type_oid} {text:?} -> {s:?}: {e}"));
        s
    }

    fn je(type_oid: u32, element: u32, text: &str) -> String {
        let mut s = String::new();
        append_json_with_element(type_oid, Some(element), text, &mut s);
        serde_json::from_str::<&serde_json::value::RawValue>(&s)
            .unwrap_or_else(|e| panic!("{type_oid}/{element} {text:?} -> {s:?}: {e}"));
        s
    }

    #[test]
    fn bool_and_numbers() {
        assert_eq!(j(oid::BOOL, "t"), "true");
        assert_eq!(j(oid::BOOL, "f"), "false");
        assert_eq!(j(oid::BOOL, "true"), "true");
        assert_eq!(j(oid::BOOL, "false"), "false");
        assert_eq!(j(oid::BOOL, "maybe"), "\"maybe\"");
        assert_eq!(j(oid::INT2, "-32768"), "-32768");
        assert_eq!(j(oid::INT4, "0"), "0");
        assert_eq!(j(oid::INT8, "-9223372036854775808"), "-9223372036854775808");
        assert_eq!(j(oid::INT8, "9223372036854775807"), "9223372036854775807");
        assert_eq!(j(oid::OID, "4294967295"), "4294967295");
        assert_eq!(j(oid::FLOAT8, "1.5"), "1.5");
        assert_eq!(j(oid::FLOAT8, "-0"), "-0");
        assert_eq!(j(oid::FLOAT8, "1e+100"), "1e+100");
        assert_eq!(
            j(oid::FLOAT8, "1.2345678901234567e-300"),
            "1.2345678901234567e-300"
        );
        assert_eq!(j(oid::FLOAT4, "3.4028235e+38"), "3.4028235e+38");
        for special in ["NaN", "Infinity", "-Infinity"] {
            for t in [oid::FLOAT4, oid::FLOAT8, oid::NUMERIC] {
                assert_eq!(j(t, special), format!("\"{special}\""));
            }
        }
        assert_eq!(j(oid::NUMERIC, "123.4500"), "123.4500");
        assert_eq!(j(oid::NUMERIC, "-0.000001"), "-0.000001");
        let huge = format!("{}.{}", "9".repeat(1000), "1".repeat(50));
        assert_eq!(j(oid::NUMERIC, &huge), huge);
        // Not JSON numbers, whatever the type says: strings.
        for bad in [
            "", "-", "01", "1.", ".5", "+1", "1e", "1e+", " 1", "1 ", "0x10", "1_000",
        ] {
            assert_eq!(j(oid::NUMERIC, bad), format!("\"{bad}\""), "{bad:?}");
            assert!(!is_json_number(bad), "{bad:?}");
        }
        for good in ["0", "-0", "0.0", "10", "1e5", "1E-5", "-1.25e+10", "0e0"] {
            assert!(is_json_number(good), "{good:?}");
        }
    }

    #[test]
    fn json_is_embedded_and_checked() {
        assert_eq!(
            j(oid::JSONB, r#"{"a": [1, 2.5, null, "x\"y"]}"#),
            r#"{"a": [1, 2.5, null, "x\"y"]}"#
        );
        assert_eq!(j(oid::JSON, "  [1,2]  \n"), "[1,2]");
        assert_eq!(j(oid::JSON, "\"s\""), "\"s\"");
        assert_eq!(j(oid::JSON, "1e400"), "1e400");
        assert_eq!(j(oid::JSON, "{bad"), "\"{bad\"");
        assert_eq!(j(oid::JSON, ""), "\"\"");
        let deep = format!("{}{}", "[".repeat(5000), "]".repeat(5000));
        assert_eq!(j(oid::JSONB, &deep), deep);
    }

    #[test]
    fn timestamps() {
        let c = |t: &str| j(oid::TIMESTAMPTZ, t);
        assert_eq!(
            c("2026-10-02 10:00:00.123456+00"),
            "\"2026-10-02T10:00:00.123456+00:00\""
        );
        assert_eq!(c("2026-10-02 10:00:00+00"), "\"2026-10-02T10:00:00+00:00\"");
        assert_eq!(
            c("2026-10-02 10:00:00.5-03"),
            "\"2026-10-02T10:00:00.5-03:00\""
        );
        assert_eq!(
            c("2026-10-02 15:30:00+05:30"),
            "\"2026-10-02T15:30:00+05:30\""
        );
        assert_eq!(
            c("1890-01-01 00:00:00+00:09:21"),
            "\"1890-01-01T00:00:00+00:09:21\""
        );
        assert_eq!(
            c("12026-10-02 10:00:00+00"),
            "\"12026-10-02T10:00:00+00:00\""
        );
        assert_eq!(c("infinity"), "\"infinity\"");
        assert_eq!(c("-infinity"), "\"-infinity\"");
        assert_eq!(
            c("0044-03-15 12:00:00+00 BC"),
            "\"0044-03-15 12:00:00+00 BC\""
        );
        // Not ISO: copied.
        for odd in [
            "Fri Oct 02 10:00:00 2026 UTC",
            "2026-10-02 10:00:00",
            "2026-10-02 10:00:00.+00",
            "2026-10-02 10:00:00+0",
            "2026-10-02 10:00:00+00:0",
        ] {
            assert_eq!(c(odd), serde_json::to_string(odd).unwrap(), "{odd:?}");
        }
        let t = |t: &str| j(oid::TIMESTAMP, t);
        assert_eq!(t("2026-10-02 10:00:00.123"), "\"2026-10-02T10:00:00.123\"");
        assert_eq!(t("2026-10-02 10:00:00"), "\"2026-10-02T10:00:00\"");
        assert_eq!(t("2026-10-02 10:00:00+00"), "\"2026-10-02 10:00:00+00\"");
        assert_eq!(t("0001-01-01 00:00:00 BC"), "\"0001-01-01 00:00:00 BC\"");
        assert_eq!(t("infinity"), "\"infinity\"");
    }

    #[test]
    fn everything_else_is_a_string() {
        let cases = [
            (oid::BYTEA, "\\x0a0b", "\"\\\\x0a0b\""),
            (oid::TEXT, "héllo ✓ 𝄞", "\"héllo ✓ 𝄞\""),
            (
                oid::TEXT,
                "a\"b\\c\nd\te\r\u{1}\u{1f}\u{7f}",
                "\"a\\\"b\\\\c\\nd\\te\\r\\u0001\\u001f\u{7f}\"",
            ),
            (oid::VARCHAR, "", "\"\""),
            (oid::BPCHAR, "ab   ", "\"ab   \""),
            (oid::NAME, "orders", "\"orders\""),
            (oid::CHAR, "\\377", "\"\\\\377\""),
            (oid::DATE, "2026-10-02", "\"2026-10-02\""),
            (oid::TIME, "10:00:00.5", "\"10:00:00.5\""),
            (oid::TIMETZ, "10:00:00+02", "\"10:00:00+02\""),
            (
                oid::INTERVAL,
                "1 year 2 mons -3 days 04:05:06",
                "\"1 year 2 mons -3 days 04:05:06\"",
            ),
            (
                oid::UUID,
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\"",
            ),
            (869, "192.168.1.5", "\"192.168.1.5\""),
            (650, "10.0.0.0/8", "\"10.0.0.0/8\""),
            (829, "08:00:2b:01:02:03", "\"08:00:2b:01:02:03\""),
            (790, "-$1,234.56", "\"-$1,234.56\""),
            (1560, "1010", "\"1010\""),
            (oid::XML, "<a x=\"1\">t</a>", "\"<a x=\\\"1\\\">t</a>\""),
            (3614, "'a':1 'b':2", "\"'a':1 'b':2\""),
            (22, "1 2 3", "\"1 2 3\""),
            (16_385, "happy", "\"happy\""),
            (16_390, "(1,\"x y\")", "\"(1,\\\"x y\\\")\""),
        ];
        for (t, text, want) in cases {
            assert_eq!(j(t, text), want, "{t} {text:?}");
        }
    }

    #[test]
    fn arrays() {
        assert_eq!(j(1007, "{1,2,NULL,3}"), "[1,2,null,3]");
        assert_eq!(j(1007, "{}"), "[]");
        assert_eq!(j(1007, "{{1,2},{3,4}}"), "[[1,2],[3,4]]");
        assert_eq!(j(1007, "{{{1},{2}},{{3},{4}}}"), "[[[1],[2]],[[3],[4]]]");
        assert_eq!(j(1007, "[0:1]={1,2}"), "[1,2]");
        assert_eq!(j(1007, "[-2:-1][1:2]={{1,2},{3,4}}"), "[[1,2],[3,4]]");
        assert_eq!(j(1000, "{t,f,NULL}"), "[true,false,null]");
        assert_eq!(
            j(1009, r#"{"a\"b","NULL",NULL,"",x,"c\\d","{}","a,b"}"#),
            r#"["a\"b","NULL",null,"","x","c\\d","{}","a,b"]"#
        );
        assert_eq!(j(1009, "{héllo,\"✓ 𝄞\"}"), "[\"héllo\",\"✓ 𝄞\"]");
        assert_eq!(
            j(1022, "{NaN,Infinity,-Infinity,1.5,-0}"),
            "[\"NaN\",\"Infinity\",\"-Infinity\",1.5,-0]"
        );
        assert_eq!(j(1231, "{1.50,NaN}"), "[1.50,\"NaN\"]");
        assert_eq!(j(1016, "{-9223372036854775808}"), "[-9223372036854775808]");
        assert_eq!(
            j(3807, r#"{"{\"a\": 1}","[1, 2]",null,NULL}"#),
            r#"[{"a": 1},[1, 2],null,null]"#
        );
        assert_eq!(j(199, r#"{"\"s\"",3}"#), r#"["s",3]"#);
        assert_eq!(
            j(1185, r#"{"2026-10-02 10:00:00+00",infinity}"#),
            r#"["2026-10-02T10:00:00+00:00","infinity"]"#
        );
        assert_eq!(
            j(1115, r#"{"2026-10-02 10:00:00.5"}"#),
            r#"["2026-10-02T10:00:00.5"]"#
        );
        assert_eq!(j(1001, r#"{"\\x0a0b",NULL}"#), r#"["\\x0a0b",null]"#);
        assert_eq!(j(1014, r#"{"ab ",cd}"#), r#"["ab ","cd"]"#);
        assert_eq!(
            j(1041, "{192.168.1.5,::1/128}"),
            r#"["192.168.1.5","::1/128"]"#
        );
        assert_eq!(
            j(791, r#"{"$1,000.00",-$1.00}"#),
            r#"["$1,000.00","-$1.00"]"#
        );
        assert_eq!(
            j(1020, "{(1,1),(0,0);(2,2),(1,1)}"),
            r#"["(1,1),(0,0)","(2,2),(1,1)"]"#
        );
        assert_eq!(j(1017, r#"{"(1,2)","(3,4)"}"#), r#"["(1,2)","(3,4)"]"#);
        assert_eq!(j(1002, r#"{a,"\\",b}"#), r#"["a","\\","b"]"#);
        // Lenient on what array_in accepts and array_out never prints.
        assert_eq!(j(1007, " { 1 , 2 } "), "[1,2]");
        assert_eq!(j(1009, r"{a\,b,c\\}"), r#"["a,b","c\\"]"#);
        assert_eq!(j(1009, "{ab  ,c}"), r#"["ab","c"]"#);
        // Not array literals: strings.
        for bad in [
            "",
            "{",
            "}",
            "{1,2",
            "{1,,2}",
            "{,}",
            "{1}x",
            "[0:1]{1}",
            "[0:]={1}",
            "{\"a}",
            "{a\"b\"}",
            "{1}{2}",
            "{{{{{{{1}}}}}}}",
        ] {
            assert_eq!(j(1007, bad), serde_json::to_string(bad).unwrap(), "{bad:?}");
        }
        // Six dimensions is the server's limit and parses.
        assert_eq!(j(1007, "{{{{{{1}}}}}}"), "[[[[[[1]]]]]]");
    }

    #[test]
    fn arrays_of_types_only_the_catalog_knows() {
        // enum[] / composite[]: the caller passes typelem.
        assert_eq!(
            je(16_386, 16_385, "{sad,happy,NULL}"),
            r#"["sad","happy",null]"#
        );
        assert_eq!(
            je(16_391, 16_390, r#"{"(1,abc)","(2,\"x y\")"}"#),
            r#"["(1,abc)","(2,\"x y\")"]"#
        );
        // A domain over int[] whose element the caller resolved to int4.
        assert_eq!(je(16_392, oid::INT4, "{1,2}"), "[1,2]");
        // An unknown OID with no element: a string, even if it looks like
        // an array.
        assert_eq!(j(16_386, "{sad,happy}"), r#""{sad,happy}""#);
        // A caller that passed typelem for a non-array (int2vector): the
        // text has no array shape, so it is still one string.
        assert_eq!(je(22, oid::INT2, "1 2 3"), r#""1 2 3""#);
    }

    #[test]
    fn every_builtin_array_maps_to_its_element() {
        assert_eq!(array_element(1007), Some(oid::INT4));
        assert_eq!(array_element(1020), Some(oid::BOX));
        assert_eq!(array_element(oid::INT4), None);
        assert_eq!(array_element(22), None);
        assert_eq!(array_element(30), None);
        for (arr, elem) in [
            (1000, 16),
            (1001, 17),
            (1005, 21),
            (1007, 23),
            (1016, 20),
            (1028, 26),
            (1009, 25),
            (1015, 1043),
            (1014, 1042),
            (1003, 19),
            (1021, 700),
            (1022, 701),
            (1231, 1700),
            (2951, 2950),
            (199, 114),
            (3807, 3802),
            (1182, 1082),
            (1115, 1114),
            (1185, 1184),
            (1183, 1083),
            (1270, 1266),
            (1187, 1186),
            (1041, 869),
            (651, 650),
            (1040, 829),
            (1561, 1560),
            (1563, 1562),
            (1002, 18),
            (143, 142),
            (791, 790),
        ] {
            assert_eq!(array_element(arr), Some(elem), "{arr}");
        }
    }

    #[test]
    fn iso_times() {
        assert_eq!(iso_utc_micros(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            iso_utc_micros(1_790_935_200_123_456),
            "2026-10-02T10:00:00.123456Z"
        );
        assert_eq!(iso_utc_micros(-1), "1969-12-31T23:59:59.999999Z");
        assert_eq!(
            iso_utc_micros(951_782_400_000_000),
            "2000-02-29T00:00:00.000000Z"
        );
        assert_eq!(
            iso_utc_micros(4_107_542_400_000_000),
            "2100-03-01T00:00:00.000000Z"
        );
        assert_eq!(
            iso_utc_micros(-62_135_596_800_000_000),
            "0001-01-01T00:00:00.000000Z"
        );
        assert_eq!(
            iso_utc_micros(253_402_300_799_999_999),
            "9999-12-31T23:59:59.999999Z"
        );
        assert_eq!(
            iso_utc_micros(253_402_300_800_000_000),
            "+010000-01-01T00:00:00.000000Z"
        );
        assert_eq!(
            iso_utc_micros(-62_135_596_800_000_001),
            "0000-12-31T23:59:59.999999Z"
        );
        assert!(iso_utc_micros(i64::MIN).starts_with('-'));
        assert!(iso_utc_micros(i64::MAX).starts_with('+'));
        // Every day of several 400-year cycles round-trips through the civil
        // date (H. Hinnant's inverse, `days_from_civil`).
        let days_from_civil = |y: i64, m: i64, d: i64| {
            let y = if m <= 2 { y - 1 } else { y };
            let era = y.div_euclid(400);
            let yoe = y.rem_euclid(400);
            let mp = if m > 2 { m - 3 } else { m + 9 };
            let doy = (153 * mp + 2) / 5 + d - 1;
            let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            era * 146_097 + doe - 719_468
        };
        for day in -1_000_000..1_000_000i64 {
            let (y, m, d) = civil_from_days(day);
            assert!(
                (1..=12).contains(&m) && (1..=31).contains(&d),
                "{day}: {y}-{m}-{d}"
            );
            assert_eq!(days_from_civil(y, m, d), day);
        }
    }

    /// Hostile text for every type this module knows: valid JSON out, no
    /// panic.
    #[test]
    fn garbage_is_always_valid_json() {
        let mut seed: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet: Vec<char> = "{}[]:=,;\"\\ NULLnul-+.eE0123456789tf T:\n\u{1}é✓"
            .chars()
            .collect();
        let types = [
            oid::BOOL,
            oid::INT4,
            oid::INT8,
            oid::FLOAT8,
            oid::NUMERIC,
            oid::JSON,
            oid::JSONB,
            oid::TIMESTAMP,
            oid::TIMESTAMPTZ,
            oid::TEXT,
            1007,
            1009,
            1000,
            1022,
            1185,
            3807,
            1020,
            16_400,
        ];
        for _ in 0..30_000 {
            let len = (next() % 24) as usize;
            let text: String = (0..len)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect();
            for t in types {
                j(t, &text);
            }
            je(16_401, 16_400, &text);
        }
    }
}
