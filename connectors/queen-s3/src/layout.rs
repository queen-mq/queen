//! Object keys (plan §6.3): where a window's bytes land, and what a reader
//! globs to find them.
//!
//! ```text
//! <prefix>/queue=<esc>/dt=2026-09-04/hour=10/
//!     w-0000001842-1756980000000000-1756980300000000.jsonl.zst        ← merged
//!     w-0000001842-…-p-<esc partition>-000000001400-000000001811.parquet  ← per-partition
//! <prefix>/_queen/<esc queue>/windows/0000001842.json                  ← manifest
//! <prefix>/_queen/<esc queue>/checkpoint/0000001840.json.zst           ← positions
//! ```
//!
//! Three properties the tests pin, each of which something depends on:
//!
//! * **Deterministic.** The key is a function of the intent (`k`, `tStart`,
//!   `tEnd`) and nothing else — no wall clock, no attempt counter, no random
//!   suffix. That is what makes the retry of plan §4.3 step 5 an OVERWRITE of
//!   the same key with the same bytes rather than a second object, and it is why
//!   exactly-once here needs no conditional PUT and no LIST.
//! * **Escaped.** Everything outside `[A-Za-z0-9._-]` is percent-encoded with
//!   uppercase hex (the rule of offsets.rs:341), so a partition named `a/b`, a
//!   queue with a space, and a name containing `..` cannot escape their prefix.
//! * **Ordered.** Keys sort lexicographically in window order, because `k` is
//!   zero-padded and leads. A reader that lists a bucket gets the windows in
//!   commit order for free.
//!
//! `_queen/` is deliberately NOT a Hive partition: readers glob
//! `queue=*/dt=*/hour=*/*.parquet` and walk straight past it.

use crate::types::{Align, Layout, Micros};

/// Digits `k` is padded to. Ten digits is 10^10 windows — at one window every
/// five minutes, about 95 000 years — and it is what makes the lexicographic
/// order of the keys the order of the windows.
pub const K_DIGITS: usize = 10;
/// Digits a microsecond timestamp is padded to in a key.
pub const TS_DIGITS: usize = 16;
/// Digits an offset is padded to in a per-partition key.
pub const OFFSET_DIGITS: usize = 12;

/// The sidecar root. Not a Hive partition, and named so that a glob over
/// `queue=*` never sees it.
pub const SIDECAR: &str = "_queen";

/// Percent-encode everything outside `[A-Za-z0-9._-]`, uppercase hex.
///
/// The same function, and the same set, as the offset store's key escaping
/// (protocols/queen-kafka/src/offsets.rs:341). The set is what an ordinary
/// queue or entity name is already made of (`orders`, `cust-0420`,
/// `svc.billing`), so a normal name is never rewritten; what it catches is the
/// separator, the escape character itself, and every byte that would give a
/// name authority over the key structure — `/`, `%`, `=`, and anything
/// non-ASCII.
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The inverse of [`escape`]. An escape that is not one is left as it stands
/// rather than dropped: this reads keys back off a listing, and a key that does
/// not round-trip must still be recognisable in a log line.
pub fn unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(b) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Everything a data object's key is derived from.
///
/// A struct rather than eleven positional arguments, and not only because
/// clippy counts: `t_start`, `t_end` and `bucket_ts` are three timestamps of
/// which two are nearly always equal, and a call site that passes them in the
/// wrong order would produce a perfectly plausible key for the wrong window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataKey<'a> {
    /// `QUEEN_S3_PREFIX`, already stripped of its slashes by the config.
    pub prefix: &'a str,
    pub queue: &'a str,
    pub layout: Layout,
    pub align: Align,
    /// The window number. Monotone per queue, and what makes the key sort.
    pub k: u64,
    pub t_start: Micros,
    pub t_end: Micros,
    /// The timestamp the `dt=`/`hour=` buckets are derived FROM.
    ///
    /// It is a parameter rather than `t_start` because of one case: the first
    /// window of `start=earliest` opens at [`Micros::MIN`] (`-∞`), which is not
    /// a date. The engine passes the FIRST RECORD's `ts` there, so the bucket
    /// names the hour the data is actually in. Every other window passes
    /// `t_start`, and windows never straddle an alignment boundary (plan §4.4),
    /// so the bucket is exact for every record in the object.
    pub bucket_ts: Micros,
    /// `jsonl.zst`, `jsonl.gz`, `jsonl`, `parquet` — the writer's own.
    pub ext: &'a str,
    /// `Some` in [`Layout::PerPartition`], `None` in [`Layout::Merged`].
    pub partition: Option<&'a str>,
    /// `(first, last)` offsets in this object, per-partition layout only.
    pub offsets: Option<(i64, i64)>,
}

impl DataKey<'_> {
    /// The object key. See [`data_key`].
    pub fn render(&self) -> String {
        data_key(self)
    }
}

/// The data object's key.
///
/// `merged`:
/// `<prefix>/queue=<esc>/dt=YYYY-MM-DD/hour=HH/w-<k>-<tStart>-<tEnd>.<ext>`
///
/// `per-partition`: the same, plus `-p-<esc partition>-<first>-<last>` before
/// the extension — the Connect-shaped key a reader that wants one entity's file
/// can find by name.
///
/// With [`Align::Day`] the `hour=` component is omitted. With [`Align::None`]
/// both are still written, derived from `bucket_ts`: a lake with no `dt=` at all
/// is one a partition-pruning reader cannot help, and the plan asks for the
/// buckets of `t_start` anyway.
pub fn data_key(k: &DataKey<'_>) -> String {
    // The two must agree: a `per-partition` key with no partition would be a
    // merged key under a per-partition sink — one object per window instead of
    // one per lane, silently overwriting every lane but the last. It is a
    // caller bug, so it is a debug assertion rather than a runtime branch.
    debug_assert_eq!(
        k.layout == Layout::PerPartition,
        k.partition.is_some(),
        "layout {:?} and partition {:?} disagree",
        k.layout,
        k.partition
    );
    let mut out = String::with_capacity(160);
    out.push_str(k.prefix);
    out.push_str("/queue=");
    out.push_str(&escape(k.queue));
    out.push_str(&hive_path(k.bucket_ts, k.align));
    out.push_str("/w-");
    out.push_str(&pad_u64(k.k, K_DIGITS));
    out.push('-');
    out.push_str(&pad_us(k.t_start));
    out.push('-');
    out.push_str(&pad_us(k.t_end));
    if let Some(p) = k.partition {
        out.push_str("-p-");
        out.push_str(&escape(p));
        if let Some((first, last)) = k.offsets {
            out.push('-');
            out.push_str(&pad_offset(first));
            out.push('-');
            out.push_str(&pad_offset(last));
        }
    }
    out.push('.');
    out.push_str(k.ext);
    out
}

/// `<prefix>/_queen/<esc queue>/windows/<k>.json` — one manifest per committed
/// window, and the only place a wall-clock value is written (plan §4.2).
pub fn manifest_key(prefix: &str, queue: &str, k: u64) -> String {
    format!(
        "{prefix}/{SIDECAR}/{}/windows/{}.json",
        escape(queue),
        pad_u64(k, K_DIGITS)
    )
}

/// `<prefix>/_queen/<esc queue>/checkpoint/<k>.json.zst` — the position cache
/// of plan §4.5 (1).
pub fn checkpoint_key(prefix: &str, queue: &str, k: u64) -> String {
    format!(
        "{prefix}/{SIDECAR}/{}/checkpoint/{}.json.zst",
        escape(queue),
        pad_u64(k, K_DIGITS)
    )
}

/// What a LIST is given to enumerate one queue's checkpoints.
pub fn checkpoint_prefix(prefix: &str, queue: &str) -> String {
    format!("{prefix}/{SIDECAR}/{}/checkpoint/", escape(queue))
}

/// What a LIST is given to enumerate one queue's manifests.
pub fn manifest_prefix(prefix: &str, queue: &str) -> String {
    format!("{prefix}/{SIDECAR}/{}/windows/", escape(queue))
}

/// The window number a checkpoint key names, or `None` when the key is not one.
///
/// Deliberately tolerant about what comes BEFORE `/checkpoint/`: a listing is
/// answered by the bucket, and a key that arrived with a different prefix (a
/// bucket holding two sinks, a prefix somebody changed) must be recognised or
/// skipped, never mis-parsed into a window number that is not there.
pub fn parse_checkpoint_key(key: &str) -> Option<u64> {
    let (_, tail) = key.rsplit_once("/checkpoint/")?;
    let digits = tail.strip_suffix(".json.zst")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The window number a manifest key names, or `None`.
pub fn parse_manifest_key(key: &str) -> Option<u64> {
    let (_, tail) = key.rsplit_once("/windows/")?;
    let digits = tail.strip_suffix(".json")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The newest checkpoint at or below `at_most` in a listing — the restart path
/// of plan §4.5: load the latest checkpoint `≤ committed.k`, then re-read at
/// most `CHECKPOINT_EVERY` windows per partition.
///
/// `at_most` matters: a checkpoint written for a window that was never
/// committed (a crash between the object and the pointer) names positions past
/// the commit truth, and reading from it would SKIP records. Correctness never
/// depends on a position, but this is the one way a position could cost more
/// than a re-read.
pub fn latest_checkpoint<'a, I>(keys: I, at_most: u64) -> Option<(u64, &'a str)>
where
    I: IntoIterator<Item = &'a str>,
{
    keys.into_iter()
        .filter_map(|key| parse_checkpoint_key(key).map(|k| (k, key)))
        .filter(|(k, _)| *k <= at_most)
        .max_by_key(|(k, _)| *k)
}

/// `/dt=YYYY-MM-DD/hour=HH`, or `/dt=YYYY-MM-DD` under [`Align::Day`].
fn hive_path(bucket_ts: Micros, align: Align) -> String {
    // `to_iso` is the broker's own rendering, so the date components here are
    // the ones every other surface of the connector prints — one calendar, one
    // implementation, and no second civil-date conversion to get wrong.
    let iso = bucket_ts.to_iso();
    let (date, hour) = match iso.len() >= 13 && iso.as_bytes()[10] == b'T' {
        true => (&iso[0..10], &iso[11..13]),
        // Only reachable for `Micros::MIN`, whose `to_iso` is `-inf`. A caller
        // that got here passed `-∞` as the BUCKET timestamp, which the doc on
        // `DataKey::bucket_ts` forbids; the epoch is the honest fallback and it
        // keeps the key well-formed rather than raising from a path builder.
        false => ("1970-01-01", "00"),
    };
    match align {
        Align::Day => format!("/dt={date}"),
        Align::Hour | Align::None => format!("/dt={date}/hour={hour}"),
    }
}

fn pad_u64(v: u64, digits: usize) -> String {
    format!("{v:0digits$}")
}

/// A microsecond timestamp, zero-padded.
///
/// Negative values are clamped to 0, and exactly one value is ever negative:
/// [`Micros::MIN`], the `-∞` start of the first `earliest` window. Rendering it
/// as `-9223372036854775808` would be twenty characters where the format says
/// sixteen and would sort before every other key of the queue by accident
/// rather than by design. The window's real start is in the intent and in the
/// manifest; the key's job is identity and order, and `k` carries both.
fn pad_us(t: Micros) -> String {
    let v = t.0.max(0) as u64;
    pad_u64(v, TS_DIGITS)
}

/// An offset, zero-padded. A negative offset is not a thing the log produces
/// (`-1` is "never written", which no object ever contains), so it is clamped
/// the same way.
fn pad_offset(v: i64) -> String {
    pad_u64(v.max(0) as u64, OFFSET_DIGITS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> Micros {
        Micros::parse_iso(s).unwrap()
    }

    fn merged<'a>(k: u64, start: &str, end: &str) -> DataKey<'a> {
        DataKey {
            prefix: "queen",
            queue: "orders",
            layout: Layout::Merged,
            align: Align::Hour,
            k,
            t_start: ts(start),
            t_end: ts(end),
            bucket_ts: ts(start),
            ext: "jsonl.zst",
            partition: None,
            offsets: None,
        }
    }

    #[test]
    fn the_merged_key_is_the_plan_s_own_example() {
        let key = merged(1842, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z").render();
        assert_eq!(
            key,
            "queen/queue=orders/dt=2026-09-04/hour=10/\
             w-0000001842-1788516000000000-1788516300000000.jsonl.zst"
        );
    }

    #[test]
    fn the_per_partition_key_carries_the_lane_and_its_offsets() {
        let mut k = merged(1842, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z");
        k.layout = Layout::PerPartition;
        k.partition = Some("cust-0420");
        k.offsets = Some((1400, 1811));
        k.ext = "parquet";
        assert_eq!(
            k.render(),
            "queen/queue=orders/dt=2026-09-04/hour=10/\
             w-0000001842-1788516000000000-1788516300000000-p-cust-0420-000000001400-000000001811.parquet"
        );
    }

    #[test]
    fn the_key_is_a_function_of_the_intent_and_nothing_else() {
        let a = merged(7, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z").render();
        let b = merged(7, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z").render();
        assert_eq!(a, b, "two renders of one intent must be one key");
    }

    #[test]
    fn escaping_stops_a_name_leaving_its_prefix() {
        assert_eq!(escape("a/b"), "a%2Fb");
        assert_eq!(escape("with space"), "with%20space");
        assert_eq!(
            escape(".."),
            "..",
            "dots are in the safe set — the SLASH is what matters"
        );
        assert_eq!(escape("../../etc"), "..%2F..%2Fetc");
        assert_eq!(escape("caffè"), "caff%C3%A8");
        assert_eq!(escape("100%"), "100%25");
        assert_eq!(escape("dt=x"), "dt%3Dx", "= would forge a Hive component");
        assert_eq!(
            escape("cust-0420.eu_1"),
            "cust-0420.eu_1",
            "an ordinary name is untouched"
        );
    }

    #[test]
    fn escaping_round_trips() {
        for s in [
            "a/b",
            "with space",
            "caffè",
            "100%",
            "../../etc",
            "dt=x",
            "plain",
        ] {
            assert_eq!(unescape(&escape(s)), s, "{s}");
        }
        // Not an escape: left as it stands rather than dropped.
        assert_eq!(unescape("100%zz"), "100%zz");
    }

    #[test]
    fn an_escaped_name_cannot_forge_a_path_component() {
        let k = DataKey {
            queue: "../../secrets",
            partition: Some("a/b"),
            layout: Layout::PerPartition,
            offsets: Some((0, 0)),
            ..merged(1, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z")
        };
        let key = k.render();
        assert!(key.starts_with("queen/queue=..%2F..%2Fsecrets/"), "{key}");
        assert!(key.contains("-p-a%2Fb-"), "{key}");
        // Exactly four slashes: prefix, queue=, dt=, hour=. No name added one.
        assert_eq!(key.matches('/').count(), 4, "{key}");
    }

    #[test]
    fn lexicographic_order_is_window_order() {
        let mut keys: Vec<String> = (0..40u64)
            .map(|k| {
                let start = Micros::from_millis(1_756_980_000_000 + (k as i64) * 300_000);
                let end = start.saturating_add(Micros(300_000_000));
                DataKey {
                    k,
                    t_start: start,
                    t_end: end,
                    bucket_ts: start,
                    ..merged(k, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z")
                }
                .render()
            })
            .collect();
        let expected = keys.clone();
        keys.sort();
        assert_eq!(keys, expected, "sorted keys must equal the commit order");
    }

    #[test]
    fn alignment_decides_the_hive_components() {
        let hour = merged(1, "2026-09-04T10:03:00Z", "2026-09-04T10:05:00Z");
        assert!(hour.render().contains("/dt=2026-09-04/hour=10/"));

        let mut day = hour.clone();
        day.align = Align::Day;
        assert!(
            day.render().contains("/dt=2026-09-04/w-"),
            "{}",
            day.render()
        );
        assert!(!day.render().contains("hour="));

        // `none` still writes both, derived from the bucket timestamp: a lake
        // with no dt= is one no partition-pruning reader can help.
        let mut none = hour.clone();
        none.align = Align::None;
        assert!(none.render().contains("/dt=2026-09-04/hour=10/"));
    }

    #[test]
    fn the_bucket_timestamp_and_not_t_start_names_the_hour() {
        // The first `earliest` window: t_start is −∞, and the bucket is the
        // first record's own timestamp.
        let k = DataKey {
            t_start: Micros::MIN,
            bucket_ts: ts("2026-09-04T07:41:02.5Z"),
            ..merged(0, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z")
        };
        let key = k.render();
        assert!(key.contains("/dt=2026-09-04/hour=07/"), "{key}");
        assert!(key.contains("/w-0000000000-0000000000000000-"), "{key}");
        assert!(!key.contains("9223372036854775808"), "{key}");
    }

    #[test]
    #[should_panic(expected = "disagree")]
    fn a_per_partition_layout_with_no_partition_is_a_caller_bug() {
        let mut k = merged(1, "2026-09-04T10:00:00Z", "2026-09-04T10:05:00Z");
        k.layout = Layout::PerPartition;
        let _ = k.render();
    }

    #[test]
    fn the_sidecar_keys_are_where_the_plan_puts_them() {
        assert_eq!(
            manifest_key("queen", "orders", 1842),
            "queen/_queen/orders/windows/0000001842.json"
        );
        assert_eq!(
            checkpoint_key("queen", "orders", 1840),
            "queen/_queen/orders/checkpoint/0000001840.json.zst"
        );
        assert_eq!(
            checkpoint_prefix("queen", "or/ders"),
            "queen/_queen/or%2Fders/checkpoint/"
        );
        assert_eq!(
            manifest_prefix("queen", "orders"),
            "queen/_queen/orders/windows/"
        );
    }

    #[test]
    fn checkpoint_keys_parse_back_and_nothing_else_does() {
        assert_eq!(
            parse_checkpoint_key("queen/_queen/orders/checkpoint/0000001840.json.zst"),
            Some(1840)
        );
        assert_eq!(
            parse_checkpoint_key("other-prefix/_queen/orders/checkpoint/0000000007.json.zst"),
            Some(7)
        );
        for not_one in [
            "queen/_queen/orders/windows/0000001840.json",
            "queen/_queen/orders/checkpoint/latest.json.zst",
            "queen/_queen/orders/checkpoint/.json.zst",
            "queen/_queen/orders/checkpoint/0000001840.json",
            "queen/queue=orders/dt=2026-09-04/hour=10/w-1-2-3.jsonl.zst",
        ] {
            assert_eq!(parse_checkpoint_key(not_one), None, "{not_one}");
        }
        assert_eq!(
            parse_manifest_key("queen/_queen/orders/windows/0000001842.json"),
            Some(1842)
        );
        assert_eq!(
            parse_manifest_key("queen/_queen/orders/checkpoint/1.json.zst"),
            None
        );
    }

    #[test]
    fn the_latest_checkpoint_never_runs_ahead_of_the_commit() {
        let keys = vec![
            "queen/_queen/orders/checkpoint/0000000020.json.zst",
            "queen/_queen/orders/checkpoint/0000000040.json.zst",
            "queen/_queen/orders/checkpoint/0000000060.json.zst",
            "queen/_queen/orders/windows/0000000041.json",
            "queen/_queen/orders/checkpoint/rubbish",
        ];
        assert_eq!(
            latest_checkpoint(keys.clone(), 50),
            Some((40, "queen/_queen/orders/checkpoint/0000000040.json.zst"))
        );
        assert_eq!(
            latest_checkpoint(keys.clone(), 1_000).map(|(k, _)| k),
            Some(60)
        );
        assert_eq!(latest_checkpoint(keys.clone(), 19), None);
        assert_eq!(latest_checkpoint(Vec::<&str>::new(), 5), None);
    }
}
