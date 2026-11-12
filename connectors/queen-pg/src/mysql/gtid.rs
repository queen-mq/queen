//! GTID sets: the MySQL source's position.
//!
//! A GTID names one transaction: the server uuid that first committed it,
//! an optional tag (MySQL 8.4, `uuid:tag:5`) and a number. A set is, per
//! (uuid, tag), a list of number ranges, written the way MySQL writes
//! `@@GLOBAL.gtid_executed`: `uuid:1-5:7,uuid2:1-3` (a tagged block is
//! `uuid:1-5:tag_a:1-3`, the tag applying to the ranges after it).
//!
//! The source's pointer is the set of transactions Queen has: the binlog
//! dump is asked for everything outside it (`COM_BINLOG_DUMP_GTID`), so a
//! restart, on this server or on a replica promoted in its place, goes on
//! from exactly there.

use std::collections::BTreeMap;
use std::fmt;

use mysql_common::packets::Sid;

use crate::error::{Error, Result};

/// One transaction's id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Gtid {
    /// Lowercase, hyphenated.
    pub uuid: String,
    /// Empty for an untagged GTID.
    pub tag: String,
    pub gno: u64,
}

impl Gtid {
    /// From a binlog GTID event's fields.
    pub fn from_event(sid: [u8; 16], tag: Option<&str>, gno: u64) -> Gtid {
        Gtid {
            uuid: uuid_text(&sid),
            tag: tag.unwrap_or("").to_ascii_lowercase(),
            gno,
        }
    }

    /// The uuid without hyphens (message ids).
    pub fn uuid_compact(&self) -> String {
        self.uuid.replace('-', "")
    }
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.tag.is_empty() {
            write!(f, "{}:{}", self.uuid, self.gno)
        } else {
            write!(f, "{}:{}:{}", self.uuid, self.tag, self.gno)
        }
    }
}

/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, lowercase.
pub fn uuid_text(b: &[u8; 16]) -> String {
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn valid_uuid(s: &str) -> bool {
    s.len() == 36
        && s.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// `[a-z_][a-z0-9_]{0,31}` (MySQL folds a tag to lowercase).
fn valid_tag(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && (b[0].is_ascii_lowercase() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

/// The largest transaction number MySQL hands out (2^63 - 1).
const GNO_MAX: u64 = i64::MAX as u64;

/// A set of GTIDs: per (uuid, tag), sorted disjoint inclusive ranges.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GtidSet {
    sids: BTreeMap<(String, String), Vec<(u64, u64)>>,
}

impl GtidSet {
    pub fn new() -> GtidSet {
        GtidSet::default()
    }

    /// MySQL's text form. Whitespace (MySQL puts a newline after each comma)
    /// is ignored; the empty string is the empty set.
    pub fn parse(s: &str) -> Result<GtidSet> {
        let bad = |why: &str| Error::io(format!("unreadable GTID set {s:?}: {why}"));
        let mut out = GtidSet::new();
        let flat: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        for block in flat.split(',').filter(|b| !b.is_empty()) {
            let mut parts = block.split(':');
            let uuid = parts.next().unwrap_or("").to_ascii_lowercase();
            if !valid_uuid(&uuid) {
                return Err(bad("a block does not start with a server uuid"));
            }
            let mut tag = String::new();
            let mut any = false;
            for p in parts {
                if p.as_bytes().first().is_some_and(|c| c.is_ascii_digit()) {
                    let (a, b) = match p.split_once('-') {
                        Some((a, b)) => (a, b),
                        None => (p, p),
                    };
                    let (Ok(a), Ok(b)) = (a.parse::<u64>(), b.parse::<u64>()) else {
                        return Err(bad("a range is not numbers"));
                    };
                    if a == 0 || a > b || b > GNO_MAX {
                        return Err(bad("a range is empty or out of 1..2^63"));
                    }
                    out.add_range(&uuid, &tag, a, b);
                    any = true;
                } else {
                    let t = p.to_ascii_lowercase();
                    if !valid_tag(&t) {
                        return Err(bad("a tag is not [a-z_][a-z0-9_]{0,31}"));
                    }
                    tag = t;
                }
            }
            if !any {
                return Err(bad("a block has no range"));
            }
        }
        Ok(out)
    }

    pub fn is_empty(&self) -> bool {
        self.sids.is_empty()
    }

    /// Add one transaction.
    pub fn add(&mut self, g: &Gtid) {
        self.add_range(&g.uuid, &g.tag, g.gno, g.gno);
    }

    fn add_range(&mut self, uuid: &str, tag: &str, a: u64, b: u64) {
        let v = self
            .sids
            .entry((uuid.to_string(), tag.to_string()))
            .or_default();
        // The common case: the next number of the last range.
        if let Some(last) = v.last_mut() {
            if a >= last.0 && a <= last.1.saturating_add(1) {
                last.1 = last.1.max(b);
                return;
            }
        }
        v.push((a, b));
        v.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(v.len());
        for &(x, y) in v.iter() {
            match merged.last_mut() {
                Some(m) if x <= m.1.saturating_add(1) => m.1 = m.1.max(y),
                _ => merged.push((x, y)),
            }
        }
        *v = merged;
    }

    pub fn contains(&self, g: &Gtid) -> bool {
        self.contains_range(&g.uuid, &g.tag, g.gno, g.gno)
    }

    fn contains_range(&self, uuid: &str, tag: &str, a: u64, b: u64) -> bool {
        let Some(v) = self.sids.get(&(uuid.to_string(), tag.to_string())) else {
            return false;
        };
        // The range must sit inside one of ours (ours are disjoint and
        // merged, so a range two of ours cover together does not exist).
        let i = v.partition_point(|r| r.1 < a);
        v.get(i).is_some_and(|r| r.0 <= a && b <= r.1)
    }

    /// Every transaction of `other` is in this set.
    pub fn contains_set(&self, other: &GtidSet) -> bool {
        other
            .sids
            .iter()
            .all(|((u, t), ranges)| ranges.iter().all(|&(a, b)| self.contains_range(u, t, a, b)))
    }

    /// The transactions of `other` this set lacks, as text (for an error
    /// that names them). Empty when `other` is a subset.
    pub fn missing_from(&self, other: &GtidSet) -> String {
        let mut miss = GtidSet::new();
        for ((u, t), ranges) in &other.sids {
            for &(a, b) in ranges {
                // Walk the range against ours, keeping the gaps.
                let mut at = a;
                let mine = self.sids.get(&(u.clone(), t.clone()));
                for &(x, y) in mine.map(Vec::as_slice).unwrap_or(&[]) {
                    if y < at || x > b {
                        continue;
                    }
                    if x > at {
                        miss.add_range(u, t, at, x - 1);
                    }
                    at = y.saturating_add(1);
                    if at > b {
                        break;
                    }
                }
                if at <= b {
                    miss.add_range(u, t, at, b);
                }
            }
        }
        miss.to_string()
    }

    /// The set as the dump request carries it.
    pub fn to_sids(&self) -> Result<Vec<Sid<'static>>> {
        let mut out = Vec::with_capacity(self.sids.len());
        for ((u, t), ranges) in &self.sids {
            let mut s = u.clone();
            if !t.is_empty() {
                s.push(':');
                s.push_str(t);
            }
            for &(a, b) in ranges {
                if a == b {
                    s.push_str(&format!(":{a}"));
                } else {
                    s.push_str(&format!(":{a}-{b}"));
                }
            }
            let sid: Sid<'static> = s
                .parse()
                .map_err(|e| Error::io(format!("GTID block {s:?}: {e}")))?;
            out.push(sid);
        }
        Ok(out)
    }
}

/// MySQL's own order: uuid blocks by uuid, the untagged ranges first, then
/// each tag's.
impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first_block = true;
        let mut current: Option<&str> = None;
        for ((u, t), ranges) in &self.sids {
            if current != Some(u.as_str()) {
                if !first_block {
                    f.write_str(",")?;
                }
                first_block = false;
                f.write_str(u)?;
                current = Some(u.as_str());
            }
            if !t.is_empty() {
                write!(f, ":{t}")?;
            }
            for &(a, b) in ranges {
                if a == b {
                    write!(f, ":{a}")?;
                } else {
                    write!(f, ":{a}-{b}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "8a94f357-aab4-11df-86ab-c80aa9429562";

    fn g(uuid: &str, gno: u64) -> Gtid {
        Gtid {
            uuid: uuid.into(),
            tag: String::new(),
            gno,
        }
    }

    #[test]
    fn parses_and_prints_mysql_text() {
        let s = GtidSet::parse(&format!("{},\n{}:1-5:7", B.to_uppercase() + ":1-3", A)).unwrap();
        assert_eq!(s.to_string(), format!("{A}:1-5:7,{B}:1-3"));
        assert!(GtidSet::parse("").unwrap().is_empty());
        assert!(GtidSet::parse(" \n").unwrap().is_empty());
        for bad in [
            "nope:1",
            A,
            &format!("{A}:0"),
            &format!("{A}:5-3"),
            &format!("{A}:x-3"),
            &format!("{A}:Bad-Tag:1"),
        ] {
            assert!(GtidSet::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn tags_are_their_own_ranges() {
        let s = GtidSet::parse(&format!("{A}:1-3:tag_a:1-2:7")).unwrap();
        assert_eq!(s.to_string(), format!("{A}:1-3:tag_a:1-2:7"));
        let tagged = Gtid {
            uuid: A.into(),
            tag: "tag_a".into(),
            gno: 2,
        };
        assert!(s.contains(&tagged));
        assert!(!s.contains(&Gtid {
            gno: 3,
            ..tagged.clone()
        }));
        assert!(s.contains(&g(A, 3)));
        assert_eq!(tagged.to_string(), format!("{A}:tag_a:2"));
    }

    #[test]
    fn add_merges_and_contains_checks_ranges() {
        let mut s = GtidSet::new();
        for n in [1, 2, 3, 5, 4, 9] {
            s.add(&g(A, n));
        }
        assert_eq!(s.to_string(), format!("{A}:1-5:9"));
        assert!(s.contains(&g(A, 4)));
        assert!(!s.contains(&g(A, 6)));
        assert!(!s.contains(&g(B, 1)));
        let sub = GtidSet::parse(&format!("{A}:2-4:9")).unwrap();
        assert!(s.contains_set(&sub));
        let not_sub = GtidSet::parse(&format!("{A}:4-6")).unwrap();
        assert!(!s.contains_set(&not_sub));
        assert_eq!(s.missing_from(&not_sub), format!("{A}:6"));
        let other = GtidSet::parse(&format!("{A}:1-12,{B}:1")).unwrap();
        assert_eq!(s.missing_from(&other), format!("{A}:6-8:10-12,{B}:1"));
        assert_eq!(s.missing_from(&sub), "");
    }

    #[test]
    fn the_dump_request_form_round_trips() {
        let s = GtidSet::parse(&format!("{A}:1-5:7,{B}:3")).unwrap();
        let sids = s.to_sids().unwrap();
        assert_eq!(sids.len(), 2);
        let iv: Vec<(u64, u64)> = sids[0]
            .intervals()
            .iter()
            .map(|i| (i.start(), i.end()))
            .collect();
        // The wire's ranges end one past the last number.
        assert_eq!(iv, vec![(1, 6), (7, 8)]);
        assert_eq!(uuid_text(&sids[1].uuid()), B);
    }

    #[test]
    fn an_event_sid_is_printed_as_mysql_does() {
        let mut b = [0u8; 16];
        hex::decode_to_slice(A.replace('-', ""), &mut b).unwrap();
        let g = Gtid::from_event(b, None, 42);
        assert_eq!(g.to_string(), format!("{A}:42"));
        assert_eq!(g.uuid_compact(), A.replace('-', ""));
    }
}
