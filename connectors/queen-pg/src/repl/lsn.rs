//! A WAL position (log sequence number): 64 bits, written `X/X` in hex like
//! PostgreSQL (`0/16B3748`), the high and low 32-bit halves.

use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    /// 16 hex digits, zero padded: sorts like the number. Used in message ids.
    pub fn to_hex16(self) -> String {
        format!("{:016X}", self.0)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = String;

    fn from_str(s: &str) -> Result<Lsn, String> {
        let (hi, lo) = s
            .trim()
            .split_once('/')
            .ok_or_else(|| format!("not an LSN: {s:?}"))?;
        let hi = u32::from_str_radix(hi, 16).map_err(|_| format!("not an LSN: {s:?}"))?;
        let lo = u32::from_str_radix(lo, 16).map_err(|_| format!("not an LSN: {s:?}"))?;
        Ok(Lsn((u64::from(hi) << 32) | u64::from(lo)))
    }
}

impl serde::Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Lsn {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Lsn, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_postgres_notation() {
        let l: Lsn = "0/16B3748".parse().unwrap();
        assert_eq!(l.0, 0x16B3748);
        assert_eq!(l.to_string(), "0/16B3748");
        let h: Lsn = "1A/FF".parse().unwrap();
        assert_eq!(h.0, (0x1A << 32) | 0xFF);
        assert_eq!(h.to_string(), "1A/FF");
        assert_eq!(h.to_hex16(), "0000001A000000FF");
        assert!("nope".parse::<Lsn>().is_err());
        assert!(Lsn(1) < Lsn(2));
    }
}
