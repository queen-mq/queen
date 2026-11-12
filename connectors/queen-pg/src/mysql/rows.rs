//! Binlog row images → values.
//!
//! `mysql_common` decodes every column type but one the way MySQL writes it;
//! its `TIME(1)`/`TIME(2)` reader (0.37.3, `binlog::misc::
//! my_time_packed_from_binary`) subtracts from a `u32` for a negative time
//! with a fraction, which panics in a debug build and yields a wrong time in
//! a release one. So the images are framed here (null bitmap, present
//! columns) and each value goes to `mysql_common`'s decoder, `TIME2` to
//! [`time2`], MySQL's own algorithm (`my_time_packed_from_binary` in
//! mysys/my_time.cc).

use mysql_common::binlog::events::{OptionalMetaExtractor, RowsEventData, TableMapEvent};
use mysql_common::binlog::value::BinlogValue;
use mysql_common::constants::ColumnType;
use mysql_common::io::ParseBuf;
use mysql_common::value::Value;

use crate::error::{Error, Result};

/// One row image: a value per column the image holds (every column under
/// `binlog_row_image=FULL`), in table order.
pub type Image = Vec<BinlogValue<'static>>;

fn short(what: &str) -> Error {
    Error::io(format!("binlog: a row image ends inside {what}"))
}

/// Every row of a rows event, as `(before, after)` images.
pub fn decode(
    ev: &RowsEventData<'_>,
    tme: &TableMapEvent<'_>,
) -> Result<Vec<(Option<Image>, Option<Image>)>> {
    let bad = |e: std::io::Error| Error::io(format!("binlog: a table map: {e}"));
    let n = ev.num_columns() as usize;
    let meta = OptionalMetaExtractor::new(tme.iter_optional_meta()).map_err(bad)?;
    let mut sign = meta.iter_signedness();
    let mut cols: Vec<(ColumnType, &[u8], bool)> = Vec::with_capacity(n);
    for i in 0..n {
        let ty = tme
            .get_column_type(i)
            .map_err(|e| Error::io(format!("binlog: column {i} of a table map: {e}")))?
            .ok_or_else(|| Error::io(format!("binlog: a table map lacks column {i}")))?;
        let unsigned = ty.is_numeric_type() && sign.next().unwrap_or(false);
        cols.push((ty, tme.get_column_metadata(i).unwrap_or(&[]), unsigned));
    }
    let (before, after) = (ev.columns_before_image(), ev.columns_after_image());
    let mut buf = ParseBuf(ev.rows_data());
    let mut out = Vec::new();
    while !buf.is_empty() {
        let b = match before {
            Some(bits) => Some(image(&mut buf, &cols, |i| bits.get(i).is_some_and(|b| *b))?),
            None => None,
        };
        let a = match after {
            Some(bits) => Some(image(&mut buf, &cols, |i| bits.get(i).is_some_and(|b| *b))?),
            None => None,
        };
        out.push((b, a));
    }
    Ok(out)
}

fn image(
    buf: &mut ParseBuf<'_>,
    cols: &[(ColumnType, &[u8], bool)],
    present: impl Fn(usize) -> bool,
) -> Result<Image> {
    let count = (0..cols.len()).filter(|&i| present(i)).count();
    let nulls = buf
        .checked_eat(count.div_ceil(8))
        .ok_or_else(|| short("the null bitmap"))?
        .to_vec();
    let mut out = Vec::with_capacity(count);
    let mut at = 0usize;
    for (i, (ty, meta, unsigned)) in cols.iter().enumerate() {
        if !present(i) {
            continue;
        }
        let null = nulls[at / 8] & (1 << (at % 8)) != 0;
        at += 1;
        if null {
            out.push(BinlogValue::Value(Value::NULL));
            continue;
        }
        let v = if *ty == ColumnType::MYSQL_TYPE_TIME2 {
            BinlogValue::Value(time2(buf, meta.first().copied().unwrap_or(0))?)
        } else {
            let mut sub = ParseBuf(buf.0);
            let v: BinlogValue<'_> = sub
                .parse((*ty, *meta, *unsigned, false))
                .map_err(|e| Error::io(format!("binlog: a {ty:?} value: {e}")))?;
            let used = buf.0.len() - sub.0.len();
            buf.skip(used);
            v.into_owned()
        };
        out.push(v);
    }
    Ok(out)
}

/// A `TIME2` value (fractional precision `dec`).
pub fn time2(buf: &mut ParseBuf<'_>, dec: u8) -> Result<Value> {
    const INT_OFS: i64 = 0x80_0000;
    const OFS: i64 = 0x8000_0000_0000;
    let packed: i64 = match dec {
        1 | 2 => {
            let mut int =
                i64::from(buf.checked_eat_u24_be().ok_or_else(|| short("a TIME"))?) - INT_OFS;
            let mut frac = i64::from(buf.checked_eat_u8().ok_or_else(|| short("a TIME"))?);
            if int < 0 && frac > 0 {
                int += 1;
                frac -= 0x100;
            }
            (int << 24) + frac * 10_000
        }
        3 | 4 => {
            let mut int =
                i64::from(buf.checked_eat_u24_be().ok_or_else(|| short("a TIME"))?) - INT_OFS;
            let mut frac = i64::from(buf.checked_eat_u16_be().ok_or_else(|| short("a TIME"))?);
            if int < 0 && frac > 0 {
                int += 1;
                frac -= 0x1_0000;
            }
            (int << 24) + frac * 100
        }
        5 | 6 => buf.checked_eat_u48_be().ok_or_else(|| short("a TIME"))? as i64 - OFS,
        _ => (i64::from(buf.checked_eat_u24_be().ok_or_else(|| short("a TIME"))?) - INT_OFS) << 24,
    };
    let neg = packed < 0;
    let t = packed.unsigned_abs();
    let hms = t >> 24;
    let us = (t % (1 << 24)) as u32;
    let h = ((hms >> 12) % (1 << 10)) as u32;
    Ok(Value::Time(
        neg,
        h / 24,
        (h % 24) as u8,
        ((hms >> 6) % (1 << 6)) as u8,
        (hms % (1 << 6)) as u8,
        us,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MySQL's `my_time_packed_to_binary`, for the round trip.
    fn pack(neg: bool, h: i64, m: i64, s: i64, us: i64, dec: u8) -> Vec<u8> {
        let hms = (h << 12) | (m << 6) | s;
        let mut packed = (hms << 24) + us;
        if neg {
            packed = -packed;
        }
        // C's: `>>` floors, `%` keeps the dividend's sign.
        let int = packed >> 24;
        let frac = packed % (1 << 24);
        match dec {
            1 | 2 => {
                let v = (int + 0x80_0000) as u32;
                let f = (frac / 10_000) as i8 as u8;
                vec![(v >> 16) as u8, (v >> 8) as u8, v as u8, f]
            }
            _ => {
                let v = (packed + 0x8000_0000_0000) as u64;
                (0..6).rev().map(|k| (v >> (8 * k)) as u8).collect()
            }
        }
    }

    #[test]
    fn negative_times_with_fractions_decode() {
        for (neg, h, m, s, us, dec) in [
            (true, 838, 59, 58, 500_000, 2),
            (true, 0, 0, 1, 500_000, 2),
            (true, 0, 0, 1, 0, 2),
            (false, 10, 20, 30, 120_000, 2),
            (true, 1, 2, 3, 456_789, 6),
            (false, 0, 0, 0, 0, 6),
        ] {
            let b = pack(neg, h, m, s, us, dec);
            let v = time2(&mut ParseBuf(&b), dec).unwrap();
            let neg = neg && (h, m, s, us) != (0, 0, 0, 0);
            assert_eq!(
                v,
                Value::Time(
                    neg,
                    (h / 24) as u32,
                    (h % 24) as u8,
                    m as u8,
                    s as u8,
                    us as u32
                ),
                "{h}:{m}:{s}.{us} dec {dec}"
            );
        }
    }
}
