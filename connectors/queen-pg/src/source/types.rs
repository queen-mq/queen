//! Converters for types the value converter cannot know by OID alone: a
//! domain (rendered as its base type: a domain over `int4` is a number), an
//! array of a non-builtin type (`enum[]`, `domain[]`: a JSON array whose
//! elements are converted by the element's own converter). One `pg_type`
//! read per type, cached for the life of the engine and shared by the stream
//! and the snapshot, so both render a value the same way.

use std::collections::HashMap;

use crate::error::Result;
use crate::pg::connect::classify;

use super::events::TypeConv;

/// OIDs below this are built in (`FirstNormalObjectId`).
pub const FIRST_USER_OID: u32 = 16_384;

/// One `pg_type` row, as far as the converter cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeRow {
    /// `typtype`: `d` domain, `e` enum, `b` base, `c` composite, `r` range, …
    pub kind: u8,
    pub base: u32,
    pub elem: u32,
    pub len: i16,
    pub category: u8,
}

/// The converter of `oid` from its row and a lookup of other rows (pure).
pub fn conv_of(oid: u32, lookup: &dyn Fn(u32) -> Option<TypeRow>) -> TypeConv {
    fn go(oid: u32, lookup: &dyn Fn(u32) -> Option<TypeRow>, depth: u8) -> TypeConv {
        if oid < FIRST_USER_OID || depth > 8 {
            return TypeConv::builtin(oid);
        }
        let Some(row) = lookup(oid) else {
            return TypeConv::builtin(oid);
        };
        if row.kind == b'd' && row.base != 0 {
            return go(row.base, lookup, depth + 1);
        }
        if row.category == b'A' && row.elem != 0 && row.len == -1 {
            let e = go(row.elem, lookup, depth + 1);
            return TypeConv {
                oid,
                elem: Some(e.oid),
            };
        }
        TypeConv::builtin(oid)
    }
    go(oid, lookup, 0)
}

/// Read the `pg_type` rows of every user type `oids` needs (bases and
/// elements included) and fill `cache` with their converters.
pub async fn resolve(
    c: &tokio_postgres::Client,
    oids: &[u32],
    cache: &mut HashMap<u32, TypeConv>,
) -> Result<()> {
    let mut rows: HashMap<u32, TypeRow> = HashMap::new();
    let mut todo: Vec<u32> = oids
        .iter()
        .copied()
        .filter(|&o| o >= FIRST_USER_OID && !cache.contains_key(&o))
        .collect();
    let mut rounds = 0;
    while !todo.is_empty() && rounds < 8 {
        rounds += 1;
        todo.sort_unstable();
        todo.dedup();
        let found = c
            .query(
                "SELECT oid, typtype::text, typbasetype, typelem, typlen, typcategory::text \
                   FROM pg_catalog.pg_type WHERE oid = ANY($1)",
                &[&todo],
            )
            .await
            .map_err(|e| classify(&e))?;
        let mut next = Vec::new();
        for r in &found {
            let oid: u32 = r.get(0);
            let kind: String = r.get(1);
            let category: String = r.get(5);
            let row = TypeRow {
                kind: kind.bytes().next().unwrap_or(b'b'),
                base: r.get(2),
                elem: r.get(3),
                len: r.get(4),
                category: category.bytes().next().unwrap_or(b'U'),
            };
            for dep in [row.base, row.elem] {
                if dep >= FIRST_USER_OID && !rows.contains_key(&dep) && !cache.contains_key(&dep) {
                    next.push(dep);
                }
            }
            rows.insert(oid, row);
        }
        todo = next;
    }
    for &o in oids {
        if o >= FIRST_USER_OID && !cache.contains_key(&o) {
            let conv = conv_of(o, &|x| rows.get(&x).cloned());
            cache.insert(o, conv);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_resolve_to_their_base_and_user_arrays_to_their_element() {
        let rows: HashMap<u32, TypeRow> = [
            // domain posint over int4
            (
                20_000,
                TypeRow {
                    kind: b'd',
                    base: 23,
                    elem: 0,
                    len: 4,
                    category: b'N',
                },
            ),
            // posint[]
            (
                20_001,
                TypeRow {
                    kind: b'b',
                    base: 0,
                    elem: 20_000,
                    len: -1,
                    category: b'A',
                },
            ),
            // enum mood
            (
                20_002,
                TypeRow {
                    kind: b'e',
                    base: 0,
                    elem: 0,
                    len: 4,
                    category: b'E',
                },
            ),
            // mood[]
            (
                20_003,
                TypeRow {
                    kind: b'b',
                    base: 0,
                    elem: 20_002,
                    len: -1,
                    category: b'A',
                },
            ),
            // domain over int4[]
            (
                20_004,
                TypeRow {
                    kind: b'd',
                    base: 1007,
                    elem: 0,
                    len: -1,
                    category: b'A',
                },
            ),
        ]
        .into_iter()
        .collect();
        let lookup = |o: u32| rows.get(&o).cloned();
        assert_eq!(conv_of(20_000, &lookup), TypeConv::builtin(23));
        assert_eq!(
            conv_of(20_001, &lookup),
            TypeConv {
                oid: 20_001,
                elem: Some(23)
            }
        );
        assert_eq!(conv_of(20_002, &lookup), TypeConv::builtin(20_002));
        assert_eq!(
            conv_of(20_003, &lookup),
            TypeConv {
                oid: 20_003,
                elem: Some(20_002)
            }
        );
        assert_eq!(conv_of(20_004, &lookup), TypeConv::builtin(1007));
        assert_eq!(conv_of(25, &lookup), TypeConv::builtin(25));
        assert_eq!(conv_of(99_999, &lookup), TypeConv::builtin(99_999));
    }
}
