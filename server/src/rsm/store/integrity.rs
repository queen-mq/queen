//! Value integrity: store format 1, a checksum on every stored value.
//!
//! LMDB has no checksums. A flipped bit in `data.mdb` (Jepsen P6, 2026-09-25,
//! twice) reopened as a different value and the node served it. The queue logs
//! carry an xxh3 per record; since format 1 the store carries one per VALUE.
//!
//! # The checksum
//!
//! Every value the store keeps — every keyspace, `meta` included, in the RAM
//! tables and in the LMDB file alike — is stored as
//!
//! ```text
//! stored   = logical value ‖ checksum            (checksum: u64, little-endian)
//! checksum = xxh3_64(value, seed = xxh3_64(key, seed = keyspace seed))
//! keyspace seed = xxh3_64(keyspace NAME, seed = DOMAIN)
//! ```
//!
//! The keyspace and the key are INSIDE the checksum, so a value that lands
//! under the wrong key — or in the wrong keyspace, which is what a damaged
//! branch page pointing into another database's leaf looks like — fails
//! exactly as a flipped bit does. The keyspace is seeded by its NAME, not its
//! slot, because the name is what is permanent ([`Keyspace::name`]); the two
//! chained one-shot hashes keep the key/value boundary unambiguous without a
//! copy of either.
//!
//! # Computed once, verified on every read
//!
//! - COMPUTED where the value is born: [`super::Writes::put_raw`] seals it (the
//!   RAM table holds the sealed bytes; an LMDB-direct keyspace writes them with
//!   `put_reserved`). The checkpoint copies the sealed bytes unchanged, so the
//!   checksum that reaches the file, a snapshot and the next boot is the one
//!   computed from the value apply wrote: end to end, through RAM, the
//!   checkpoint thread, the file, a copy and the reload.
//! - VERIFIED on every read: `get_raw`, `scan_raw` and `scan_rev_raw` of both
//!   handles — so every typed read, the planner's, pop's and apply's, and the
//!   §12.9 digest — plus the load at open (every row of every keyspace, the key
//!   order, and each B-tree's own row count), the scrub, the copy a snapshot
//!   ships, and `read_checkpoint_meta`.
//! - The checksum never leaves the adapter: every caller sees the LOGICAL
//!   bytes, which is why the §12.9 digest is the same over both formats and on
//!   every node.
//!
//! # What a failure means
//!
//! [`StoreError::CorruptValue`] names the keyspace and the key. It is FATAL and
//! NODE-LOCAL: another node holds the same logical value under a correct
//! checksum, so corruption is never an apply outcome, never a planner effect,
//! never anything replicated. A corrupt store refuses to OPEN (boot fails with
//! the message below); a corrupt value found at RUNTIME poisons the store — it
//! refuses every later call — and calls the store's `on_corrupt` hook once (the
//! binary's hook ends the process; a restart reloads the last checkpoint, which
//! the load verifies).
//!
//! # Format and compatibility
//!
//! - A NEW store is created in format 1: the format row is written in the same
//!   LMDB transaction that creates the keyspaces.
//! - The format row lives in the `meta` DATABASE under [`FORMAT_KEY`] (sealed
//!   like every value), but it belongs to the adapter: it is never loaded into
//!   the RAM table, so no get, scan, count or digest sees it (a format is a
//!   property of this node's FILE, not of the replicated state), and the
//!   keyspace API refuses to write or delete it.
//! - A store WITHOUT the row and with rows is a format-0 (legacy) store,
//!   written before checksums. Opened by default it is MIGRATED once: one LMDB
//!   transaction rewrites every row sealed and adds the format row, then the
//!   normal format-1 load verifies what was just written. It is cheap because
//!   the open already reads the whole store into RAM, and it is atomic (a
//!   crash leaves it format 0 and the next open migrates again). With
//!   migration off (`StoreOpts::migrate_legacy = false`), or if the migration
//!   fails, a legacy store keeps working UNVERIFIED with one warning at open.
//! - A store with rows that verify as format 1 but no format row lost its
//!   format row: it is refused, never migrated (sealing it twice would corrupt
//!   every value).
//! - A format newer than this build knows is refused.
//! - A snapshot ships the store FILE, so it carries its own format row. The
//!   SENDER verifies its copy before it leaves (`copy_checkpoint`: damage stops
//!   on the node it belongs to, which is poisoned, instead of a follower
//!   refusing to boot from it); the RECEIVER verifies a format-1 image at the
//!   load and migrates a format-0 one there. A build that predates format 1
//!   cannot read a format-1 store (its fixed-width `meta` reads fail loudly):
//!   upgrade every node before a snapshot flows from a new node to an old one,
//!   and a node is not downgraded across this change.
//! - A migration seals the bytes it finds: damage a format-0 store took BEFORE
//!   its migration is sealed with it, and only the digest can then tell.
//!
//! # The scrub
//!
//! - At open with `QUEEN_STORE_VERIFY=1` ([`super::StoreOpts::verify_at_open`]):
//!   the whole image in one read transaction, every failure counted, the first
//!   one named, the open refused.
//! - At runtime, [`super::HeedStore::scrub_step`]: a bounded step (one short
//!   read transaction) of a pass over the image and then the RAM tables, for a
//!   background loop to pace; the first corrupt row is its error and poisons
//!   the store. [`super::HeedStore::scrub`] is the whole pass at once, and
//!   [`super::heed_store::scrub_dir`] the same over a store that is not open.
//!
//! # What it does not cover
//!
//! LMDB's own structure outside the leaves: a damaged free list can make a
//! later commit overwrite a live page — caught by the next load or scrub, not
//! prevented. A branch page that loses or repeats a subtree is caught by the
//! row-count and key-order checks, and a page LMDB cannot read by LMDB's own
//! checks (reported as [`StoreError::CorruptValue`] by the load and the scrub).
//! A well-formed but WRONG value written by a bug carries a valid checksum.

use std::sync::Arc;

use xxhash_rust::xxh3::xxh3_64_with_seed;

use super::{Keyspace, StoreError};

/// The store formats this build reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreFormat {
    /// Format 0: written by a build before value checksums. Values are the
    /// logical bytes and nothing verifies them.
    Legacy,
    /// Format 1: every value is `logical ‖ checksum` (module header).
    V1,
}

impl StoreFormat {
    /// What a new store is created in.
    pub const CURRENT: StoreFormat = StoreFormat::V1;

    /// The number recorded in the format row (format 0 has no row).
    pub fn version(self) -> u32 {
        match self {
            StoreFormat::Legacy => 0,
            StoreFormat::V1 => 1,
        }
    }

    /// Whether values carry (and reads verify) a checksum.
    pub fn checksummed(self) -> bool {
        self == StoreFormat::V1
    }
}

/// Bytes the checksum adds to every stored value.
pub const CHECKSUM_LEN: usize = 8;

/// The row of the `meta` database that records the store format: `u32`
/// little-endian, sealed with `meta`'s seed like every format-1 value. The
/// adapter's own row (module header): never in RAM, never in a scan or the
/// digest, refused by `put_raw` and `del_raw`.
pub const FORMAT_KEY: &[u8] = b"store_format";

/// Domain separation for the keyspace seeds ("QUEENSV1"). PERMANENT: it is in
/// every checksum a format-1 store holds.
const DOMAIN: u64 = 0x5155_4545_4E53_5631;

/// The checksum seed of one keyspace: its permanent NAME, hashed.
pub fn keyspace_seed(ks: Keyspace) -> u64 {
    xxh3_64_with_seed(ks.name().as_bytes(), DOMAIN)
}

/// Every keyspace's seed, indexed by [`Keyspace::slot`].
pub(crate) fn seeds() -> [u64; Keyspace::ALL.len()] {
    let mut out = [0u64; Keyspace::ALL.len()];
    for ks in Keyspace::ALL {
        out[ks.slot()] = keyspace_seed(ks);
    }
    out
}

/// The format-1 checksum of `val` stored under `key` in the keyspace whose seed
/// is `seed`.
#[inline]
pub fn checksum(seed: u64, key: &[u8], val: &[u8]) -> u64 {
    xxh3_64_with_seed(val, xxh3_64_with_seed(key, seed))
}

/// The stored form of `val`, `val ‖ checksum`, in ONE allocation — the same
/// single allocation `Arc::from(val)` made before format 1.
#[inline]
pub(crate) fn seal_arc(seed: u64, key: &[u8], val: &[u8]) -> Arc<[u8]> {
    let sum = checksum(seed, key, val).to_le_bytes();
    let n = val.len();
    let mut out = Arc::<[u8]>::new_uninit_slice(n + CHECKSUM_LEN);
    let dst = Arc::get_mut(&mut out).expect("a fresh Arc has no other owner");
    // SAFETY: `dst` is exactly `n + CHECKSUM_LEN` bytes long and the two copies
    // below write all of them — `n` bytes of `val` at 0, the 8 checksum bytes at
    // `n` — from sources that cannot overlap a fresh allocation. So every byte
    // is initialized before `assume_init`.
    unsafe {
        let p = dst.as_mut_ptr() as *mut u8;
        std::ptr::copy_nonoverlapping(val.as_ptr(), p, n);
        std::ptr::copy_nonoverlapping(sum.as_ptr(), p.add(n), CHECKSUM_LEN);
        out.assume_init()
    }
}

/// The stored form of `val` as a plain vector (the format row, the migration,
/// a test fixture).
pub(crate) fn seal_vec(seed: u64, key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(val.len() + CHECKSUM_LEN);
    out.extend_from_slice(val);
    out.extend_from_slice(&checksum(seed, key, val).to_le_bytes());
    out
}

/// Why a stored value did not verify.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mismatch {
    /// Shorter than its own checksum.
    Short(usize),
    /// The checksum it carries is not the one its keyspace, key and bytes make.
    Sum { stored: u64, computed: u64 },
}

/// Verify a stored format-1 value and return its logical bytes.
#[inline]
pub(crate) fn open<'v>(seed: u64, key: &[u8], stored: &'v [u8]) -> Result<&'v [u8], Mismatch> {
    let Some(split) = stored.len().checked_sub(CHECKSUM_LEN) else {
        return Err(Mismatch::Short(stored.len()));
    };
    let (val, tail) = stored.split_at(split);
    let mut b = [0u8; CHECKSUM_LEN];
    b.copy_from_slice(tail);
    let want = u64::from_le_bytes(b);
    let got = checksum(seed, key, val);
    if want == got {
        Ok(val)
    } else {
        Err(Mismatch::Sum {
            stored: want,
            computed: got,
        })
    }
}

/// Whether `stored` verifies as a format-1 value. The lost-format-row test of a
/// store that looks like format 0: a legacy value passes it with probability
/// 2^-64.
pub(crate) fn verifies(seed: u64, key: &[u8], stored: &[u8]) -> bool {
    open(seed, key, stored).is_ok()
}

/// The error a failed value check answers. `site` says where it was found ("at
/// the load", "by a read", "by the scrub", …).
pub(crate) fn mismatch_error(ks: Keyspace, key: &[u8], m: Mismatch, site: &str) -> StoreError {
    let detail = match m {
        Mismatch::Short(len) => format!(
            "a value of {len} B is shorter than its {CHECKSUM_LEN} B checksum, found {site}"
        ),
        Mismatch::Sum { stored, computed } => format!(
            "value checksum mismatch (stored {stored:#018x}, computed {computed:#018x}), found {site}"
        ),
    };
    StoreError::corrupt_value(ks, key, detail)
}

/// A key for an operator: its hex, and its printable bytes (names are escaped
/// `0x00`-terminated strings and integers are big-endian, so both views help).
/// Long keys are cut at 96 bytes.
pub fn render_key(key: &[u8]) -> String {
    const MAX: usize = 96;
    let shown = &key[..key.len().min(MAX)];
    let mut hex = String::with_capacity(shown.len() * 2 + 2);
    hex.push_str("0x");
    for b in shown {
        hex.push_str(&format!("{b:02x}"));
    }
    let text: String = shown
        .iter()
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect();
    if key.len() > MAX {
        format!("{hex}… (\"{text}…\", {} B)", key.len())
    } else {
        format!("{hex} (\"{text}\")")
    }
}

/// The operator's instruction, carried by every corruption this module
/// reports.
pub const RESTORE_HINT: &str = "This node's store is damaged and the node refuses to \
     serve it (the damage is node-local: nothing corrupt was applied or replicated). \
     Restore this node: a cluster member is wiped (stop it, delete its data directory) \
     and rejoins from a peer, which sends it a snapshot; a single node restores its data \
     directory from a backup";

/// Called ONCE, with the first corrupt value a store finds at runtime (the load
/// at open answers an error instead). The binary's hook ends the process — a
/// node-local fatal — so no caller that swallows a read error can go on
/// planning against a store that is known to be damaged.
#[derive(Clone)]
pub struct CorruptHook(pub Arc<dyn Fn(&StoreError) + Send + Sync>);

impl CorruptHook {
    pub fn new(f: impl Fn(&StoreError) + Send + Sync + 'static) -> CorruptHook {
        CorruptHook(Arc::new(f))
    }
}

impl std::fmt::Debug for CorruptHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CorruptHook(..)")
    }
}

// ---------------------------------------------------------------------------
// Scrub
// ---------------------------------------------------------------------------

/// What a scrub found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubReport {
    pub format: StoreFormat,
    /// Keyspaces walked.
    pub keyspaces: usize,
    /// Rows verified (the format row included).
    pub rows: u64,
    /// Key and stored value bytes walked.
    pub bytes: u64,
    /// Rows that failed (a value, the key order, or a B-tree's row count).
    pub corrupt: u64,
    /// The first failure, naming its keyspace and key.
    pub first_corrupt: Option<StoreError>,
}

impl ScrubReport {
    pub(crate) fn new(format: StoreFormat) -> ScrubReport {
        ScrubReport {
            format,
            keyspaces: 0,
            rows: 0,
            bytes: 0,
            corrupt: 0,
            first_corrupt: None,
        }
    }

    pub(crate) fn fail(&mut self, e: StoreError) {
        self.corrupt += 1;
        if self.first_corrupt.is_none() {
            self.first_corrupt = Some(e);
        }
    }

    /// `Err` with the first failure, if there was one.
    pub fn into_result(self) -> Result<ScrubReport, StoreError> {
        match &self.first_corrupt {
            Some(e) => Err(e.clone()),
            None => Ok(self),
        }
    }
}

/// Which half of a pass [`ScrubCursor`] is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum ScrubPhase {
    /// The LMDB image: what the next boot and a snapshot will read.
    #[default]
    Image,
    /// The RAM tables: what every read is served from.
    Ram,
}

/// Where an incremental scrub is ([`super::HeedStore::scrub_step`]). A pass
/// walks every keyspace of the LMDB image, then every RAM table, a bounded
/// number of rows per step, each step in one short read transaction; a
/// background loop calls it with a pause between steps. The cursor restarts
/// at the beginning when a pass completes.
#[derive(Clone, Debug, Default)]
pub struct ScrubCursor {
    pub(crate) phase: ScrubPhase,
    pub(crate) slot: usize,
    /// The last key verified in the current keyspace (the walk resumes after
    /// it), `None` at the start of a keyspace.
    pub(crate) after: Option<Vec<u8>>,
    /// Rows verified in the current pass.
    pub rows: u64,
    /// Passes completed.
    pub passes: u64,
}
