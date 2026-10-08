//! The cluster link: a STANDBY cluster replays a SOURCE cluster's log.
//!
//! Two clusters, each with its own raft group, its own membership and its own
//! log indexes. The source serves its clients as it always did. The standby's
//! leader pulls the source's committed entries over HTTP and proposes each of
//! them into its own log, so the standby holds the source's state a few
//! moments behind: messages, cursors and their leases, KV, timers, the dedup
//! window — everything an entry carries. A standby refuses its own clients'
//! writes until it is PROMOTED; from that entry on it is an ordinary cluster.
//!
//! # Why replaying works
//!
//! An entry is self-contained ([`super::entry`]): the planner stamped the
//! time (`now_us`), the partition ids and KV versions it hands out, and every
//! offset. Apply has no clock and reads nothing but the entry and the state
//! before it, so the same entries in the same order build the same state on
//! any node — that is what makes a follower a follower — and nothing in an
//! entry names the log index it sat at. Two things in the state do record a
//! log position: `meta.applied_index` / `applied_term`, and a group row's
//! `reg_index`. Both are this cluster's own positions and are never compared
//! across clusters.
//!
//! Apply's gates are the divergence check: an entry whose stamp or bases
//! (`pid_base`, `kv_version_base`) do not continue this cluster's state is
//! refused, and a refused entry stops every node. So the standby's driver
//! checks the same three BEFORE it proposes ([`Cursor::admit`]): a standby
//! that drifted from its source stops the link with a message, and keeps
//! running.
//!
//! # What the standby writes of its own
//!
//! Nothing but the link's own rows, in [`Keyspace::Flags`] under
//! [`FLAG_PREFIX`] (flags are whole-row overwrites that assign no partition
//! id and no KV version, so they never move a base the source's next entry
//! depends on):
//!
//! - [`FLAG_ROLE`]: [`RoleDoc`], written when the cluster becomes a standby
//!   and when it is promoted;
//! - [`FLAG_POSITION`]: [`Position`], the source entry the standby has
//!   applied, rewritten by EVERY mirrored entry in the same entry as the
//!   source's effects ([`mirror::mirror_entry`]). Applied state and position
//!   move together or not at all, which is what lets a new standby leader
//!   resume exactly where the last one stopped.
//!
//! Every entry a standby builds itself — becoming one, a promotion — is
//! stamped with the LAST applied `now_us`, never this node's clock: the
//! source's next entry carries the source's stamp, which is older than this
//! node's wall clock by the link's lag, and apply refuses time going
//! backwards.
//!
//! # What is not replayed
//!
//! The source's consensus-internal entries (a leader's blank entry, a
//! membership change) never leave it: they describe the source's raft group.
//! Inside an application entry two kinds of effect are cluster-local and are
//! replaced by [`Effect::Noop`] ([`mirror::is_cluster_local`]): a
//! [`Effect::MembershipNote`] (the identity of a SOURCE node; this cluster's
//! nodes carry the same node ids), and a [`Effect::FlagSet`] under
//! [`FLAG_PREFIX`] (the link rows of a source that was once a standby
//! itself).
//!
//! [`Keyspace::Flags`]: super::store::Keyspace::Flags

use super::effect::{CodecError, Effect, Reader, Writer};
use super::store::{Result as StoreResult, TypedReads};

pub mod driver;
pub mod mirror;
pub mod seed;
pub mod wire;

/// Every link row's flag name starts with this.
pub const FLAG_PREFIX: &str = "link/";
/// The flag holding the cluster's [`RoleDoc`]. Absent on a cluster that was
/// never part of a link.
pub const FLAG_ROLE: &str = "link/role";
/// The flag holding the standby's [`Position`].
pub const FLAG_POSITION: &str = "link/position";

/// Whether `name` is one of the link's own flags.
pub fn is_link_flag(name: &str) -> bool {
    name.starts_with(FLAG_PREFIX)
}

/// The value the promotion's REQUEST carries in the role flag: never a role
/// row (which is JSON), never written to the store.
const PROMOTE_REQUEST: &[u8] = b"promote";

/// The command that asks for a standby's promotion.
///
/// A promotion is asked of whichever node the operator reached and done by
/// the leader, so it travels the way every write does: as a command, which a
/// follower forwards under its request id and retries across a leader change.
/// It is an effects command with one effect no planner ever plans — the role
/// flag set to the request marker — which the batcher takes out of its queue
/// and turns into the promotion's entry ([`mirror::promote_entry`]).
pub fn promote_command(request_id: super::entry::RequestId) -> super::planner::EffectsCommand {
    super::planner::EffectsCommand {
        request_id,
        tenant: String::new(),
        effects: vec![Effect::FlagSet {
            key: FLAG_ROLE.to_string(),
            value: PROMOTE_REQUEST.to_vec(),
        }],
    }
}

/// Whether `c` is [`promote_command`]'s.
pub fn is_promote_command(c: &super::planner::EffectsCommand) -> bool {
    matches!(
        c.effects.as_slice(),
        [Effect::FlagSet { key, value }] if key == FLAG_ROLE && value == PROMOTE_REQUEST
    )
}

/// Whether `c` would write one of the link's rows. Only the batcher's own
/// link entries do: a command that carries one is refused, so no client, no
/// subsystem and no forwarded command can make a cluster a standby or move
/// its position.
pub fn writes_link_rows(c: &super::planner::EffectsCommand) -> bool {
    c.effects
        .iter()
        .any(|e| matches!(e, Effect::FlagSet { key, .. } if is_link_flag(key)))
}

/// The refusal code of every client write a standby is sent.
pub const STANDBY_CODE: &str = "standby";

/// What a standby answers a client write: retryable. A producer that keeps
/// retrying through a switch-over is served the moment the standby is
/// promoted; one that was pointed here by mistake sees the code in its log.
pub fn standby_refusal() -> super::planner::Refusal {
    super::planner::Refusal::retry(
        STANDBY_CODE,
        "this cluster is a standby: it replays its source and takes no writes until it is promoted",
    )
}

// ---------------------------------------------------------------------------
// Position
// ---------------------------------------------------------------------------

/// Where a standby is in its source's log: the last source entry it applied.
///
/// `index` and `term` are the SOURCE's (raft log matching makes the pair name
/// one entry of one log, so the source can tell a standby that followed
/// another log from one that is merely behind). `now_us` is that entry's
/// stamp: how far behind the standby is in time, without asking the source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    pub index: u64,
    pub term: u64,
    pub now_us: i64,
}

/// The one shape of a [`Position`] row this build writes.
const POSITION_V1: u8 = 1;

impl Position {
    /// The start of a source's log: nothing applied yet.
    pub const START: Position = Position {
        index: 0,
        term: 0,
        now_us: 0,
    };

    /// `version:u8 | index:u64 | term:u64 | now_us:i64`, little-endian.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(1 + 8 + 8 + 8);
        w.u8(POSITION_V1);
        w.u64(self.index);
        w.u64(self.term);
        w.i64(self.now_us);
        w.into_inner()
    }

    pub fn decode(b: &[u8]) -> Result<Position, CodecError> {
        let mut r = Reader::new(b);
        let version = r.u8("link position version")?;
        if version != POSITION_V1 {
            return Err(CodecError::Field("link position version"));
        }
        let p = Position {
            index: r.u64("link position index")?,
            term: r.u64("link position term")?,
            now_us: r.i64("link position now_us")?,
        };
        if !r.done() {
            return Err(CodecError::Field("link position trailing bytes"));
        }
        Ok(p)
    }

    /// The effect that records this position.
    pub fn effect(&self) -> Effect {
        Effect::FlagSet {
            key: FLAG_POSITION.to_string(),
            value: self.encode(),
        }
    }
}

// ---------------------------------------------------------------------------
// Role
// ---------------------------------------------------------------------------

/// What a cluster is with respect to a link, as its replicated state says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// Never part of a link: an ordinary cluster.
    Primary,
    /// Replaying `source`. Client writes are refused.
    Standby(RoleDoc),
    /// Was a standby and was promoted: an ordinary cluster again, for good.
    /// A promoted cluster does not become a standby again by configuration;
    /// only a seed (which replaces its data) makes it one.
    Promoted(RoleDoc),
}

impl Role {
    pub fn is_standby(&self) -> bool {
        matches!(self, Role::Standby(_))
    }

    /// `primary`, `standby` or `promoted`.
    pub fn name(&self) -> &'static str {
        match self {
            Role::Primary => "primary",
            Role::Standby(_) => "standby",
            Role::Promoted(_) => "promoted",
        }
    }
}

/// The [`FLAG_ROLE`] row. JSON, like every other flag: it is written twice in
/// a cluster's life and read by people.
///
/// ```json
/// {"role":"standby","id":"5e1f09c2","source":"queen-0.queen-headless:7400","atUs":0,"wallUs":1759900000000000}
/// {"role":"promoted","id":"5e1f09c2","source":"...","atUs":...,"wallUs":...,"position":{"index":41233,"term":7,"nowUs":...}}
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RoleDoc {
    /// `standby` or `promoted`.
    pub role: String,
    /// This standby's id ([`link_id`]): drawn once, when the cluster becomes a
    /// standby, and the same on every node since it is replicated state. It
    /// is what the source knows the standby by ([`reader_name`]), so the
    /// standby's hold on the source's log follows the standby's leadership,
    /// and two standbys of one source never share a hold.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Where the source was reached when the row was written: a label for
    /// people. The link follows its configuration, not this field.
    #[serde(default)]
    pub source: String,
    /// The stamp of the entry that wrote the row: the last stamp applied
    /// before it, which on a standby is the SOURCE's (a standby's own entries
    /// never read a clock). How far the cluster's state had got, not when
    /// the row was written.
    #[serde(default, rename = "atUs")]
    pub at_us: i64,
    /// When the row was written, by the wall clock of the node that planned
    /// it: for people. Nothing is ordered by it.
    #[serde(default, rename = "wallUs", skip_serializing_if = "is_zero")]
    pub wall_us: i64,
    /// `promoted` only: the last source entry applied before the promotion —
    /// everything the source committed after it is not in this cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<PositionDoc>,
}

/// A [`Position`] inside a [`RoleDoc`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PositionDoc {
    pub index: u64,
    pub term: u64,
    #[serde(rename = "nowUs")]
    pub now_us: i64,
}

impl From<Position> for PositionDoc {
    fn from(p: Position) -> PositionDoc {
        PositionDoc {
            index: p.index,
            term: p.term,
            now_us: p.now_us,
        }
    }
}

pub const ROLE_STANDBY: &str = "standby";
pub const ROLE_PROMOTED: &str = "promoted";

/// A standby's id, from the request id of the entry that made it one: the
/// random end of it (the start of a UUIDv7 is the time).
pub fn link_id(request_id: &super::entry::RequestId) -> String {
    request_id[12..].iter().map(|b| format!("{b:02x}")).collect()
}

/// What a standby calls itself when it reads its source: the name it was
/// given (`QUEEN_LINK_NAME`), else one made of its id.
pub fn reader_name(configured: Option<&str>, id: &str) -> String {
    match configured {
        Some(name) => name.to_string(),
        None if id.is_empty() => "standby".to_string(),
        None => format!("standby-{id}"),
    }
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

impl RoleDoc {
    pub fn standby(id: &str, source: &str, at_us: i64, wall_us: i64) -> RoleDoc {
        RoleDoc {
            role: ROLE_STANDBY.to_string(),
            id: id.to_string(),
            source: source.to_string(),
            at_us,
            wall_us,
            position: None,
        }
    }

    pub fn promoted(
        id: &str,
        source: &str,
        at_us: i64,
        wall_us: i64,
        position: Position,
    ) -> RoleDoc {
        RoleDoc {
            role: ROLE_PROMOTED.to_string(),
            id: id.to_string(),
            source: source.to_string(),
            at_us,
            wall_us,
            position: Some(position.into()),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a role document serializes")
    }

    /// The effect that records this role.
    pub fn effect(&self) -> Effect {
        Effect::FlagSet {
            key: FLAG_ROLE.to_string(),
            value: self.encode(),
        }
    }
}

/// The role a [`FLAG_ROLE`] row's bytes say. A row this build cannot read is
/// an error, never "primary": a standby that took its own role row for
/// nothing would start serving writes.
pub fn decode_role(row: Option<&[u8]>) -> Result<Role, String> {
    let Some(bytes) = row else {
        return Ok(Role::Primary);
    };
    let doc: RoleDoc = serde_json::from_slice(bytes)
        .map_err(|e| format!("the {FLAG_ROLE} flag does not parse: {e}"))?;
    match doc.role.as_str() {
        ROLE_STANDBY => Ok(Role::Standby(doc)),
        ROLE_PROMOTED => Ok(Role::Promoted(doc)),
        other => Err(format!(
            "the {FLAG_ROLE} flag names a role this build does not know: `{other}`"
        )),
    }
}

// ---------------------------------------------------------------------------
// Reading the link's rows
// ---------------------------------------------------------------------------

/// A store row of the link that this build cannot read.
fn unreadable(what: String) -> super::store::StoreError {
    super::store::StoreError::Io(what)
}

/// The cluster's role, from committed state.
pub fn read_role<R: TypedReads + ?Sized>(r: &R) -> StoreResult<Role> {
    let row = r.flag(FLAG_ROLE)?;
    decode_role(row.as_deref()).map_err(unreadable)
}

/// The standby's position, from committed state. [`Position::START`] when
/// none was ever written.
pub fn read_position<R: TypedReads + ?Sized>(r: &R) -> StoreResult<Position> {
    match r.flag(FLAG_POSITION)? {
        None => Ok(Position::START),
        Some(b) => Position::decode(&b)
            .map_err(|e| unreadable(format!("the {FLAG_POSITION} flag does not decode: {e:?}"))),
    }
}

// ---------------------------------------------------------------------------
// The standby's cursor
// ---------------------------------------------------------------------------

/// What the standby's driver knows about the state its next entry lands on:
/// the position, the three values apply will gate that entry on, and the
/// cluster version it may not exceed.
///
/// Read from committed state when a node starts leading a standby (a leader
/// plans only once it has applied an entry of its own term, so everything
/// before it is applied and the read is exact), then advanced by every entry
/// the driver PLANS — in flight or landed — so several mirrored entries can be
/// in the pipeline at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub position: Position,
    /// `meta.last_now_us` after everything planned so far.
    pub last_now_us: i64,
    /// `meta.next_pid` after everything planned so far.
    pub next_pid: u64,
    /// `meta.kv_version_next` after everything planned so far.
    pub kv_version_next: u64,
    /// `meta.cluster_version` after everything planned so far: the source's
    /// own raises ([`Effect::ClusterVersionSet`]) are replayed like any other
    /// effect, so the standby's version follows the source's.
    pub cluster_version: u32,
}

/// Why a source entry cannot be the standby's next entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    /// Not the entry after the standby's position: a duplicate, or one sent
    /// out of order. The sender reads the position again and resumes.
    OutOfSequence { position: Position, prev: u64 },
    /// The entry does not continue this cluster's state: the standby and its
    /// source have diverged. Nothing a retry can fix; the standby needs a new
    /// seed.
    Diverged(String),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refused::OutOfSequence { position, prev } => write!(
                f,
                "the entry follows source index {prev}, and this standby is at {}",
                position.index
            ),
            Refused::Diverged(why) => write!(f, "this standby has diverged from its source: {why}"),
        }
    }
}

impl Cursor {
    /// The cursor committed state holds.
    pub fn read<R: TypedReads + ?Sized>(r: &R) -> StoreResult<Cursor> {
        Ok(Cursor {
            position: read_position(r)?,
            last_now_us: r.last_now_us()?,
            next_pid: r.next_pid()?,
            kv_version_next: r.kv_version_next()?,
            cluster_version: r.cluster_version()?,
        })
    }

    /// The cluster version the source entry `e` raises this cluster to, if it
    /// carries a raise above the current one. Every member of the standby must
    /// read that version before the entry is proposed: the source's leader
    /// checked its own members, not these.
    pub fn raises(&self, e: &super::entry::Entry) -> Option<u32> {
        e.effects
            .iter()
            .filter_map(|eff| match eff {
                Effect::ClusterVersionSet { version } => Some(*version),
                _ => None,
            })
            .max()
            .filter(|v| *v > self.cluster_version)
    }

    /// Whether the source entry at `index`, which follows source index `prev`,
    /// is this standby's next entry — apply's own gates, asked before the
    /// entry is proposed.
    pub fn admit(&self, prev: u64, index: u64, e: &super::entry::Entry) -> Result<(), Refused> {
        if prev != self.position.index || index <= prev {
            return Err(Refused::OutOfSequence {
                position: self.position,
                prev,
            });
        }
        if e.now_us < self.last_now_us {
            return Err(Refused::Diverged(format!(
                "source entry {index} is stamped {} and this cluster has applied {}",
                e.now_us, self.last_now_us
            )));
        }
        if e.pid_base != self.next_pid {
            return Err(Refused::Diverged(format!(
                "source entry {index} assigns partition ids from {} and this cluster's next is {}",
                e.pid_base, self.next_pid
            )));
        }
        if e.kv_version_base != self.kv_version_next {
            return Err(Refused::Diverged(format!(
                "source entry {index} assigns KV versions from {} and this cluster's next is {}",
                e.kv_version_base, self.kv_version_next
            )));
        }
        // The source's leader never writes above its own cluster version, and
        // every raise of it was replayed before this entry.
        if e.kinds_version > self.cluster_version {
            return Err(Refused::Diverged(format!(
                "source entry {index} uses effect catalogue version {} and this cluster is at {}",
                e.kinds_version, self.cluster_version
            )));
        }
        Ok(())
    }

    /// Advance past the source entry at `index`/`term`, once it is planned.
    pub fn advance(&mut self, index: u64, term: u64, e: &super::entry::Entry) {
        use super::effect::Assigns;
        for eff in &e.effects {
            match eff.assigns() {
                Assigns::Pid(_) => self.next_pid += 1,
                Assigns::KvVersion(_) => self.kv_version_next += 1,
                Assigns::Nothing => {}
            }
            if let Effect::ClusterVersionSet { version } = eff {
                self.cluster_version = self.cluster_version.max(*version);
            }
        }
        self.last_now_us = e.now_us;
        self.position = Position {
            index,
            term,
            now_us: e.now_us,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_position_round_trips_and_refuses_another_shape() {
        let p = Position {
            index: 41_233,
            term: 7,
            now_us: 1_759_900_000_000_000,
        };
        assert_eq!(Position::decode(&p.encode()).unwrap(), p);
        let mut longer = p.encode();
        longer.push(0);
        assert!(Position::decode(&longer).is_err());
        let mut other = p.encode();
        other[0] = 2;
        assert!(Position::decode(&other).is_err());
        assert!(Position::decode(&[]).is_err());
    }

    #[test]
    fn a_role_row_that_does_not_parse_is_an_error_not_a_primary() {
        assert_eq!(decode_role(None).unwrap(), Role::Primary);
        let standby = RoleDoc::standby("5e1f09c2", "a:7400", 5, 1_759_900_000_000_000);
        assert_eq!(
            decode_role(Some(&standby.encode())).unwrap(),
            Role::Standby(standby)
        );
        let promoted = RoleDoc::promoted("5e1f09c2", "a:7400", 9, 0, Position::START);
        assert!(matches!(
            decode_role(Some(&promoted.encode())).unwrap(),
            Role::Promoted(_)
        ));
        assert!(decode_role(Some(b"{")).is_err());
        assert!(decode_role(Some(br#"{"role":"observer"}"#)).is_err());
    }

    #[test]
    fn a_standby_is_known_to_its_source_by_its_name_else_by_its_id() {
        let mut request_id = [0u8; 16];
        request_id[12..].copy_from_slice(&[0x5e, 0x1f, 0x09, 0xc2]);
        let id = link_id(&request_id);
        assert_eq!(id, "5e1f09c2");
        assert_eq!(reader_name(None, &id), "standby-5e1f09c2");
        assert_eq!(reader_name(Some("eu-west"), &id), "eu-west");
        // A role row without an id (none this build writes).
        assert_eq!(reader_name(None, ""), "standby");
    }
}
