//! The entries a standby builds: a source entry as the standby proposes it,
//! and the two entries of its own (becoming a standby, a promotion).

use super::super::effect::{CodecError, Effect};
use super::super::entry::{catalogue_version_of, Entry, Outcome, RequestId};
use super::{is_link_flag, link_id, Cursor, Position, RoleDoc};

/// Whether an effect describes the cluster it was planned in and nothing a
/// standby may take over (see the module header of [`super`]).
pub fn is_cluster_local(e: &Effect) -> bool {
    match e {
        Effect::MembershipNote { .. } => true,
        Effect::FlagSet { key, .. } => is_link_flag(key),
        _ => false,
    }
}

/// The standby's entry for the source entry `src`, which sat at `index` /
/// `term` in the source's log.
///
/// The header is the source's, untouched: its stamp and its bases are what the
/// effects were planned against, and what apply gates on. The commands and
/// their outcomes are the source's too, so the standby records the same
/// request ids. The effects are the source's in the source's order, a
/// cluster-local one replaced by [`Effect::Noop`] IN PLACE (every command's
/// span stays what it was), followed by one more: the [`Position`] this entry
/// brings the standby to. That last effect joins the span of the command the
/// source's effects end with — no command of its own, so the standby's
/// request-id rows stay the source's, row for row.
///
/// An application entry always carries a command (the leader builds an entry
/// only when one was logged); a source entry without one is refused.
pub fn mirror_entry(src: &Entry, index: u64, term: u64) -> Result<Entry, CodecError> {
    let n = src.effects.len() as u32;
    let mut commands = src.commands.clone();
    let last = commands
        .iter_mut()
        .find(|c| c.first_effect + c.effect_count == n)
        .ok_or(CodecError::Layout(
            "a source entry with no command to carry the link position",
        ))?;
    last.effect_count += 1;

    let mut effects: Vec<Effect> = Vec::with_capacity(src.effects.len() + 1);
    for e in &src.effects {
        if is_cluster_local(e) {
            effects.push(Effect::Noop);
        } else {
            effects.push(e.clone());
        }
    }
    effects.push(
        Position {
            index,
            term,
            now_us: src.now_us,
        }
        .effect(),
    );

    let entry = Entry {
        format: src.format,
        // What the entry carries NOW: a replaced effect may have been the one
        // that set the source's value, and an overstated header would be held
        // back by the cluster-version gate for nothing.
        kinds_version: catalogue_version_of(&commands, &effects),
        now_us: src.now_us,
        pid_base: src.pid_base,
        kv_version_base: src.kv_version_base,
        commands,
        effects,
    };
    entry.validate()?;
    Ok(entry)
}

/// An entry of the standby's own: `effects` as one command, on the state
/// `cursor` describes. Stamped with the last applied stamp, never a clock (the
/// module header of [`super`] says why), and assigning nothing from the bases.
fn own_entry(
    cursor: &Cursor,
    request_id: RequestId,
    effects: Vec<Effect>,
) -> Result<Entry, CodecError> {
    let mut e = Entry::new(cursor.last_now_us, cursor.next_pid, cursor.kv_version_next);
    e.add_command(request_id, Outcome::Empty, effects)?;
    e.validate()?;
    Ok(e)
}

/// The entry that makes this cluster a standby of `source`, at `position` in
/// the source's log. The standby's id is drawn from the entry's request id
/// ([`link_id`]). `wall_us`: the planning node's wall clock, written into the
/// role row for people and used for nothing.
pub fn standby_entry(
    cursor: &Cursor,
    request_id: RequestId,
    source: &str,
    position: Position,
    wall_us: i64,
) -> Result<Entry, CodecError> {
    own_entry(
        cursor,
        request_id,
        vec![
            RoleDoc::standby(&link_id(&request_id), source, cursor.last_now_us, wall_us).effect(),
            position.effect(),
        ],
    )
}

/// The entry that promotes the standby `id`: the role row names the last
/// source entry applied before it, and the position row stays, so what the
/// cluster held when it was promoted can be read afterwards.
pub fn promote_entry(
    cursor: &Cursor,
    request_id: RequestId,
    id: &str,
    source: &str,
    wall_us: i64,
) -> Result<Entry, CodecError> {
    own_entry(
        cursor,
        request_id,
        vec![
            RoleDoc::promoted(id, source, cursor.last_now_us, wall_us, cursor.position).effect(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::super::{decode_role, Role, FLAG_POSITION, FLAG_ROLE};
    use super::*;
    use crate::rsm::entry::{decode_entry, encode_entry};

    fn id(n: u8) -> RequestId {
        [n; 16]
    }

    fn flag(key: &str) -> Effect {
        Effect::FlagSet {
            key: key.to_string(),
            value: br#"{"enabled":true}"#.to_vec(),
        }
    }

    fn note() -> Effect {
        Effect::MembershipNote {
            node_id: 2,
            generation: 4,
            disk_uuid: [7; 16],
            address: "queen-1.queen-headless:7400".to_string(),
        }
    }

    /// Two commands: the first sets a flag and writes a membership note, the
    /// second sets a link flag (a source that was once a standby) and a flag.
    fn source() -> Entry {
        let mut e = Entry::new(1_000, 16, 3);
        e.add_command(id(1), Outcome::Empty, vec![flag("kv_enabled"), note()])
            .unwrap();
        e.add_command(id(2), Outcome::Empty, vec![flag(FLAG_ROLE), flag("x")])
            .unwrap();
        e
    }

    #[test]
    fn a_mirrored_entry_keeps_the_header_the_commands_and_the_effect_order() {
        let src = source();
        let m = mirror_entry(&src, 41, 7).expect("mirror");

        assert_eq!(
            (m.now_us, m.pid_base, m.kv_version_base),
            (1_000, 16, 3),
            "the header is the source's"
        );
        assert_eq!(m.commands.len(), 2, "no command of the standby's own");
        assert_eq!(m.commands[0], src.commands[0]);
        assert_eq!(m.commands[1].request_id, id(2));
        assert_eq!(
            (m.commands[1].first_effect, m.commands[1].effect_count),
            (2, 3),
            "the position joins the last command's span"
        );

        assert_eq!(m.effects.len(), 5);
        assert_eq!(m.effects[0], flag("kv_enabled"));
        assert_eq!(m.effects[1], Effect::Noop, "a source node's identity");
        assert_eq!(m.effects[2], Effect::Noop, "a source's own link row");
        assert_eq!(m.effects[3], flag("x"));
        assert_eq!(
            m.effects[4],
            Effect::FlagSet {
                key: FLAG_POSITION.to_string(),
                value: Position {
                    index: 41,
                    term: 7,
                    now_us: 1_000
                }
                .encode(),
            }
        );

        // What is proposed is what a node reads back.
        let bytes = encode_entry(&m).expect("encode");
        assert_eq!(decode_entry(&bytes).expect("decode"), m);
    }

    #[test]
    fn the_position_joins_the_command_whose_span_ends_the_effects_wherever_it_sits() {
        // The second command's effects come FIRST (lanes order their parts by
        // lane, not by command).
        let mut src = Entry::new(5, 0, 0);
        src.add_command(id(1), Outcome::Empty, vec![flag("a")])
            .unwrap();
        src.add_command(id(2), Outcome::Empty, vec![flag("b")])
            .unwrap();
        src.commands.swap(0, 1);
        src.validate().expect("still a valid entry");

        let m = mirror_entry(&src, 9, 1).expect("mirror");
        let carrier = m
            .commands
            .iter()
            .find(|c| c.request_id == id(2))
            .expect("the command at the end of the effects");
        assert_eq!((carrier.first_effect, carrier.effect_count), (1, 2));
    }

    #[test]
    fn an_entry_without_a_command_is_refused() {
        assert!(mirror_entry(&Entry::noop(), 3, 1).is_err());
    }

    #[test]
    fn the_standbys_own_entries_take_the_last_stamp_and_assign_nothing() {
        let cursor = Cursor {
            position: Position {
                index: 41,
                term: 7,
                now_us: 900,
            },
            last_now_us: 900,
            next_pid: 16,
            kv_version_next: 3,
            cluster_version: 4,
        };
        let start = standby_entry(&cursor, id(9), "a:7400", Position::START, 5_000).unwrap();
        assert_eq!(
            (start.now_us, start.pid_base, start.kv_version_base),
            (900, 16, 3)
        );
        let Effect::FlagSet { key, value } = &start.effects[0] else {
            panic!("the role row comes first");
        };
        assert_eq!(key, FLAG_ROLE);
        let Role::Standby(doc) = decode_role(Some(value)).unwrap() else {
            panic!("a standby role");
        };
        assert_eq!(doc.id, "09090909", "the id is the request id's end");
        assert_eq!(
            (doc.at_us, doc.wall_us),
            (900, 5_000),
            "the stamp is the state's; the wall clock is only written down"
        );

        let promote = promote_entry(&cursor, id(10), &doc.id, "a:7400", 6_000).unwrap();
        assert_eq!(promote.now_us, 900);
        let Effect::FlagSet { value, .. } = &promote.effects[0] else {
            panic!("the role row");
        };
        let Role::Promoted(promoted) = decode_role(Some(value)).unwrap() else {
            panic!("a promoted role");
        };
        assert_eq!(promoted.position.unwrap().index, 41);
        assert_eq!(promoted.id, doc.id, "a promotion keeps the standby's id");
    }
}
