"""The crash-point catalogue of PLAN_RAFT.md §13.5.

A point is a named place in the broker where `rsm/faults.rs` may kill the
process (or return an error, or stall) when `QUEEN_TEST_FAULTS` names it. The
wire format is the one pgless used, because the harness and the broker have to
agree on it before either exists:

    QUEEN_TEST_FAULTS="apply.segment_written:2,gc.before_unlink"

    point            fire the first time control reaches it
    point:nth        fire the nth time (1-based)

Off unless set. §0.3: `QUEEN_NATIVE_*` knobs belong to pgless and mean nothing
here; the fault variable keeps its name because it is a TEST knob, not a
storage one.

This file is the SPEC SIDE of that contract: it exists before rsm/faults.rs and
is what WP-1.x implements against. `crashdrv.py --list-points` prints it, and
`--verify-points <binary>` (stub) will one day diff it against the strings the
binary actually registers, so a renamed point cannot silently stop being tested.

Each point carries:
  group     the §13.5 grouping, which is also the order they are printed in
  stage     what has happened when it fires, in one line — the thing a reader
            needs to predict what recovery must do
  invariant the plan invariant it is there to test (I-ids of §4)
  topology  which topologies can arm it: raft1, raft3, or both
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Point:
    name: str
    group: str
    stage: str
    invariant: str
    topology: tuple = ("raft1", "raft3")
    # The phase whose WP wires this point into a real code path. A point above
    # the current phase is a valid §13.5 name whose code does not exist yet, so
    # the broker of an earlier phase refuses to arm it (rsm/faults.rs exits 2).
    # WP-1.8 wires the 13 phase-1 points; the rest are named here for the
    # catalogue and set to the phase that owns them.
    phase: int = 1


POINTS = [
    # --- batching and planning -------------------------------------------
    Point("batcher.drained", "batching and planning",
          "commands are out of the channel and in the cycle; nothing is planned yet",
          "I3, I14"),
    Point("planner.planned", "batching and planning",
          "effects and outcomes exist in the overlay; nothing is proposed",
          "I1, I3"),
    Point("propose.sent", "batching and planning",
          "the entry is with the replicator; the client is still waiting and the outcome is UNKNOWN (D6)",
          "I4, I6"),
    # --- log and apply ----------------------------------------------------
    Point("log.appended", "log and apply",
          "the entry is in the local raft log, not yet flushed",
          "I4"),
    Point("log.flushed", "log and apply",
          "the entry is durable in the raft log; it may or may not be committed",
          "I4"),
    Point("commit.before_apply", "log and apply",
          "the entry is committed by quorum and not yet applied here",
          "I4, I13"),
    Point("apply.mid_entry", "log and apply",
          "some effects of one entry are applied, the rest are not: the atomicity test",
          "I1, I11"),
    Point("apply.segment_written", "log and apply",
          "payload bytes are in a segment file, the store commit has not happened",
          "I11"),
    Point("apply.store_committed", "log and apply",
          "the store commit carrying the applied index landed; files may be unsynced",
          "I11"),
    # --- qlog WAL durability (ALICE_PGLESS_NEWARCH.md §5, Phase A3a) -------
    # NOT §13.5: these fire only with QUEEN_RAFT_QLOG on, inside the store
    # commit's qlog block, and prove the per-queue qlog is durable AT the store
    # commit (so A3b can remove the raft-log payload). raft1 only in phase 1.
    Point("qlog.record_written", "qlog WAL durability",
          "this commit's qlog records are on the page cache; the qlog fsync has NOT run",
          "NA-QLOG-I1", ("raft1", "raft3")),
    Point("qlog.record_fsynced", "qlog WAL durability",
          "the qlog is fsynced through this commit's records; the store commit has NOT landed",
          "NA-QLOG-I1", ("raft1", "raft3")),
    # --- durable points ---------------------------------------------------
    Point("durable.files_synced", "durable points",
          "segment files are fsynced, the store commit that records their lengths has not landed",
          "I11"),
    Point("durable.store_committed", "durable points",
          "the durable point is complete; the next entries are not durable yet",
          "I11"),
    # --- snapshots --------------------------------------------------------
    Point("snapshot.sealed", "snapshots",
          "the snapshot's index and file set are fixed; nothing is copied",
          "I10, I17", phase=4),
    Point("snapshot.built", "snapshots",
          "the snapshot exists locally and is referenced by its manifest",
          "I10, I17", phase=4),
    Point("snapshot.file_sent", "snapshots",
          "one file of a transfer reached the receiver; the rest did not",
          "I17", ("raft3",), phase=4),
    Point("snapshot.install_staged", "snapshots",
          "sm-<index>-<term>/ is written and verified; CURRENT still names the old directory",
          "I17", ("raft3",), phase=4),
    Point("snapshot.install_activated", "snapshots",
          "CURRENT names the new directory; the old one is not yet reclaimed",
          "I17", ("raft3",), phase=4),
    # --- GC and compaction ------------------------------------------------
    Point("gc.before_unlink", "GC and compaction",
          "a store commit no longer references the file; the file is still on disk",
          "I10"),
    Point("gc.after_unlink", "GC and compaction",
          "the file is unlinked; a reader or a manifest may still believe in it",
          "I10"),
    Point("compaction.copied", "GC and compaction",
          "live frames are copied into a new file; positions still point at the old one",
          "I7, I10", phase=2),
    Point("compaction.loc_committed", "GC and compaction",
          "positions point at the new file; the old file is not yet unlinked",
          "I7, I10", phase=2),
    # --- leadership, identity, membership ---------------------------------
    Point("transfer.started", "leadership, identity, membership",
          "a leadership transfer is in flight; the old leader must plan nothing more",
          "I3, I13", ("raft3",), phase=4),
    Point("identity.written", "leadership, identity, membership",
          "IDENTITY is on disk; the membership record may not know this generation",
          "I9", phase=4),
    Point("membership.learner_added", "leadership, identity, membership",
          "the learner is in the membership; it has caught up with nothing",
          "I9", ("raft3",), phase=4),
    Point("membership.changed", "leadership, identity, membership",
          "the new voter set is committed; the old leader may still think it leads",
          "I9, I13", ("raft3",), phase=4),
]

BY_NAME = {p.name: p for p in POINTS}

GROUPS = []
for _p in POINTS:
    if _p.group not in GROUPS:
        GROUPS.append(_p.group)


def points_for(topology: str):
    """The points that can be armed in a topology ("raft1" or "raft3")."""
    if topology not in ("raft1", "raft3"):
        raise ValueError(f"unknown topology {topology!r} (raft1, raft3)")
    return [p for p in POINTS if topology in p.topology]


def spec(name: str, nth: int | None = None) -> str:
    """The QUEEN_TEST_FAULTS spec for one point, validated against the catalogue."""
    if name not in BY_NAME:
        raise KeyError(f"unknown crash point {name!r} (crashdrv.py --list-points)")
    if nth is None:
        return name
    if nth < 1:
        raise ValueError("nth is 1-based")
    return f"{name}:{nth}"
