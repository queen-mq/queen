#!/usr/bin/env bash
#
# Does the I2 pin actually bite? (PLAN_RAFT.md I2, WP-1.4)
#
# I2 says apply is a pure function of (committed state, entry): no clock, no
# randomness, no environment. `server/clippy.toml` lists the calls that make
# that false and `#![deny(clippy::disallowed_methods)]` at the top of
# `rsm/apply.rs`, `rsm/state/mod.rs` and `rsm/store/mod.rs` is what turns the
# list into an error. Both halves are easy to get wrong in ways that are
# SILENT: a path that names nothing the crate links (`rand::thread_rng` is a
# re-export, and clippy resolves definitions) enforces nothing, and a `deny`
# that is never exercised is a comment.
#
# So: put one call of every listed method into `rsm/apply.rs`, run clippy, and
# require an error for each. The file is restored on every exit path, including
# a failure and a ^C.
#
# Usage: test/raft/lint/deny-bites.sh
# Exit:  0 every entry bit; 1 one did not; 2 the harness itself could not run.
set -u -o pipefail

here=$(cd "$(dirname "$0")" && pwd)
server=$(cd "$here/../../../server" && pwd)
target="$server/src/rsm/apply.rs"
backup=$(mktemp "${TMPDIR:-/tmp}/deny-bites.XXXXXX") || exit 2

restore() {
    if [ -f "$backup" ]; then
        cp "$backup" "$target"
        rm -f "$backup"
    fi
}
trap restore EXIT INT TERM

cp "$target" "$backup" || exit 2

# One call per entry of clippy.toml. The name on the left is what clippy
# reports; `rand::thread_rng` is written as a caller would write it, which is
# the point: the pin must catch the re-export spelling.
cat >> "$target" <<'CANARY'

// ---------------------------------------------------------------------------
// I2 canary, appended by test/raft/lint/deny-bites.sh and removed by it.
// ---------------------------------------------------------------------------
#[allow(dead_code)]
fn i2_canary() -> u64 {
    let _ = std::time::SystemTime::now();
    let _ = Instant::now();
    let _ = std::env::var("QUEEN_RAFT_DIR");
    let _ = std::env::var_os("QUEEN_RAFT_DIR");
    let _ = std::env::vars().count();
    let _ = rand::thread_rng();
    rand::random::<u64>()
}
CANARY

out=$(cd "$server" && cargo clippy -p queen-engine --lib --message-format short 2>&1)
status=0
for m in \
    'std::time::SystemTime::now' \
    'std::time::Instant::now' \
    'std::env::var' \
    'std::env::var_os' \
    'std::env::vars' \
    'rand::rngs::thread::thread_rng' \
    'rand::random'
do
    if printf '%s\n' "$out" | grep -q "use of a disallowed method \`$m\`"; then
        echo "ok    $m"
    else
        echo "FAIL  $m — listed in clippy.toml, but clippy did not refuse it"
        status=1
    fi
done

if printf '%s\n' "$out" | grep -q 'does not refer to a reachable'; then
    printf '%s\n' "$out" | grep 'does not refer to a reachable'
    echo "FAIL  an entry of clippy.toml resolves to nothing: it enforces nothing"
    status=1
fi

if [ "$status" -eq 0 ]; then
    echo "the I2 pin bites: 7 methods refused inside rsm/apply.rs"
fi
exit "$status"
