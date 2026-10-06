# `src/tests_unit/` — unit tests kept in their own files

Each file here is the body of a `#[cfg(test)] mod` that belongs to another module. The owner
wires it in with a `#[path]` attribute, so the test reaches the owner's private items through
`use super::*` without making them `pub`.

| file | owner module | wiring block |
|---|---|---|
| `quota_gate.rs` | `src/quota.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/quota_gate.rs"]`<br>`mod quota_gate_tests;` |
| `quota_overshoot.rs` | `src/quota.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/quota_overshoot.rs"]`<br>`mod quota_overshoot_tests;` |
| `switch_levels.rs` | `src/switches.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/switch_levels.rs"]`<br>`mod switch_levels_tests;` |
| `ephemeral_engine.rs` | `src/ephemeral.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/ephemeral_engine.rs"]`<br>`mod ephemeral_engine_tests;` |
| `ephemeral_handover.rs` | `src/handlers/ephemeral.rs` | `#[cfg(all(test, feature = "server"))]`<br>`#[path = "../tests_unit/ephemeral_handover.rs"]`<br>`mod ephemeral_handover_tests;` |
| `pop_commit_on_delivery.rs` | `src/handlers/data.rs` | `#[cfg(test)]`<br>`#[path = "../tests_unit/pop_commit_on_delivery.rs"]`<br>`mod pop_commit_on_delivery_tests;` |

`quota_overshoot.rs` is the one to read first if you are new to the quota: it runs the real gate
under both the rejected design and the shipped one and measures the difference (PLAN_KV_TIMERS
§9.3).

`#[path]` on a non-inline `mod` resolves **relative to the directory of the file that declares
it**, i.e. `src/`, and the module is a child of the declaring module.
