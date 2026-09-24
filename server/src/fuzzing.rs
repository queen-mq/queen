//! W7 fuzz entry points (PLAN_SINGLE_BINARY.md): the broker's untrusted
//! decoders, each as `fn(&[u8])` that must never panic. `server/fuzz`
//! (cargo-fuzz, nightly) drives them; the seed-corpus tests at the bottom run
//! the same functions on stable, so a target that stops compiling or starts
//! panicking on a known input fails `cargo test`.
//!
//! Compiled only for tests and with the `fuzzing` feature (lib.rs).

use serde_json::Value;

use crate::frames::{pack_frames, unpack_frames, unpack_frames_ref, FrameIn};

/// A raft push body (`POST /api/v1/push` in raft mode): the real `PushBodyIn`
/// decode, the facade's name and transaction-id checks, then one frame per
/// admitted item packed and unpacked — which must be the identity. The
/// round-trip assertion is what caught the u16 transaction-id truncation.
pub fn push_body(data: &[u8]) {
    use crate::rsm::facade::real::{txn_too_long, PushBodyIn};
    let Ok(body) = serde_json::from_slice::<PushBodyIn>(data) else {
        return;
    };
    for it in &body.items {
        let partition = it
            .partition
            .as_deref()
            .filter(|p| !p.is_empty())
            .unwrap_or("Default");
        if crate::rsm::facade::check_message_key_names(
            crate::config::DEFAULT_TENANT,
            &it.queue,
            None,
            Some(partition),
        )
        .is_err()
        {
            continue;
        }
        if txn_too_long(it.transaction_id.as_deref()) {
            continue; // refused at the boundary (RsmError::Rejected)
        }
        let txn = it.transaction_id.as_deref().unwrap_or("");
        let payload = it.payload.get().as_bytes();
        let frame = pack_frames(&[FrameIn {
            message_id: [7; 16],
            txn,
            trace_id: it
                .trace_id
                .as_deref()
                .and_then(crate::frames::uuid_string_to_bytes),
            producer_sub: Some("fuzz-sub"),
            payload,
            encrypted: false,
        }]);
        let back = unpack_frames_ref(&frame).expect("a packed frame unpacks");
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].txn, txn, "transaction id survives the frame codec");
        assert_eq!(back[0].payload, payload, "payload survives the frame codec");
        assert_eq!(back[0].producer_sub, Some("fuzz-sub"));
    }
}

/// Stored/replicated segment frames: both unpackers on arbitrary bytes.
pub fn frames(data: &[u8]) {
    let _ = unpack_frames_ref(data);
    let _ = unpack_frames(data);
}

/// The ops array of a KV batch (`POST /api/v1/kv/batch`: a bare array or
/// `{"operations": [...]}`) and of a transaction's KV rider.
pub fn kv_ops(data: &[u8]) {
    let Some(ops) = ops_of(data) else { return };
    let _ = crate::rsm::planner::kv::parse_ops(&ops, crate::config::DEFAULT_TENANT, false, 511);
    let _ = crate::rsm::planner::kv::parse_ops(&ops, crate::config::DEFAULT_TENANT, true, 511);
}

/// The timer ops of a transaction / timer request.
pub fn timer_ops(data: &[u8]) {
    let Some(ops) = ops_of(data) else { return };
    let _ = crate::rsm::planner::timers::parse_timer_ops(&ops, None);
    let _ = crate::rsm::planner::timers::parse_timer_ops(&ops, Some("fuzz-sub"));
}

fn ops_of(data: &[u8]) -> Option<Vec<Value>> {
    match serde_json::from_slice::<Value>(data).ok()? {
        Value::Array(a) => Some(a),
        Value::Object(mut o) => match o.remove("operations") {
            Some(Value::Array(a)) => Some(a),
            _ => None,
        },
        _ => None,
    }
}

/// A raft log entry as it arrives from a peer's socket or a crash-truncated
/// file: every entry/effect decoder on the same bytes.
pub fn raft_entry(data: &[u8]) {
    let _ = crate::rsm::entry::parse_entry_header(data);
    let _ = crate::rsm::entry::decode_entry(data);
    let _ = crate::rsm::entry::decode_entry_at(data);
    let _ = crate::rsm::effect::decode_effect(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzz_seed_corpus_push_body() {
        let long_txn = format!(
            r#"{{"items":[{{"queue":"q","payload":1,"transactionId":"{}"}}]}}"#,
            "t".repeat(u16::MAX as usize + 1)
        );
        let edge_txn = format!(
            r#"{{"items":[{{"queue":"q","payload":1,"transactionId":"{}"}}]}}"#,
            "t".repeat(u16::MAX as usize)
        );
        for seed in [
            r#"{"items":[{"queue":"orders","payload":{"id":1}}]}"#,
            r#"{"items":[{"queue":"o","partition":"eu","payload":"x","transactionId":"Bed&Breakfast-771"}]}"#,
            r#"{"items":[{"queue":"o","payload":[1,2,3],"traceId":"0190a3c4-1111-7000-8000-000000000001"}]}"#,
            r#"{"items":[{"queue":"o","partition":"","payload":null,"transactionId":""}]}"#,
            r#"{"items":[{"queue":"a\u0000b","payload":1}]}"#,
            r#"{"items":[]}"#,
            r#"{"items":[{"queue":"q"}]}"#,
            r#"not json"#,
            long_txn.as_str(),
            edge_txn.as_str(),
        ] {
            push_body(seed.as_bytes());
        }
    }

    #[test]
    fn fuzz_seed_corpus_frames() {
        let good = pack_frames(&[FrameIn {
            message_id: [1; 16],
            txn: "t",
            trace_id: Some([2; 16]),
            producer_sub: Some("s"),
            payload: b"{}",
            encrypted: true,
        }]);
        frames(&good);
        for cut in 0..good.len() {
            frames(&good[..cut]);
        }
        let mut lie = good.clone();
        lie[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        frames(&lie);
        frames(&[]);
        frames(&[0xff; 64]);
    }

    #[test]
    fn fuzz_seed_corpus_kv_and_timer_ops() {
        for seed in [
            r#"[{"op":"put","ns":"app","key":"k1","value":{"a":1},"ttlSeconds":60}]"#,
            r#"{"operations":[{"op":"incr","ns":"ctr","key":"hits","delta":1.5,"max":100,"forever":true}]}"#,
            r#"[{"op":"getPrefix","ns":"app","prefix":"user:","limit":10,"keysOnly":true}]"#,
            r#"[{"op":"put","ns":"a\u0000","key":"","value":1e999}]"#,
            r#"[{"op":"cas","ns":"a","key":"k","expected":null,"value":"v"}]"#,
            r#"[{"type":"schedule","queue":"q","payload":{"x":1},"fireAt":"2026-09-24T00:00:00Z"}]"#,
            r#"[{"type":"cancel","timerId":"nope"}]"#,
            r#"[1,"x",null,{}]"#,
            r#"{"operations":7}"#,
            "",
        ] {
            kv_ops(seed.as_bytes());
            timer_ops(seed.as_bytes());
        }
    }

    #[test]
    fn fuzz_seed_corpus_raft_entry() {
        let mut e = crate::rsm::entry::Entry::new(1, 1, 1);
        e.add_command(
            [9; 16],
            crate::rsm::entry::Outcome::Empty,
            vec![crate::rsm::effect::Effect::Noop],
        )
        .expect("a sample entry");
        let good = crate::rsm::entry::encode_entry(&e).expect("encode");
        raft_entry(&good);
        for cut in 0..good.len() {
            raft_entry(&good[..cut]);
        }
        let mut flipped = good.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0x55;
        raft_entry(&flipped);
        raft_entry(&[0; 64]);
        raft_entry(&[0xff; 64]);
    }
}
