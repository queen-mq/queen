//! At-rest payload encryption (RUSTFIX item 8) — AES-256-GCM, wire-compatible
//! with the C++ `EncryptionService` (server/src/services/encryption.cpp).
//!
//! Key: `QUEEN_ENCRYPTION_KEY` = exactly 64 hex chars → 32 bytes. The envelope
//! stores a 16-byte IV, but the C++ OpenSSL service never set the GCM IV length,
//! so it silently used only the FIRST 12 bytes (OpenSSL's default GCM nonce). We
//! match that: generate/store 16 random IV bytes but feed only `iv[..12]` to the
//! cipher (`Aes256Gcm` = `AesGcm<Aes256, U12>`). Tag is the default 16
//! bytes. The stored envelope is the JSON object
//! `{"encrypted":<b64(ct)>,"iv":<b64(iv)>,"authTag":<b64(tag)>}` (STANDARD base64
//! with padding, camelCase `authTag`) — byte-identical to push.cpp:382-405 so
//! messages encrypted by v0.16.0 decrypt here, and vice-versa.
//!
//! Failure mode matches C++: a missing/invalid key disables encryption (a
//! flagged queue then stores plaintext with a warning — it NEVER fails the push).

use std::sync::Arc;

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::Aes256Gcm;
use base64::{engine::general_purpose::STANDARD as B64, Engine};

pub struct Encryption {
    key: Option<[u8; 32]>,
}

impl Encryption {
    pub fn from_env() -> Arc<Encryption> {
        let hex = std::env::var("QUEEN_ENCRYPTION_KEY").unwrap_or_default();
        if hex.is_empty() {
            // No key configured: encryption disabled (a flagged queue stores plaintext).
            return Arc::new(Encryption { key: None });
        }
        if hex.len() != 64 {
            tracing::warn!(
                target: "encryption",
                len = hex.len(),
                "QUEEN_ENCRYPTION_KEY must be 64 hex chars; encryption DISABLED"
            );
            return Arc::new(Encryption { key: None });
        }
        let mut key = [0u8; 32];
        for i in 0..32 {
            match u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                Ok(b) => key[i] = b,
                Err(_) => {
                    tracing::warn!(
                        target: "encryption",
                        "QUEEN_ENCRYPTION_KEY is not valid hex; encryption DISABLED"
                    );
                    return Arc::new(Encryption { key: None });
                }
            }
        }
        tracing::info!(target: "encryption", "encryption service initialized (AES-256-GCM)");
        Arc::new(Encryption { key: Some(key) })
    }

    pub fn is_enabled(&self) -> bool {
        self.key.is_some()
    }

    fn cipher(&self) -> Option<Aes256Gcm> {
        let key = self.key.as_ref()?;
        Some(Aes256Gcm::new(GenericArray::from_slice(key)))
    }

    /// Encrypt `plaintext` → the envelope JSON bytes, or None if disabled / on any
    /// crypto failure (the caller falls back to plaintext).
    pub fn encrypt(&self, plaintext: &[u8]) -> Option<Vec<u8>> {
        let cipher = self.cipher()?;
        let iv: [u8; 16] = rand::random();
        // C++ OpenSSL used only the first 12 bytes of the IV (GCM default nonce
        // length); feed the same 12 while still storing all 16 in the envelope.
        let nonce = GenericArray::from_slice(&iv[..12]);
        // aes-gcm returns ciphertext || tag; C++ stores them separately.
        let mut ct = cipher.encrypt(nonce, plaintext).ok()?;
        if ct.len() < 16 {
            return None;
        }
        let tag = ct.split_off(ct.len() - 16);
        let env = format!(
            "{{\"encrypted\":\"{}\",\"iv\":\"{}\",\"authTag\":\"{}\"}}",
            B64.encode(&ct),
            B64.encode(iv),
            B64.encode(&tag)
        );
        Some(env.into_bytes())
    }

    /// Decrypt the three base64 envelope fields → plaintext bytes, or None on a
    /// tag-verify failure / disabled / bad sizes (require 16-byte IV + 16-byte tag).
    pub fn decrypt_envelope(&self, enc_b64: &str, iv_b64: &str, tag_b64: &str) -> Option<Vec<u8>> {
        let cipher = self.cipher()?;
        let ct = B64.decode(enc_b64).ok()?;
        let iv = B64.decode(iv_b64).ok()?;
        let tag = B64.decode(tag_b64).ok()?;
        if iv.len() != 16 || tag.len() != 16 {
            return None;
        }
        let mut combined = ct;
        combined.extend_from_slice(&tag);
        let nonce = GenericArray::from_slice(&iv[..12]);
        cipher.decrypt(nonce, combined.as_slice()).ok()
    }

    /// Envelope-sniff a stored payload: if `raw` is a JSON object carrying
    /// `encrypted`+`iv`+`authTag` string fields, decrypt it; else None (leave
    /// as-is). Matches the C++ pop/messages sniff (by key presence, not a flag),
    /// so migrated v0.16.0 envelopes decrypt regardless of the current queue flag.
    pub fn decrypt_payload_bytes(&self, raw: &[u8]) -> Option<Vec<u8>> {
        if !self.is_enabled() {
            return None;
        }
        let v: serde_json::Value = serde_json::from_slice(raw).ok()?;
        let obj = v.as_object()?;
        let enc = obj.get("encrypted")?.as_str()?;
        let iv = obj.get("iv")?.as_str()?;
        let tag = obj.get("authTag")?.as_str()?;
        self.decrypt_envelope(enc, iv, tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_enc() -> Encryption {
        // Fixed 32-byte key; construct directly (bypass env) so the test is hermetic.
        Encryption { key: Some([7u8; 32]) }
    }

    #[test]
    fn round_trip_and_sniff() {
        let e = test_enc();
        let pt = br#"{"hello":"world","n":42}"#;
        let env = e.encrypt(pt).expect("encrypt");
        // Envelope is valid JSON with a 16-byte iv (wire-compatible with v0.16.0).
        let v: serde_json::Value = serde_json::from_slice(&env).unwrap();
        let iv = B64.decode(v["iv"].as_str().unwrap()).unwrap();
        assert_eq!(iv.len(), 16, "envelope must store the full 16-byte IV");
        let tag = B64.decode(v["authTag"].as_str().unwrap()).unwrap();
        assert_eq!(tag.len(), 16, "GCM tag is 16 bytes");
        // Round-trips via both the field API and the envelope-sniff API.
        let dec = e.decrypt_payload_bytes(&env).expect("decrypt");
        assert_eq!(&dec, pt);
    }

    #[test]
    fn wrong_key_fails_tag_verify() {
        let a = test_enc();
        let env = a.encrypt(b"secret").unwrap();
        let b = Encryption { key: Some([9u8; 32]) };
        assert!(b.decrypt_payload_bytes(&env).is_none());
    }

    // NOTE: a cross-implementation known-answer test against a REAL v0.16.0
    // envelope (a captured {encrypted,iv,authTag} + its plaintext under a fixed
    // key) should be added when such a vector is available — it is the only way to
    // pin the "C++ used iv[..12]" premise this module now depends on.
}
