use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Key};
use base64::{engine::general_purpose::STANDARD as B64, Engine};

/// AES-256-GCM payload encryption (C++ EncryptionService wire format:
/// {encrypted, iv, authTag} base64). Real per-message crypto CPU.
pub struct Crypto {
    cipher: Aes256Gcm,
}

impl Crypto {
    pub fn new(hex_key: &str) -> Option<Crypto> {
        if hex_key.len() != 64 {
            return None;
        }
        let key_bytes = hex::decode(hex_key).ok()?;
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        Some(Crypto { cipher: Aes256Gcm::new(key) })
    }

    /// Returns the {encrypted,iv,authTag} object bytes for a plaintext payload.
    pub fn encrypt_payload(&self, plaintext: &[u8]) -> Vec<u8> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 12 bytes
        let sealed = self.cipher.encrypt(&nonce, plaintext).unwrap_or_default();
        let (ct, tag) = if sealed.len() >= 16 {
            sealed.split_at(sealed.len() - 16)
        } else {
            (&sealed[..], &sealed[..])
        };
        format!(
            "{{\"encrypted\":\"{}\",\"iv\":\"{}\",\"authTag\":\"{}\"}}",
            B64.encode(ct),
            B64.encode(nonce.as_slice()),
            B64.encode(tag)
        )
        .into_bytes()
    }

    pub fn decrypt(&self, enc_b64: &str, iv_b64: &str, tag_b64: &str) -> Option<Vec<u8>> {
        let ct = B64.decode(enc_b64).ok()?;
        let iv = B64.decode(iv_b64).ok()?;
        let tag = B64.decode(tag_b64).ok()?;
        let mut sealed = ct;
        sealed.extend_from_slice(&tag);
        let nonce = GenericArray::from_slice(&iv);
        self.cipher.decrypt(nonce, sealed.as_slice()).ok()
    }
}
