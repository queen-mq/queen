use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64URL, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// HS256 JWT machinery matching the C++ per-request auth work: verify HMAC-SHA256
/// signature + decode & check claims. Verifies a server-built token per request
/// to measure the CPU cost (the goload loader sends no token).
pub struct Auth {
    secret: Vec<u8>,
    token: String,
}

impl Auth {
    pub fn new(secret: &str) -> Option<Auth> {
        if secret.is_empty() {
            return None;
        }
        let mut a = Auth { secret: secret.as_bytes().to_vec(), token: String::new() };
        a.token = a.build();
        Some(a)
    }

    fn build(&self) -> String {
        let hdr = B64URL.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let exp = now() + 3600;
        let claims = format!(r#"{{"sub":"bench-producer","scope":"read_write","exp":{},"iat":{}}}"#, exp, now());
        let payload = B64URL.encode(claims.as_bytes());
        let signing = format!("{hdr}.{payload}");
        let mut mac = HmacSha256::new_from_slice(&self.secret).unwrap();
        mac.update(signing.as_bytes());
        let sig = B64URL.encode(mac.finalize().into_bytes());
        format!("{signing}.{sig}")
    }

    pub fn verify(&self, token: &str) -> bool {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return false;
        }
        let signing = format!("{}.{}", parts[0], parts[1]);
        let mut mac = HmacSha256::new_from_slice(&self.secret).unwrap();
        mac.update(signing.as_bytes());
        let sig = match B64URL.decode(parts[2]) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if mac.verify_slice(&sig).is_err() {
            return false;
        }
        let claims_raw = match B64URL.decode(parts[1]) {
            Ok(c) => c,
            Err(_) => return false,
        };
        // parse exp cheaply
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&claims_raw) {
            if let Some(exp) = v.get("exp").and_then(|e| e.as_i64()) {
                return exp > now();
            }
        }
        false
    }

    pub fn check(&self) -> bool {
        self.verify(&self.token)
    }
}

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}
