//! A credential reduced to something a long-lived map may hold.
//!
//! Two of this facade's maps are filed by "which credential is this": the
//! queue-list cache ([`crate::queen::Catalog`]) and the group registry
//! ([`crate::coordinator`]). Both are process-wide and both outlive the
//! connection that first inserted into them.
//!
//! Keyed by the raw bearer token — which is what a SASL/PLAIN password IS here
//! ([`crate::sasl`]) — those maps keep every credential the facade has ever
//! been shown, valid or refused, for the life of the process: in the heap, in
//! any core dump taken from it, and in whatever a future `Debug` prints. A
//! REFUSED password is the worse half of that: it is a secret the facade has no
//! use for at all, usually one character away from a real one, and it arrives
//! from anyone who can open a socket.
//!
//! A key does not need the secret. It needs to be equal for equal credentials
//! and different for different ones, and that is exactly what a keyed hash is.
//!
//! ## Why two hashes and not one
//!
//! The map these keys go in decides which tenant's queue list and which
//! tenant's consumer group a connection sees, so a collision is not a wrong
//! cache entry, it is one tenant served another's. One `RandomState` gives 64
//! bits; two independent ones give 128, which puts an accidental collision past
//! anything a process could reach (a facade holding a million credentials is at
//! ~10^-27). A deliberate one is harder still and not the point of the hash
//! being keyed: the keys are drawn once per process from the OS, so they are
//! not known to anyone who did not already have the memory this module exists
//! to protect.
//!
//! `RandomState` — SipHash-1-3 with a random key — is the same primitive every
//! `HashMap` in this binary already seeds itself from, so this adds no
//! dependency and no crypto to keep current.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::OnceLock;

/// One credential, as a map may keep it: 128 bits of process-local keyed hash,
/// or `None` for a connection that presented no credential at all — a listener
/// with no SASL, where every connection reaches Queen as the process does.
///
/// `Copy`, so passing it around costs nothing and nobody is tempted to keep the
/// string instead.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialKey(Option<(u64, u64)>);

/// Deliberately says nothing. The key is not the secret, but it is a stable
/// per-process identifier for one, and there is no line this facade writes that
/// is improved by having it in it — the username label is what identifies a
/// connection in a log ([`crate::sasl`]).
impl std::fmt::Debug for CredentialKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            None => write!(f, "CredentialKey(none)"),
            Some(_) => write!(f, "CredentialKey(hashed)"),
        }
    }
}

impl CredentialKey {
    /// The key for `token`. The string is read and dropped; nothing here keeps
    /// a copy of it.
    pub fn of(token: Option<&str>) -> CredentialKey {
        let Some(token) = token else {
            return CredentialKey(None);
        };
        let (a, b) = hashers();
        CredentialKey(Some((a.hash_one(token), b.hash_one(token))))
    }

    /// Whether this is the "no credential" key. For the one caller that treats
    /// an unauthenticated listener differently from an authenticated one.
    pub fn is_anonymous(&self) -> bool {
        self.0.is_none()
    }
}

/// The two hash keys, drawn once per process. Two `RandomState`s are two
/// independent keys — the type seeds itself from the OS on construction.
fn hashers() -> &'static (RandomState, RandomState) {
    static HASHERS: OnceLock<(RandomState, RandomState)> = OnceLock::new();
    HASHERS.get_or_init(|| (RandomState::new(), RandomState::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn one_credential_is_one_key_and_two_are_two() {
        let a = CredentialKey::of(Some("eyJhbGciOi.tenant.a"));
        assert_eq!(a, CredentialKey::of(Some("eyJhbGciOi.tenant.a")));
        assert_ne!(a, CredentialKey::of(Some("eyJhbGciOi.tenant.b")));
        // A one-character difference is a different key, which is the property
        // a map of these is built on.
        assert_ne!(a, CredentialKey::of(Some("eyJhbGciOi.tenant.A")));
    }

    /// The absent credential is its own key, and it is not any token's.
    #[test]
    fn no_credential_is_a_key_of_its_own() {
        let none = CredentialKey::of(None);
        assert!(none.is_anonymous());
        assert_eq!(none, CredentialKey::of(None));
        assert_ne!(none, CredentialKey::of(Some("")));
        assert!(!CredentialKey::of(Some("t")).is_anonymous());
    }

    /// What the type is for: a map keyed by it behaves like a map keyed by the
    /// token, and holds none of them.
    #[test]
    fn it_keys_a_map_the_way_the_token_would() {
        let mut m: HashMap<CredentialKey, &str> = HashMap::new();
        m.insert(CredentialKey::of(Some("token-a")), "a");
        m.insert(CredentialKey::of(Some("token-b")), "b");
        m.insert(CredentialKey::of(Some("token-a")), "a again");
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(&CredentialKey::of(Some("token-a"))), Some(&"a again"));
        assert_eq!(m.get(&CredentialKey::of(Some("nobody"))), None);
    }

    /// THE point of the type: printing it cannot print the credential, however
    /// a future log line gets hold of one.
    #[test]
    fn printing_a_key_never_prints_the_credential() {
        const SECRET: &str = "s3cr3t-tenant-token";
        let printed = format!("{:?}", CredentialKey::of(Some(SECRET)));
        assert!(!printed.contains(SECRET), "{printed}");
        assert!(!printed.contains("s3cr3t"), "{printed}");
    }
}
