//! Who may call, and which Queen bearer the facade presents on their behalf.
//!
//! CONTRACT. `Directory` is a static, operator-configured map from an SQS access
//! key id to (secret, Queen token). It is NOT a credential store and must never
//! grow into one: sasl.rs's ruling in queen-kafka ("no credential store") holds
//! here with one adaptation forced by the algorithm — SigV4 is SCRAM-shaped, the
//! secret never crosses the wire, so the VERIFIER has to hold it. That is the
//! whole reason this file exists and the kafka one did not need to.
//!
//! Three deployments, one type:
//!
//!   * **OSS**: `QUEEN_SQS_CREDENTIALS=akid:secret:queen_token[,…]`, operator
//!     config — the MinIO model.
//!   * **Dev**: `QUEEN_SQS_AUTH=off`, an empty directory, everything accepted.
//!     Nothing is verified and every request is attributed to
//!     [`ANONYMOUS_ACCESS_KEY_ID`], whose bearer is the process default
//!     (`QUEEN_TOKEN`) — [`crate::Facade::token_for`] is the one place that
//!     mapping happens, and it is the same mapping a principal with no token of
//!     its own gets.
//!   * **Cloud** (later, not in M0-M5): per-tenant keypairs live in the PROXY and
//!     the facade asks an introspection endpoint by access key id and caches the
//!     answer. The facade still never grows a directory — which is why every
//!     lookup below goes through this type rather than reading a map inline.
//!
//! The secret is held in memory for the life of the process, by necessity. What
//! does NOT happen to it: it is never logged, never rendered in an error, and
//! never compared with `==` — [`Directory::verify`]'s comparison is
//! constant-time, because a byte-by-byte early exit on a signature comparison is
//! a remote timing oracle for the signature, and through it for the secret.

/// The access key id an `auth=off` listener attributes a request to when the
/// caller presented none. It is a LABEL for the log line and for
/// [`crate::actions::Principal`]; it is never looked up here, because an
/// `auth=off` deployment has an empty directory by construction.
pub const ANONYMOUS_ACCESS_KEY_ID: &str = "anonymous";

/// One principal.
#[derive(Clone)]
pub struct Credential {
    pub access_key_id: String,
    /// The SigV4 secret. Never printed: the `Debug` below is deliberately blind.
    pub secret: String,
    /// The bearer the facade presents to Queen for this principal. `None` means
    /// "the process default" (`QUEEN_TOKEN`), which is the single-tenant OSS
    /// shape.
    pub queen_token: Option<String>,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("access_key_id", &self.access_key_id)
            .field("secret", &"<redacted>")
            .field(
                "queen_token",
                &self.queen_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Every principal this deployment knows.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    entries: Vec<Credential>,
}

impl Directory {
    /// Parse `akid:secret:token[,akid:secret:token…]`.
    ///
    /// Refuses a duplicate access key id rather than letting the last one win: a
    /// typo in a comma-separated list would otherwise silently disable a
    /// principal that is still in someone's config.
    ///
    /// Split rules, in the order they matter:
    ///
    ///   * entries are separated by `,` and fields by `:`, at most THREE fields
    ///     — so a Queen token containing colons (a URL, a `k:v` opaque string)
    ///     survives, while an access key id or a secret containing one cannot be
    ///     expressed. Neither ever does: an AWS-shaped key id is `[A-Z0-9]` and
    ///     a secret is base64's alphabet;
    ///   * the token is optional. `akid:secret` and `akid:secret:` both mean
    ///     "this principal uses the process default bearer", which is the whole
    ///     single-tenant OSS deployment;
    ///   * every field is trimmed. A leading space on a secret is a copy-paste
    ///     artefact from a multi-line env file, never a secret — and the failure
    ///     it would otherwise cause is an unsigned-request error nobody can
    ///     debug from the client side.
    ///
    /// Every refusal names `QUEEN_SQS_CREDENTIALS` and the entry's ORDINAL, and
    /// none of them can carry a secret: the operator reads this message out of a
    /// boot log that is shipped somewhere.
    pub fn from_spec(spec: &str) -> Result<Directory, String> {
        let mut entries: Vec<Credential> = Vec::new();
        for (index, raw) in spec.split(',').enumerate() {
            let ordinal = index + 1;
            let entry = raw.trim();
            if entry.is_empty() {
                return Err(format!(
                    "QUEEN_SQS_CREDENTIALS entry {ordinal} is empty (a stray comma?). The format \
                     is akid:secret:token[,akid:secret:token…]"
                ));
            }
            let mut fields = entry.splitn(3, ':');
            let access_key_id = fields.next().unwrap_or_default().trim();
            let secret = fields.next().map(str::trim);
            let token = fields.next().unwrap_or_default().trim();
            if access_key_id.is_empty() {
                return Err(format!(
                    "QUEEN_SQS_CREDENTIALS entry {ordinal} has no access key id. The format is \
                     akid:secret:token[,akid:secret:token…]"
                ));
            }
            let Some(secret) = secret else {
                return Err(format!(
                    "QUEEN_SQS_CREDENTIALS entry {ordinal} ({access_key_id}) has no ':'. The \
                     format is akid:secret:token, and the token may be omitted to use QUEEN_TOKEN"
                ));
            };
            if secret.is_empty() {
                return Err(format!(
                    "QUEEN_SQS_CREDENTIALS entry {ordinal} ({access_key_id}) has an empty secret. \
                     SigV4 verification needs it: the secret never crosses the wire, so this \
                     process is the only thing that can hold it"
                ));
            }
            if entries.iter().any(|c| c.access_key_id == access_key_id) {
                return Err(format!(
                    "QUEEN_SQS_CREDENTIALS names the access key id {access_key_id} twice (entry \
                     {ordinal}). One of the two is dead config; say which"
                ));
            }
            entries.push(Credential {
                access_key_id: access_key_id.to_string(),
                secret: secret.to_string(),
                queen_token: if token.is_empty() {
                    None
                } else {
                    Some(token.to_string())
                },
            });
        }
        Ok(Directory { entries })
    }

    pub fn empty() -> Directory {
        Directory {
            entries: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The principal for an access key id, or `None`.
    ///
    /// The scan is linear and not constant-time on purpose: an access key id is
    /// PUBLIC — it travels in the clear in every `Authorization` header and in
    /// every presigned URL — so there is nothing here to leak. The secret it
    /// selects is what must never be compared in variable time, and that
    /// comparison is [`Directory::verify`].
    pub fn get(&self, access_key_id: &str) -> Option<&Credential> {
        self.entries
            .iter()
            .find(|c| c.access_key_id == access_key_id)
    }

    /// Constant-time compare of a computed signature against the presented one.
    /// Both are lowercase hex of 32 bytes; a length mismatch answers false
    /// without leaking where it differed.
    ///
    /// The length check exits early, which leaks the LENGTH and nothing else: a
    /// SigV4 signature is 64 hex characters by definition, so the length is not
    /// a secret. The fold below is over the whole buffer regardless of where the
    /// first difference is, and `black_box` is what stops a compiler from
    /// noticing it could stop early.
    pub fn verify(computed: &[u8], presented: &[u8]) -> bool {
        if computed.len() != presented.len() {
            return false;
        }
        let mut difference = 0u8;
        for (a, b) in computed.iter().zip(presented.iter()) {
            difference |= a ^ b;
        }
        std::hint::black_box(difference) == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    #[test]
    fn a_spec_parses_into_principals() {
        let directory = Directory::from_spec(&format!("AKIDEXAMPLE:{SECRET}:tok-1")).unwrap();
        assert_eq!(directory.len(), 1);
        assert!(!directory.is_empty());
        let credential = directory.get("AKIDEXAMPLE").unwrap();
        assert_eq!(credential.secret, SECRET);
        assert_eq!(credential.queen_token.as_deref(), Some("tok-1"));
    }

    #[test]
    fn several_entries_keep_their_own_tokens() {
        let directory =
            Directory::from_spec(&format!("A:{SECRET}:tok-a, B:{SECRET}:tok-b ,C:{SECRET}"))
                .unwrap();
        assert_eq!(directory.len(), 3);
        assert_eq!(
            directory.get("A").unwrap().queen_token.as_deref(),
            Some("tok-a")
        );
        assert_eq!(
            directory.get("B").unwrap().queen_token.as_deref(),
            Some("tok-b")
        );
        // The third named none: the process default is what it gets, and
        // `token_for` is where that happens.
        assert_eq!(directory.get("C").unwrap().queen_token, None);
    }

    /// A Queen bearer is an opaque string and JWTs and URLs both carry colons.
    /// Only the first two separators are separators.
    #[test]
    fn a_token_may_contain_colons_and_an_absent_one_is_none() {
        let directory = Directory::from_spec(&format!(
            "AKID:{SECRET}:https://queen:6789/t/acme:extra,OTHER:{SECRET}:"
        ))
        .unwrap();
        assert_eq!(
            directory.get("AKID").unwrap().queen_token.as_deref(),
            Some("https://queen:6789/t/acme:extra")
        );
        assert_eq!(directory.get("OTHER").unwrap().queen_token, None);
    }

    /// The whole reason the parse is strict rather than last-one-wins: the
    /// operator who typed the key id twice believes both principals work.
    #[test]
    fn a_duplicate_access_key_id_is_refused_rather_than_shadowed() {
        let err =
            Directory::from_spec(&format!("AKID:{SECRET}:one,AKID:{SECRET}:two")).unwrap_err();
        assert!(err.contains("twice"), "{err}");
        assert!(err.contains("AKID"), "{err}");
        assert!(err.contains("QUEEN_SQS_CREDENTIALS"), "{err}");
    }

    #[test]
    fn a_malformed_entry_names_the_variable_and_the_ordinal() {
        let cases = [
            (format!("AKID:{SECRET}:tok,"), "entry 2 is empty"),
            (format!("AKID:{SECRET}:tok,,B:{SECRET}"), "entry 2 is empty"),
            ("AKIDONLY".to_string(), "has no ':'"),
            (format!(":{SECRET}:tok"), "has no access key id"),
            ("AKID::tok".to_string(), "has an empty secret"),
            ("AKID:   :tok".to_string(), "has an empty secret"),
        ];
        for (spec, expected) in cases {
            let err = Directory::from_spec(&spec).unwrap_err();
            assert!(err.contains(expected), "{spec:?} answered {err:?}");
            assert!(err.contains("QUEEN_SQS_CREDENTIALS"), "{err}");
        }
    }

    /// Boot logs are shipped. No refusal may carry the one field that must not
    /// travel.
    #[test]
    fn no_refusal_can_carry_the_secret() {
        for spec in [
            format!("AKID:{SECRET}:tok,AKID:{SECRET}:tok"),
            format!("AKID:{SECRET}:tok,"),
            format!(":{SECRET}"),
            format!("AKID:{SECRET}:tok,AKIDONLY"),
        ] {
            let err = Directory::from_spec(&spec).unwrap_err();
            assert!(!err.contains(SECRET), "the secret leaked into {err:?}");
        }
    }

    #[test]
    fn the_debug_rendering_never_prints_the_secret_or_the_token() {
        let directory = Directory::from_spec(&format!("AKID:{SECRET}:tok-1")).unwrap();
        let rendered = format!("{directory:?}");
        assert!(rendered.contains("AKID"));
        assert!(!rendered.contains(SECRET), "{rendered}");
        assert!(!rendered.contains("tok-1"), "{rendered}");
    }

    #[test]
    fn an_unknown_access_key_id_is_none() {
        let directory = Directory::from_spec(&format!("AKID:{SECRET}:tok")).unwrap();
        assert!(
            directory.get("akid").is_none(),
            "the lookup is case-sensitive"
        );
        assert!(directory.get("OTHER").is_none());
        assert!(directory.get("").is_none());
    }

    /// `QUEEN_SQS_AUTH=off` is this shape, and nothing else in the process needs
    /// to know about the mode to behave correctly.
    #[test]
    fn an_empty_directory_is_the_auth_off_shape() {
        let directory = Directory::empty();
        assert!(directory.is_empty());
        assert_eq!(directory.len(), 0);
        assert!(directory.get(ANONYMOUS_ACCESS_KEY_ID).is_none());
        assert!(Directory::default().is_empty());
    }

    #[test]
    fn the_constant_time_compare_agrees_with_equality() {
        let signature = b"5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31";
        assert!(Directory::verify(signature, signature));
        // First byte, last byte, and a length that differs: all false, none of
        // them by a different route.
        let mut first = *signature;
        first[0] ^= 0x01;
        assert!(!Directory::verify(signature, &first));
        let mut last = *signature;
        last[63] ^= 0x01;
        assert!(!Directory::verify(signature, &last));
        assert!(!Directory::verify(signature, &signature[..63]));
        assert!(!Directory::verify(b"", signature));
        assert!(Directory::verify(b"", b""));
    }
}
