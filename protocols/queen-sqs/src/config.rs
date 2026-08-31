//! The operator's whole surface, read once at boot.
//!
//! CONTRACT. `Config::from_env` is the ONLY reader of the process environment in
//! this binary, and it either produces a complete `Config` or a `String` that
//! names the variable that is wrong. Nothing below this module may call
//! `std::env::var`: the embedded mode (`QUEEN_SQS_EMBEDDED`) strips secrets out
//! of the child's environment after boot (queen-kafka's discipline, and the
//! server's `STRIPPED_ENV`), so a late read would see a variable that is
//! deliberately no longer there.
//!
//! The surface is PLAN_QUEEN_SQS.md's "Config surface", verbatim:
//!
//! ```text
//! QUEEN_SQS_LISTEN=0.0.0.0:9324        QUEEN_SQS_AUTH=sigv4|off
//! QUEEN_SQS_CREDENTIALS=akid:secret:token[,...]
//! QUEEN_SQS_REGION=queen-1             QUEEN_SQS_ACCOUNT=000000000000
//! QUEEN_SQS_RECEIVE_MODE=exact|amortized   (amortized requires broker with C-SQS-1)
//! QUEEN_SQS_DEFAULT_PARTITIONS=64      QUEEN_SQS_HANDLE_SECRET=<hmac key, else random>
//! QUEEN_URL / QUEEN_TOKEN              (as kafka)
//! QUEEN_SQS_EMBEDDED=true|false        QUEEN_SQS_BIN / QUEEN_SQS_SHUTDOWN_GRACE_MS
//! QUEEN_SQS_TLS_CERT / _KEY            (SDKs are happy on plain HTTP for custom endpoints)
//! ```
//!
//! `QUEEN_SQS_BIN` is the one name above this process never reads: it tells the
//! BROKER which binary to spawn in embedded mode (server-side, as
//! `QUEEN_KAFKA_BIN` already is), so a field for it here would be a value that
//! can only ever be wrong.
//!
//! ## Blank is unset
//!
//! Every read below treats an empty or whitespace-only value as absent. A
//! Kubernetes manifest, a Compose file and a `.env` all express "leave this
//! alone" by setting the variable to `""`, and a facade that read that as a
//! zero-length region label, an empty credential list or a listener bound to
//! nowhere would fail somewhere the operator cannot connect to the line they
//! wrote.

use crate::credentials::Directory;
use crate::queen::normalize_base_url;
use crate::registry::Naming;

/// The default listener. 9324 is the de-facto self-hosted SQS port (ElasticMQ's,
/// and what every "point boto3 at a local queue" tutorial already writes), so a
/// client config that worked against one of those works here unchanged.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9324";
/// The default region label. It is a LABEL: it appears in queue URLs, in ARNs
/// and in the SigV4 credential scope, and it identifies this deployment rather
/// than any AWS region.
pub const DEFAULT_REGION: &str = "queen-1";
/// The default account segment of a queue URL and an ARN. Twelve zeroes is what
/// every SQS-compatible emulator uses, so tooling that parses ARNs is happy.
pub const DEFAULT_ACCOUNT: &str = "000000000000";
/// The default width a standard queue synthesizes. 64 lanes is the number the
/// plan fixed: enough parallelism for a real fleet, few enough that a queue's
/// partition rows are not a cost of their own.
pub const DEFAULT_PARTITIONS: u32 = 64;
/// The broker this facade talks to when nothing says otherwise — the broker's
/// own default bind, spelled exactly as queen-kafka spells it so that the two
/// facades are configured the same way.
pub const DEFAULT_QUEEN_URL: &str = "http://localhost:6632";

/// How long in-flight requests get after the listener stops accepting.
///
/// It has to outlive ONE LONG POLL, which is what makes it larger than a web
/// server's usual grace: a `ReceiveMessage` with `WaitTimeSeconds=20` is a
/// request doing its job while looking idle, and cutting it off at, say, five
/// seconds turns every rolling deploy into a burst of client-side timeouts on
/// requests that were about to answer. 25s = SQS's own 20s ceiling plus the
/// round trip.
pub const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 25_000;

/// The shortest configured receipt-handle key this facade will start with.
///
/// A receipt handle is a capability: whoever can forge its tag can delete any
/// message in any queue this facade serves. The tag is HMAC-SHA256, whose
/// security is the key's, so a key an attacker can enumerate is no tag at all —
/// and unlike a weak password there is no rate limit in front of it, because the
/// verification is offline arithmetic on a string the client already holds.
pub const MIN_HANDLE_SECRET_BYTES: usize = 16;

/// The widest standard queue this facade will synthesize.
///
/// Not a broker limit — Queen carries far more — but a facade one: `M` lanes is
/// `M` partition rows created at `CreateQueue` and, in exact mode, the space a
/// `ReceiveMessage` claims across. A five-figure default set by a typo (`64000`
/// for `64`) is a queue whose every operation is slow and which cannot be
/// narrowed afterwards, since partition counts never shrink.
pub const MAX_DEFAULT_PARTITIONS: u32 = 100_000;

/// The ceiling on the shutdown grace: ten minutes. Past that a stop is not
/// graceful, it is a pod that will be SIGKILLed by its orchestrator mid-answer
/// while this process still believes it is draining politely.
pub const MAX_SHUTDOWN_GRACE_MS: u64 = 600_000;

/// How a request proves who it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// Verify SigV4 against [`Directory`]. The only mode a deployment reachable
    /// by anything but localhost may run.
    SigV4,
    /// Accept anything, ElasticMQ-style, for local development. boto3 still
    /// wants dummy credentials configured, which is its normal fake-endpoint
    /// workflow.
    Off,
}

impl AuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthMode::SigV4 => "sigv4",
            AuthMode::Off => "off",
        }
    }
}

/// How `ReceiveMessage` claims from the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveMode {
    /// N parallel `batch=1` pops. Claim width 1 makes every SQS verb exact, at
    /// the cost of one pop write-transaction per message.
    Exact,
    /// One pop claiming k partitions capped at one message each (C-SQS-1).
    /// Restores single-write-transaction amortization and admits two bounded
    /// divergences — extending one message extends its pop-mates, terminating
    /// one returns the others as duplicates — both inside SQS's own
    /// at-least-once envelope. Never the default, and never silent.
    ///
    /// NOT REACHABLE FROM THE ENVIRONMENT TODAY. C-SQS-1 is not implemented, so
    /// [`Config::from_source`] refuses `QUEEN_SQS_RECEIVE_MODE=amortized` at
    /// boot rather than accepting a name it would serve as `exact`. The variant
    /// stays because the mode is a decided part of PLAN_QUEEN_SQS.md and the
    /// refusal is what has to be deleted when the broker grows the parameter —
    /// one place, named by its test.
    Amortized,
}

impl ReceiveMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ReceiveMode::Exact => "exact",
            ReceiveMode::Amortized => "amortized",
        }
    }
}

/// The optional TLS listener's material. SDKs are perfectly happy on plain HTTP
/// against a custom endpoint, so this exists for deployments whose policy is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsMaterial {
    pub cert_path: String,
    pub key_path: String,
}

/// Everything the process was configured with.
#[derive(Clone)]
pub struct Config {
    pub listen: String,
    pub auth: AuthMode,
    /// The static principal directory. Empty is legal ONLY under
    /// [`AuthMode::Off`]; a `sigv4` listener with no credentials can answer
    /// nothing and is refused at boot rather than at the first request.
    pub credentials: Directory,
    pub region: String,
    pub account: String,
    pub receive_mode: ReceiveMode,
    pub default_partitions: u32,
    /// The HMAC key receipt handles are tagged with. Configured, it makes a
    /// handle valid across every instance AND across a restart; unset, one is
    /// generated per process, which is correct for a single instance and visible
    /// as `ReceiptHandleIsInvalid` the moment a second one is added — so the
    /// boot line says which of the two happened.
    pub handle_secret: Vec<u8>,
    /// Whether [`Config::handle_secret`] was generated rather than configured.
    pub handle_secret_generated: bool,
    pub queen_url: String,
    /// The bearer used when a principal names none — and the only one on an
    /// `auth=off` listener.
    pub queen_token: Option<String>,
    pub embedded: bool,
    pub shutdown_grace_ms: u64,
    pub tls: Option<TlsMaterial>,
}

/// Hand-written rather than derived, for TWO fields: `queen_token` is a bearer
/// and `handle_secret` is the key every receipt handle is tagged with, and a
/// derived `Debug` prints both.
///
/// Nothing prints a `Config` today, which is exactly why this is here: the whole
/// value of a redacting `Debug` is that it is already in place on the day
/// someone adds `?config` to a boot line, or a panic renders the struct. The
/// rest is configuration a person set and would want to read back, so it is
/// printed as it is. Same rule, and the same reason, as queen-kafka's own.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("listen", &self.listen)
            .field("auth", &self.auth)
            .field("credentials", &self.credentials)
            .field("region", &self.region)
            .field("account", &self.account)
            .field("receive_mode", &self.receive_mode)
            .field("default_partitions", &self.default_partitions)
            // Where it came from is worth printing; what it is never is.
            .field(
                "handle_secret",
                match self.handle_secret_generated {
                    true => &"<generated>",
                    false => &"<configured>",
                },
            )
            .field("queen_url", &self.queen_url)
            .field(
                "queen_token",
                &self.queen_token.as_ref().map(|_| "<redacted>"),
            )
            .field("embedded", &self.embedded)
            .field("shutdown_grace_ms", &self.shutdown_grace_ms)
            .field("tls", &self.tls)
            .finish()
    }
}

impl Config {
    /// Read and validate the whole surface. `Err` names the variable and what
    /// was wrong with it, in the one message an operator sees at boot.
    pub fn from_env() -> Result<Config, String> {
        Config::from_source(&|name| std::env::var(name).ok())
    }

    /// [`Config::from_env`] against an arbitrary source.
    ///
    /// The seam exists for the tests and for nothing else, but it is a real one:
    /// a validation suite that set process environment variables would be a
    /// suite whose cases cannot run in parallel — `std::env::set_var` is
    /// process-global and, since Rust 1.80, documented as unsound beside
    /// threads. Every rule below is therefore exercised through a map.
    pub fn from_source(get: &dyn Fn(&str) -> Option<String>) -> Result<Config, String> {
        let read = |name: &str| -> Option<String> {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };

        let listen = read("QUEEN_SQS_LISTEN").unwrap_or_else(|| DEFAULT_LISTEN.to_string());
        validate_listen(&listen)?;

        let auth = match read("QUEEN_SQS_AUTH") {
            None => AuthMode::SigV4,
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "sigv4" | "sig-v4" | "v4" | "on" => AuthMode::SigV4,
                "off" | "none" | "disabled" => AuthMode::Off,
                other => {
                    return Err(format!(
                        "QUEEN_SQS_AUTH={other} is not a mode. It is `sigv4` (verify every \
                         request against QUEEN_SQS_CREDENTIALS) or `off` (accept anything, for \
                         local development only)"
                    ))
                }
            },
        };

        let credentials = match read("QUEEN_SQS_CREDENTIALS") {
            None => Directory::empty(),
            Some(spec) => Directory::from_spec(&spec)?,
        };
        // A listener that verifies signatures against nothing cannot answer a
        // single request: every one of them is `InvalidClientTokenId`, which
        // reads to the client like a wrong access key id rather than like a
        // server that was started without its keys. The refusal names both ways
        // forward because the operator meant one of exactly two things.
        if auth == AuthMode::SigV4 && credentials.is_empty() {
            return Err(
                "QUEEN_SQS_AUTH=sigv4 (the default) with no QUEEN_SQS_CREDENTIALS: this listener \
                 would refuse every request as InvalidClientTokenId. Set \
                 QUEEN_SQS_CREDENTIALS=akid:secret:token[,…], or set QUEEN_SQS_AUTH=off for a \
                 development listener that accepts anything"
                    .to_string(),
            );
        }

        let region = read("QUEEN_SQS_REGION").unwrap_or_else(|| DEFAULT_REGION.to_string());
        validate_label("QUEEN_SQS_REGION", &region)?;
        let account = read("QUEEN_SQS_ACCOUNT").unwrap_or_else(|| DEFAULT_ACCOUNT.to_string());
        validate_label("QUEEN_SQS_ACCOUNT", &account)?;

        let receive_mode = match read("QUEEN_SQS_RECEIVE_MODE") {
            None => ReceiveMode::Exact,
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "exact" => ReceiveMode::Exact,
                // REFUSED, and this is the whole of the decision: `amortized`
                // is one pop claiming k lanes capped at one message each, and
                // that cap is `maxPerPartition` — C-SQS-1, which is NOT
                // implemented in the broker. A facade that ACCEPTED the value
                // would serve every receive in exact mode: correct, and silently
                // not the mode the operator asked for. The whole point of the
                // dial is that it is never silent (PLAN_QUEEN_SQS.md, the
                // batching dial), so the one honest answer to a mode that cannot
                // be served is to refuse to start rather than to run as
                // something else under its name.
                "amortized" | "amortised" => {
                    return Err(format!(
                        "QUEEN_SQS_RECEIVE_MODE={value} cannot be served by this build: it needs \
                         `maxPerPartition` on the broker's pop (C-SQS-1 in PLAN_QUEEN_SQS.md, \
                         Core changes), which is not implemented. Accepting it would run every \
                         ReceiveMessage in `exact` mode under the other mode's name. Unset the \
                         variable, or set QUEEN_SQS_RECEIVE_MODE=exact"
                    ))
                }
                other => {
                    return Err(format!(
                        "QUEEN_SQS_RECEIVE_MODE={other} is not a mode. It is `exact` (one pop per \
                         message, every SQS verb exact) or `amortized` (one pop across k \
                         partitions, which needs a broker carrying maxPerPartition — C-SQS-1, not \
                         implemented, so `amortized` is refused at boot today)"
                    ))
                }
            },
        };

        let default_partitions = number(
            "QUEEN_SQS_DEFAULT_PARTITIONS",
            read("QUEEN_SQS_DEFAULT_PARTITIONS"),
        )?
        .map(|n| u32::try_from(n).unwrap_or(u32::MAX))
        .unwrap_or(DEFAULT_PARTITIONS);
        if default_partitions == 0 || default_partitions > MAX_DEFAULT_PARTITIONS {
            return Err(format!(
                "QUEEN_SQS_DEFAULT_PARTITIONS={default_partitions} is outside 1..={MAX_DEFAULT_PARTITIONS}. \
                 It is the number of lanes a standard queue synthesizes, it is stamped on the \
                 queue at CreateQueue, and partition counts never shrink"
            ));
        }

        let (handle_secret, handle_secret_generated) = match read("QUEEN_SQS_HANDLE_SECRET") {
            Some(secret) => {
                if secret.len() < MIN_HANDLE_SECRET_BYTES {
                    return Err(format!(
                        "QUEEN_SQS_HANDLE_SECRET is {} bytes; it must be at least \
                         {MIN_HANDLE_SECRET_BYTES}. It is the HMAC key every receipt handle is \
                         tagged with, so whoever can guess it can delete any message in any queue \
                         this facade serves",
                        secret.len()
                    ));
                }
                (secret.into_bytes(), false)
            }
            None => (generated_secret(), true),
        };

        let queen_url = normalize_base_url(
            &read("QUEEN_URL").unwrap_or_else(|| DEFAULT_QUEEN_URL.to_string()),
        )?;
        let queen_token = read("QUEEN_TOKEN");

        let embedded = flag("QUEEN_SQS_EMBEDDED", read("QUEEN_SQS_EMBEDDED"))?.unwrap_or(false);
        let shutdown_grace_ms = number(
            "QUEEN_SQS_SHUTDOWN_GRACE_MS",
            read("QUEEN_SQS_SHUTDOWN_GRACE_MS"),
        )?
        .map(|n| n as u64)
        .unwrap_or(DEFAULT_SHUTDOWN_GRACE_MS);
        if shutdown_grace_ms > MAX_SHUTDOWN_GRACE_MS {
            return Err(format!(
                "QUEEN_SQS_SHUTDOWN_GRACE_MS={shutdown_grace_ms} is longer than \
                 {MAX_SHUTDOWN_GRACE_MS}ms. Past that a stop is not graceful: the orchestrator \
                 SIGKILLs the process mid-answer while it still believes it is draining"
            ));
        }

        // Both or neither. One of the two is the failure that looks like a
        // working listener until a client dials it: with a certificate and no
        // key there is nothing to serve TLS with, and with a key and no
        // certificate the operator who set it believes the listener is
        // encrypted while it is answering in the clear — which is also the
        // scheme every queue URL this facade mints would then be wrong about.
        let tls =
            match (read("QUEEN_SQS_TLS_CERT"), read("QUEEN_SQS_TLS_KEY")) {
                (None, None) => None,
                (Some(cert_path), Some(key_path)) => Some(TlsMaterial {
                    cert_path,
                    key_path,
                }),
                (Some(_), None) => {
                    return Err(
                        "QUEEN_SQS_TLS_CERT is set without QUEEN_SQS_TLS_KEY: both or neither"
                            .to_string(),
                    )
                }
                (None, Some(_)) => return Err(
                    "QUEEN_SQS_TLS_KEY is set without QUEEN_SQS_TLS_CERT: both or neither. A key \
                     alone serves nothing, and the queue URLs this facade mints would say https \
                     for a listener answering in the clear"
                        .to_string(),
                ),
            };

        Ok(Config {
            listen,
            auth,
            credentials,
            region,
            account,
            receive_mode,
            default_partitions,
            handle_secret,
            handle_secret_generated,
            queen_url,
            queen_token,
            embedded,
            shutdown_grace_ms,
            tls,
        })
    }

    /// The region and account every URL and ARN this deployment mints comes
    /// from. One value, so the minting and the PARSING cannot drift.
    pub fn naming(&self) -> Naming {
        Naming::new(&self.region, &self.account)
    }

    /// The scheme a client reached this facade on, as far as this process can
    /// tell: its own listener's. Behind a TLS-terminating load balancer that is
    /// wrong, and the operator must front the fleet with one that rewrites the
    /// URLs — the caveat every self-hosted S3 and SQS endpoint carries.
    pub fn scheme(&self) -> &'static str {
        match self.tls {
            Some(_) => "https",
            None => "http",
        }
    }

    /// The URL a queue of this name is addressed by:
    /// `http://<host>:<port>/<account>/<name>`. It is what `CreateQueue` and
    /// `GetQueueUrl` answer and what every SDK then posts to, so the host must be
    /// the one CLIENTS reach rather than the one this process bound.
    pub fn queue_url(&self, host: &str, name: &str) -> String {
        self.naming().url(self.scheme(), host, name)
    }

    /// `arn:aws:sqs:<region>:<account>:<name>`.
    pub fn queue_arn(&self, name: &str) -> String {
        self.naming().arn(name)
    }

    /// The queue name inside a URL this facade issued, or `None` when the URL is
    /// not one of ours. `QueueUrl` is a client-supplied string on every message
    /// action, so this is a PARSER of untrusted input and not a formatter run
    /// backwards.
    pub fn queue_name_of(&self, queue_url: &str) -> Option<String> {
        self.naming().name_of(queue_url)
    }
}

/// 32 bytes nobody configured.
///
/// Two v4 UUIDs rather than a random-number crate: `uuid` is already in the
/// dependency list for request ids, its v4 constructor is `getrandom` — the
/// operating system's CSPRNG — and 128 bits twice is the key size HMAC-SHA256
/// wants. The alternative, one more crate for one call, is a dependency to keep
/// in step forever.
fn generated_secret() -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    key
}

/// `host:port`, where the port is a port.
///
/// Deliberately NOT `to_socket_addrs`: that resolves, which at boot means a DNS
/// lookup that can hang, and it refuses `0.0.0.0:9324` on no host at all. What
/// has to be true here is that the string has a port this process can bind and a
/// host half that is not empty — the bind itself reports anything else, in a
/// message that already names the address.
fn validate_listen(listen: &str) -> Result<(), String> {
    let Some((host, port)) = listen.rsplit_once(':') else {
        return Err(format!(
            "QUEEN_SQS_LISTEN={listen} has no port. It is `host:port`, for example \
             {DEFAULT_LISTEN}"
        ));
    };
    if host.is_empty() {
        return Err(format!(
            "QUEEN_SQS_LISTEN={listen} has no host. Bind every interface with 0.0.0.0:{port}, or \
             loopback only with 127.0.0.1:{port}"
        ));
    }
    match port.parse::<u16>() {
        Ok(0) => Err(format!(
            "QUEEN_SQS_LISTEN={listen} asks for port 0, which binds an ephemeral port no client \
             can be told about"
        )),
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "QUEEN_SQS_LISTEN={listen}: `{port}` is not a port number"
        )),
    }
}

/// The charset for the two values that appear inside a URL, inside an ARN and
/// inside the SigV4 credential scope.
///
/// Strict on purpose: a `/` in the account would make every queue URL parse back
/// to a different name, a space would make every ARN unquotable in the tooling
/// that reads them, and neither failure appears until a client is already
/// holding the wrong string.
fn validate_label(variable: &str, value: &str) -> Result<(), String> {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Ok(());
    }
    Err(format!(
        "{variable}={value} may only contain letters, digits, `-` and `_`: it appears inside \
         every queue URL, every ARN and the SigV4 credential scope, and anything else there is a \
         URL that does not parse back to the queue it names"
    ))
}

/// A non-negative integer, or an error that names the variable.
///
/// Negatives are refused HERE rather than by each range check below, so that
/// `-1` is answered as what it is instead of being widened into a number the
/// range message then reports as astronomically large.
fn number(variable: &str, value: Option<String>) -> Result<Option<i64>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.parse::<i64>() {
        Ok(n) if n >= 0 => Ok(Some(n)),
        _ => Err(format!(
            "{variable}={value} is not a whole number of zero or more"
        )),
    }
}

/// A boolean in any of the spellings a shell, a Helm chart and a Compose file
/// produce. Anything else is refused rather than read as false: an operator who
/// wrote `QUEEN_SQS_EMBEDDED=yes please` meant `true`, and a silent `false` is a
/// facade the broker never supervises.
fn flag(variable: &str, value: Option<String>) -> Result<Option<bool>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(Some(true)),
        "false" | "0" | "no" | "off" => Ok(Some(false)),
        _ => Err(format!(
            "{variable}={value} is not a boolean: it is true/false (1/0, yes/no, on/off)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    /// A source built from pairs, so no test touches the process environment.
    fn source(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn config(pairs: &[(&str, &str)]) -> Result<Config, String> {
        Config::from_source(&source(pairs))
    }

    /// The minimum an OSS deployment sets, and what it then is.
    fn dev(extra: &[(&str, &str)]) -> Config {
        let mut pairs = vec![("QUEEN_SQS_AUTH", "off")];
        pairs.extend_from_slice(extra);
        config(&pairs).expect("the development shape boots")
    }

    /// The defaults are part of the contract: a deployment that sets nothing is
    /// the one the documentation describes.
    #[test]
    fn the_documented_defaults_are_these() {
        assert_eq!(DEFAULT_LISTEN, "0.0.0.0:9324");
        assert_eq!(DEFAULT_REGION, "queen-1");
        assert_eq!(DEFAULT_ACCOUNT, "000000000000");
        assert_eq!(DEFAULT_PARTITIONS, 64);

        let cfg = dev(&[]);
        assert_eq!(cfg.listen, DEFAULT_LISTEN);
        assert_eq!(cfg.region, DEFAULT_REGION);
        assert_eq!(cfg.account, DEFAULT_ACCOUNT);
        assert_eq!(cfg.default_partitions, DEFAULT_PARTITIONS);
        assert_eq!(cfg.receive_mode, ReceiveMode::Exact);
        assert_eq!(cfg.queen_url, DEFAULT_QUEEN_URL);
        assert_eq!(cfg.queen_token, None);
        assert!(!cfg.embedded);
        assert_eq!(cfg.shutdown_grace_ms, DEFAULT_SHUTDOWN_GRACE_MS);
        assert!(cfg.tls.is_none());
        assert!(cfg.credentials.is_empty());
    }

    /// The whole surface, set at once, read back.
    #[test]
    fn every_variable_lands_where_it_is_read() {
        let cfg = config(&[
            ("QUEEN_SQS_LISTEN", "127.0.0.1:19324"),
            ("QUEEN_SQS_AUTH", "sigv4"),
            ("QUEEN_SQS_CREDENTIALS", &format!("AKID:{SECRET}:tok-1")),
            ("QUEEN_SQS_REGION", "eu-west-9"),
            ("QUEEN_SQS_ACCOUNT", "123456789012"),
            ("QUEEN_SQS_RECEIVE_MODE", "exact"),
            ("QUEEN_SQS_DEFAULT_PARTITIONS", "8"),
            ("QUEEN_SQS_HANDLE_SECRET", "a sixteen byte+ key"),
            ("QUEEN_URL", "https://queen.internal:6632/"),
            ("QUEEN_TOKEN", "process-default"),
            ("QUEEN_SQS_EMBEDDED", "true"),
            ("QUEEN_SQS_SHUTDOWN_GRACE_MS", "1000"),
            ("QUEEN_SQS_TLS_CERT", "/tls/cert.pem"),
            ("QUEEN_SQS_TLS_KEY", "/tls/key.pem"),
        ])
        .expect("boots");

        assert_eq!(cfg.listen, "127.0.0.1:19324");
        assert_eq!(cfg.auth, AuthMode::SigV4);
        assert_eq!(cfg.credentials.len(), 1);
        assert_eq!(cfg.region, "eu-west-9");
        assert_eq!(cfg.account, "123456789012");
        // `amortized` is the other spelling and it is REFUSED at boot; see
        // `the_unserved_receive_mode_is_refused_at_boot`.
        assert_eq!(cfg.receive_mode, ReceiveMode::Exact);
        assert_eq!(cfg.default_partitions, 8);
        assert_eq!(cfg.handle_secret, b"a sixteen byte+ key".to_vec());
        assert!(!cfg.handle_secret_generated);
        // Normalized: the trailing slash is gone, because every path this
        // facade appends already starts with one.
        assert_eq!(cfg.queen_url, "https://queen.internal:6632");
        assert_eq!(cfg.queen_token.as_deref(), Some("process-default"));
        assert!(cfg.embedded);
        assert_eq!(cfg.shutdown_grace_ms, 1_000);
        assert_eq!(
            cfg.tls,
            Some(TlsMaterial {
                cert_path: "/tls/cert.pem".to_string(),
                key_path: "/tls/key.pem".to_string(),
            })
        );
        assert_eq!(cfg.scheme(), "https");
    }

    /// A variable set to the empty string is a variable an operator meant to
    /// leave alone — every deployment format expresses it that way.
    #[test]
    fn a_blank_value_is_an_unset_one() {
        let cfg = config(&[
            ("QUEEN_SQS_AUTH", "off"),
            ("QUEEN_SQS_LISTEN", "   "),
            ("QUEEN_SQS_REGION", ""),
            ("QUEEN_SQS_ACCOUNT", ""),
            ("QUEEN_TOKEN", ""),
            ("QUEEN_SQS_CREDENTIALS", ""),
            ("QUEEN_SQS_HANDLE_SECRET", ""),
        ])
        .expect("boots on the defaults");
        assert_eq!(cfg.listen, DEFAULT_LISTEN);
        assert_eq!(cfg.region, DEFAULT_REGION);
        assert_eq!(cfg.queen_token, None);
        assert!(cfg.credentials.is_empty());
        assert!(cfg.handle_secret_generated);
    }

    /// THE boot refusal: signature verification against an empty directory can
    /// only ever answer InvalidClientTokenId, which reads to the client as its
    /// own misconfiguration.
    #[test]
    fn sigv4_without_credentials_is_refused_at_boot() {
        let err = config(&[]).expect_err("refused");
        assert!(err.contains("QUEEN_SQS_CREDENTIALS"), "{err}");
        assert!(err.contains("QUEEN_SQS_AUTH=off"), "{err}");

        // ...and the same with the mode spelled out.
        assert!(config(&[("QUEEN_SQS_AUTH", "sigv4")]).is_err());
        // With credentials it boots.
        let cfg = config(&[("QUEEN_SQS_CREDENTIALS", &format!("AKID:{SECRET}"))])
            .expect("boots with a directory");
        assert_eq!(cfg.auth, AuthMode::SigV4, "sigv4 is the default mode");
    }

    /// A bad credential spec fails the boot with the spec parser's own message,
    /// which names the entry — and never carries the secret.
    #[test]
    fn a_bad_credential_spec_refuses_the_boot() {
        let err = config(&[("QUEEN_SQS_CREDENTIALS", &format!("AKID:{SECRET},AKIDONLY"))])
            .expect_err("refused");
        assert!(err.contains("QUEEN_SQS_CREDENTIALS"), "{err}");
        assert!(
            err.contains("entry 2") || err.contains("has no ':'"),
            "{err}"
        );
        assert!(!err.contains(SECRET), "the secret leaked: {err}");
    }

    /// An unset handle secret is generated, and two processes generate two
    /// different ones — which is exactly the fact the boot line has to say out
    /// loud, because it is what breaks the day a second replica appears.
    #[test]
    fn an_unset_handle_secret_is_generated_and_is_per_process() {
        let one = dev(&[]);
        let two = dev(&[]);
        assert!(one.handle_secret_generated);
        assert_eq!(one.handle_secret.len(), 32);
        assert_ne!(
            one.handle_secret, two.handle_secret,
            "a generated key is per process; two replicas cannot serve each other's handles"
        );
    }

    /// A key an attacker can enumerate is not a key: the handle it tags is a
    /// capability to delete any message in any queue.
    #[test]
    fn a_short_handle_secret_is_refused_rather_than_padded() {
        let err = dev_err(&[("QUEEN_SQS_HANDLE_SECRET", "short")]);
        assert!(err.contains("QUEEN_SQS_HANDLE_SECRET"), "{err}");
        assert!(err.contains("16"), "{err}");
        // The boundary is inclusive.
        let cfg = dev(&[("QUEEN_SQS_HANDLE_SECRET", "0123456789abcdef")]);
        assert_eq!(cfg.handle_secret.len(), MIN_HANDLE_SECRET_BYTES);
        assert!(!cfg.handle_secret_generated);
    }

    /// D1, pinned: `amortized` needs `maxPerPartition` on the broker's pop
    /// (C-SQS-1), which is not implemented, so the facade REFUSES TO START
    /// rather than accepting the name and serving `exact` under it. The refusal
    /// has to be self-contained — an operator reading a boot failure has the
    /// message and nothing else — so it names the missing core change and the
    /// document that decided it, and says what to do instead.
    ///
    /// When C-SQS-1 lands, THIS is the test that has to change, and the arm in
    /// [`Config::from_source`] it points at is the only code that has to.
    #[test]
    fn the_unserved_receive_mode_is_refused_at_boot() {
        for spelling in ["amortized", "amortised", "AMORTIZED", " Amortized "] {
            let err = dev_err(&[("QUEEN_SQS_RECEIVE_MODE", spelling)]);
            assert!(err.contains("QUEEN_SQS_RECEIVE_MODE"), "{err}");
            assert!(err.contains("C-SQS-1"), "{err}");
            assert!(err.contains("PLAN_QUEEN_SQS.md"), "{err}");
            assert!(err.contains("exact"), "{err}");
        }
        // The mode that CAN be served still boots, and unset is the same one.
        assert_eq!(
            dev(&[("QUEEN_SQS_RECEIVE_MODE", "exact")]).receive_mode,
            ReceiveMode::Exact
        );
        assert_eq!(dev(&[]).receive_mode, ReceiveMode::Exact);
        // A mode that is not a mode at all still says both names, and names the
        // reason `amortized` is not one of the two available answers.
        let err = dev_err(&[("QUEEN_SQS_RECEIVE_MODE", "batched")]);
        assert!(err.contains("C-SQS-1"), "{err}");
    }

    fn dev_err(extra: &[(&str, &str)]) -> String {
        let mut pairs = vec![("QUEEN_SQS_AUTH", "off")];
        pairs.extend_from_slice(extra);
        config(&pairs).expect_err("refused")
    }

    /// One table, one row per way a value can be wrong. Each message must name
    /// the variable, because that is the whole of what the operator gets.
    #[test]
    fn every_refusal_names_its_own_variable() {
        let cases: &[(&str, &str)] = &[
            ("QUEEN_SQS_LISTEN", "0.0.0.0"),
            ("QUEEN_SQS_LISTEN", "0.0.0.0:not-a-port"),
            ("QUEEN_SQS_LISTEN", ":9324"),
            ("QUEEN_SQS_LISTEN", "0.0.0.0:0"),
            ("QUEEN_SQS_AUTH", "maybe"),
            ("QUEEN_SQS_REGION", "eu west 1"),
            ("QUEEN_SQS_REGION", "../etc"),
            ("QUEEN_SQS_ACCOUNT", "0000/0000"),
            ("QUEEN_SQS_RECEIVE_MODE", "batched"),
            ("QUEEN_SQS_DEFAULT_PARTITIONS", "0"),
            ("QUEEN_SQS_DEFAULT_PARTITIONS", "100001"),
            ("QUEEN_SQS_DEFAULT_PARTITIONS", "sixty-four"),
            ("QUEEN_SQS_EMBEDDED", "probably"),
            ("QUEEN_SQS_SHUTDOWN_GRACE_MS", "600001"),
            ("QUEEN_SQS_SHUTDOWN_GRACE_MS", "-1"),
            ("QUEEN_URL", "queen.internal:6632"),
            ("QUEEN_URL", "ftp://queen.internal"),
            ("QUEEN_SQS_TLS_CERT", "/tls/cert.pem"),
            ("QUEEN_SQS_TLS_KEY", "/tls/key.pem"),
        ];
        for (variable, value) in cases {
            let err = dev_err(&[(variable, value)]);
            assert!(
                err.contains(variable),
                "{variable}={value} answered {err:?}, which does not name the variable"
            );
        }
    }

    /// A shutdown grace of zero is legal and means "do not wait": it is what a
    /// test rig and a `docker kill` both want, and refusing it would be a policy
    /// this file does not get to have.
    #[test]
    fn a_zero_grace_is_legal() {
        assert_eq!(
            dev(&[("QUEEN_SQS_SHUTDOWN_GRACE_MS", "0")]).shutdown_grace_ms,
            0
        );
    }

    /// Minting and parsing are the same two values, so a URL this facade issues
    /// parses back to the name it was issued for — and one from another account
    /// or another shape does not.
    #[test]
    fn urls_and_arns_round_trip_through_the_registrys_naming() {
        let cfg = dev(&[("QUEEN_SQS_ACCOUNT", "123456789012")]);
        let url = cfg.queue_url("sqs.example.test:9324", "orders");
        assert_eq!(url, "http://sqs.example.test:9324/123456789012/orders");
        assert_eq!(cfg.queue_name_of(&url).as_deref(), Some("orders"));
        assert_eq!(
            cfg.queue_arn("orders"),
            "arn:aws:sqs:queen-1:123456789012:orders"
        );
        // Another account's queue, and a traversal, are both `None` — the
        // action layer reports that as QueueDoesNotExist.
        assert_eq!(
            cfg.queue_name_of("http://sqs.example.test:9324/000000000000/orders"),
            None
        );
        assert_eq!(
            cfg.queue_name_of("http://sqs.example.test:9324/123456789012/../orders"),
            None
        );
        assert_eq!(cfg.queue_name_of("orders"), None);
    }

    /// The scheme in a minted URL is this listener's, and TLS is the only thing
    /// that changes it.
    #[test]
    fn tls_changes_the_scheme_every_queue_url_carries() {
        let plain = dev(&[]);
        assert_eq!(plain.scheme(), "http");
        assert!(plain.queue_url("h", "q").starts_with("http://"));
        let secure = dev(&[
            ("QUEEN_SQS_TLS_CERT", "/c.pem"),
            ("QUEEN_SQS_TLS_KEY", "/k.pem"),
        ]);
        assert_eq!(secure.scheme(), "https");
        assert!(secure.queue_url("h", "q").starts_with("https://"));
    }

    /// A rendered `Config` is a thing that ends up in a log. Neither secret may
    /// be in it.
    #[test]
    fn the_debug_rendering_carries_neither_secret() {
        let cfg = config(&[
            ("QUEEN_SQS_CREDENTIALS", &format!("AKID:{SECRET}:tok-1")),
            ("QUEEN_SQS_HANDLE_SECRET", "a shared sixteen+ byte key"),
            ("QUEEN_TOKEN", "the-process-bearer"),
        ])
        .expect("boots");
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("queen-1"), "{rendered}");
        assert!(!rendered.contains(SECRET), "{rendered}");
        assert!(!rendered.contains("the-process-bearer"), "{rendered}");
        assert!(
            !rendered.contains("a shared sixteen+ byte key"),
            "{rendered}"
        );
        assert!(!rendered.contains("tok-1"), "{rendered}");
        assert!(rendered.contains("<configured>"), "{rendered}");
    }

    #[test]
    fn the_two_modes_spell_themselves_for_the_boot_line() {
        assert_eq!(AuthMode::SigV4.as_str(), "sigv4");
        assert_eq!(AuthMode::Off.as_str(), "off");
        assert_eq!(ReceiveMode::Exact.as_str(), "exact");
        assert_eq!(ReceiveMode::Amortized.as_str(), "amortized");
    }
}
