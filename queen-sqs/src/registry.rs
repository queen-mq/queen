//! Everything durable this facade owns, in Queen's key/value store.
//!
//! CONTRACT. SQS has state Queen has no column for — a queue's `RedrivePolicy`,
//! its tags, an SNS topic, a subscription's filter policy, a FIFO batch's
//! delete-set, a message-move task's progress. All of it lives HERE, under one
//! namespace with a `qs:` key prefix, and NONE of it lives in this process. That
//! is the whole architectural claim of PLAN_QUEEN_SQS.md: *stateless: any
//! instance answers any request, a plain Service/LB in front is supported*.
//! Every in-memory copy in this module is a CACHE with a TTL, and every write
//! goes to the store under a CAS.
//!
//! The key space, all under namespace [`NS`]:
//!
//! ```text
//!   qs:q:<queue>              the SQS queue record: attributes, tags, created
//!   qs:qdel:<queue>           a QueueDeletedRecently tombstone (TTL = 60s)
//!   qs:purge:<queue>          a PurgeQueueInProgress window (TTL = 60s)
//!   qs:t:<topic>              an SNS topic
//!   qs:s:<topic>:<sub-id>     one subscription
//!   qs:ds:<partition>:<lease> a FIFO batch's delete-set (TTL = lease + slack)
//!   qs:rra:<queue>:<id>       a ReceiveRequestAttemptId (TTL = visibility)
//!   qs:mv:<task>              a message-move task's progress
//! ```
//!
//! `qs:qdel:` is not under `qs:q:` and cannot be: the fifth byte is `d` against
//! `:`, so the prefix read that answers ListQueues cannot see a tombstone and a
//! tombstone lookup cannot see a queue. [`the_queue_key_spaces_cannot_see_each_other`]
//! pins it rather than leaving it to be re-derived.
//!
//! Two rules the whole module is written to, both from the stored procedure:
//!
//!   * **Admin mutations are CAS.** CreateQueue is a `putIfAbsent`;
//!     SetQueueAttributes is a `put` with `expect` on the version it read. Two
//!     instances racing for the same name is the NORMAL case behind a load
//!     balancer, and last-writer-wins on an attribute set would silently drop
//!     one client's `RedrivePolicy`. A lost CAS is not an error a client should
//!     ever see: the loser re-merges onto the WINNER's record — which the answer
//!     already carries, so no second round trip — and writes once more. Losing
//!     twice is [`crate::error::ErrorKind::ServiceUnavailable`], the one code in
//!     the catalog whose meaning is "this may work if you send it again".
//!   * **Records are `forever`; everything derived has a TTL.** Expiry is
//!     mandatory on every write (024_kv.sql §5.1), so each key above declares
//!     which of the two it is, and a delete-set that outlived its lease would
//!     suppress a redelivery that is supposed to happen. The one TTL here is the
//!     tombstone's, and it IS the emulated 60-second window.
//!
//! ## The stored row, and what is NOT in it
//!
//! ```json
//! {"attributes": {"VisibilityTimeout": "30", "queen.partitions": "64"},
//!  "tags": {"team": "billing"},
//!  "createdTs": 1787011200000,
//!  "arn": "arn:aws:sqs:queen-1:000000000000:orders"}
//! ```
//!
//! The NAME is the key, and `partitions` and `fifo` are DERIVED on read — from
//! `queen.partitions` and from the `.fifo` suffix — rather than stored beside the
//! attributes. A record that stored them would have two sources of truth for one
//! fact, and a row whose `fifo` disagreed with its own name is a defect nothing
//! can repair: the suffix is the declaration (AWS's rule, and
//! [`crate::actions::queues`]'s).
//!
//! ## Errors, and why this module has its own
//!
//! A registry call fails in two unrelated ways and the caller does different
//! things with them: Queen was unreachable ([`RegistryError::Store`]), or the
//! REQUEST is refused by a rule this module owns — the name charset, the
//! attribute catalog, the attribute comparison behind `QueueAlreadyExists`
//! ([`RegistryError::Refused`]). Mapping a broker failure onto the client-visible
//! catalog is [`crate::error::SqsError::from_queen`]'s POLICY and stays there;
//! this module never second-guesses it, which is exactly what the two variants
//! and the `From` at the boundary buy. An action writes `registry.create(…)?`
//! and gets the right answer for both.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{ErrorKind, SqsError};
use crate::queen::{self, CredentialKey, KvAnswer, KvOp, KvRow, QueenApi, Result};

/// The key/value namespace. Validated by the stored procedure against
/// `^[a-z0-9][a-z0-9._-]{0,63}$`, so it is never a place to put anything a
/// client chose — the client's name goes in the KEY, after a `qs:` prefix.
pub const NS: &str = "queen-sqs";

/// How long a registry read is served from memory. Short: it is what stands
/// between a `SendMessage` and a `GetQueueAttributes` on every request, and a
/// long one would serve a deleted queue's attributes to a client that just
/// changed them through another instance.
pub const CACHE_TTL: Duration = Duration::from_secs(3);

/// The 60-second window AWS refuses a re-creation in, and every SDK's retry
/// behaviour depends on it existing.
pub const DELETE_COOLDOWN: Duration = Duration::from_secs(60);

/// The 60-second window AWS refuses a second `PurgeQueue` in.
///
/// It is the whole of this facade's emulation of the action's ASYNCHRONY. AWS
/// answers a purge immediately and empties the queue over the following minute;
/// here the delete-and-recreate is synchronous, so the only part of that
/// contract a client can observe is the refusal — and it is the part clients
/// are written against, because a fleet that purges on every boot must see
/// `PurgeQueueInProgress` rather than N successful purges of each other's
/// messages.
pub const PURGE_COOLDOWN: Duration = Duration::from_secs(60);

/// The suffix that DECLARES a FIFO queue. There is no separate attribute that
/// can disagree with it: `FifoQueue=true` and this suffix must both be present
/// or both absent, which is what [`validate_name`] enforces.
pub const FIFO_SUFFIX: &str = ".fifo";

/// The longest queue name AWS accepts, `.fifo` included.
pub const MAX_NAME_LEN: usize = 80;

/// The most tags one queue may carry (AWS's own cap).
pub const MAX_TAGS: usize = 50;

/// The width attribute, stamped into every standard queue's record at create.
/// It is the one attribute that is IMMUTABLE and load-bearing: partition counts
/// never shrink, and a queue whose width changed would strand messages on lanes
/// nothing pops any more. Stamping it means the record spells its own width even
/// when the operator later changes the process default.
pub const ATTR_PARTITIONS: &str = "queen.partitions";
/// The `.fifo` declaration, as an attribute.
pub const ATTR_FIFO: &str = "FifoQueue";

/// Records one call to [`Registry::list_queues`] will return at most. A cap and
/// not a page size: the walk below pages the store until it has this many.
pub const MAX_LISTED: i64 = 10_000;

/// Queue records kept in memory across every credential. Bounded, and the
/// coldest goes when a new one arrives — the same shape (and the same reason) as
/// [`crate::queen::Catalog`]'s bound.
const MAX_CACHED: usize = 4_096;

/// Pages one prefix walk will read before giving up. A bound and not a limit
/// anyone should reach; it exists because the loop's termination depends on the
/// broker's cursor advancing, and a loop whose exit condition lives in another
/// process needs one. It bounds every listing in this facade, SNS's included:
/// they all page through [`Registry::walk_rows`].
const MAX_PAGES: usize = 1_024;

const QUEUE_PREFIX: &str = "qs:q:";
const DELETED_PREFIX: &str = "qs:qdel:";
const PURGE_PREFIX: &str = "qs:purge:";
const TOPIC_PREFIX: &str = "qs:t:";
const SUBSCRIPTION_PREFIX: &str = "qs:s:";
const DELETE_SET_PREFIX: &str = "qs:ds:";
const RECEIVE_ATTEMPT_PREFIX: &str = "qs:rra:";
const MOVE_TASK_PREFIX: &str = "qs:mv:";
const MOVE_FENCE_PREFIX: &str = "qs:mvf:";

/// Attributes a client may set at any time. The four this facade only STORES —
/// `Policy`, `RedriveAllowPolicy` and the KMS pair — are here deliberately:
/// PLAN_QUEEN_SQS.md's first non-goal is that they are *accepted and stored,
/// never enforced*, and an attribute the facade refused to store would break
/// Terraform and MassTransit, which set them unconditionally.
const MUTABLE: &[&str] = &[
    "VisibilityTimeout",
    "MessageRetentionPeriod",
    "MaximumMessageSize",
    "DelaySeconds",
    "ReceiveMessageWaitTimeSeconds",
    "RedrivePolicy",
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "FifoThroughputLimit",
    "Policy",
    "RedriveAllowPolicy",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
    "SqsManagedSseEnabled",
    "queen.dedupWindowSeconds",
];

/// Attributes fixed at CreateQueue. SetQueueAttributes answers
/// `InvalidAttributeName` for these, which is what AWS answers for an attribute
/// that exists and cannot be set.
const CREATE_ONLY: &[&str] = &[ATTR_FIFO, ATTR_PARTITIONS];

/// `(attribute, min, max)` for every attribute whose value is a bounded integer.
/// A table rather than a match, because it is DATA: the numbers are AWS's
/// documented ranges, except `MaximumMessageSize`'s ceiling (raised to 1 MiB in
/// 2025-08) and the two `queen.*` extensions.
const RANGES: &[(&str, i64, i64)] = &[
    ("VisibilityTimeout", 0, 43_200),
    ("MessageRetentionPeriod", 60, 1_209_600),
    ("MaximumMessageSize", 1_024, 1_048_576),
    ("DelaySeconds", 0, 900),
    ("ReceiveMessageWaitTimeSeconds", 0, 20),
    ("KmsDataKeyReusePeriodSeconds", 60, 86_400),
    (ATTR_PARTITIONS, 1, 100_000),
    ("queen.dedupWindowSeconds", 1, 31_536_000),
];

/// Attributes whose value must be `true` or `false`.
const BOOLEANS: &[&str] = &[
    ATTR_FIFO,
    "ContentBasedDeduplication",
    "SqsManagedSseEnabled",
];

// ------------------------------------------------------------------- errors

/// What a registry call failed with. See the module header for why there are
/// two.
///
/// Not `PartialEq`: [`queen::Error`] is not, deliberately — comparing two
/// transport failures for equality compares their MESSAGES, and a test that
/// passed because two unrelated failures printed the same string is a test that
/// proves nothing. [`RegistryError::kind`] is what an assertion wants.
#[derive(Debug, Clone)]
pub enum RegistryError {
    /// The store itself. Mapped to the client-visible catalog by
    /// [`SqsError::from_queen`] at the boundary and nowhere else.
    Store(queen::Error),
    /// A rule this module owns said no.
    Refused(SqsError),
}

pub type RegistryResult<T> = std::result::Result<T, RegistryError>;

impl RegistryError {
    /// The client-visible code, for a caller that wants to branch on one without
    /// rendering anything. `None` for a store failure, whose code is
    /// [`SqsError::from_queen`]'s to decide.
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            RegistryError::Store(_) => None,
            RegistryError::Refused(e) => Some(e.kind),
        }
    }
}

impl From<queen::Error> for RegistryError {
    fn from(e: queen::Error) -> RegistryError {
        RegistryError::Store(e)
    }
}

impl From<SqsError> for RegistryError {
    fn from(e: SqsError) -> RegistryError {
        RegistryError::Refused(e)
    }
}

impl From<RegistryError> for SqsError {
    fn from(e: RegistryError) -> SqsError {
        match e {
            RegistryError::Store(e) => SqsError::from_queen(&e),
            RegistryError::Refused(e) => e,
        }
    }
}

/// Deliberately NOT the wire spelling. This is what a log line shows, and a log
/// line that went through the protocol renderer would say `Sender` in one
/// deployment and `AWS.SimpleQueueService.NonExistentQueue` in another for the
/// same event.
impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Store(e) => write!(f, "kv: {e}"),
            RegistryError::Refused(e) => write!(f, "{:?}: {}", e.kind, e.message),
        }
    }
}

impl std::error::Error for RegistryError {}

/// One refusal. Built by field rather than through [`SqsError::with`] so that
/// every refusal in this module carries a call-site sentence and no
/// `retry_after_ms` in ONE place — the field is Queen's `Retry-After` and a
/// registry rule has none.
///
/// `pub(crate)` for the SNS half of this store ([`crate::sns::registry`]), which
/// refuses for the same reasons and must not grow a second constructor.
pub(crate) fn refuse(kind: ErrorKind, message: impl Into<String>) -> RegistryError {
    RegistryError::Refused(SqsError {
        kind,
        message: message.into(),
        retry_after_ms: None,
    })
}

/// The one sentence a missing queue gets, from every entry point, so a client
/// cannot tell which call could not find it.
fn missing(name: &str) -> RegistryError {
    refuse(
        ErrorKind::QueueDoesNotExist,
        format!("The specified queue does not exist: {name}"),
    )
}

// ------------------------------------------------------------------ records

/// One SQS queue, as this facade knows it.
///
/// `attributes` is kept as the SQS attribute map — the client's own vocabulary,
/// verbatim — and never as parsed fields. `GetQueueAttributes` has to answer
/// what was set, including the attributes this facade accepts and does not
/// enforce (`Policy`, the KMS ones), and a record of parsed fields cannot.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueueRecord {
    pub name: String,
    /// The SQS attribute map: `VisibilityTimeout`, `MessageRetentionPeriod`,
    /// `RedrivePolicy`, `Policy`, `FifoQueue`, `ContentBasedDeduplication`, the
    /// `queen.*` extensions.
    pub attributes: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    /// The synthesized lane width, fixed at CreateQueue and IMMUTABLE after:
    /// partition counts never shrink, and a queue whose width changed would
    /// strand messages on lanes nothing pops any more. DERIVED from
    /// [`ATTR_PARTITIONS`], and 0 for a FIFO queue, which synthesizes no lanes at
    /// all — a lane there IS a `MessageGroupId`.
    pub partitions: u32,
    /// Derived from the `.fifo` suffix, which is the whole declaration.
    pub fifo: bool,
    /// Epoch milliseconds — `CreatedTimestamp`.
    pub created_ms: i64,
    /// Epoch milliseconds of the last write to this record —
    /// `LastModifiedTimestamp`, which AWS answers on every queue.
    ///
    /// A row written before this field existed reads back as its own
    /// `createdTs`, which is the truth for a queue nobody has changed and the
    /// only honest guess for one that was.
    pub modified_ms: i64,
    /// `arn:aws:sqs:<region>:<account>:<name>`, as it was minted at create.
    /// STORED rather than re-derived on read: it pins the account the queue was
    /// created under, so an operator who changes `QUEEN_SQS_ACCOUNT` does not
    /// silently re-label every existing queue's ARN in a client's config.
    pub arn: String,
    /// The KV version this record was read at, for the CAS on the next write. 0
    /// means "was not there".
    pub version: i64,
}

impl QueueRecord {
    fn to_value(&self) -> serde_json::Value {
        serde_json::json!({
            "attributes": self.attributes,
            "tags": self.tags,
            "createdTs": self.created_ms,
            "modifiedTs": self.modified_ms,
            "arn": self.arn,
        })
    }

    /// Read a stored row back.
    ///
    /// LENIENT, and for the reason kafka's group index is: the KEY is the
    /// existence claim, so a row with a field this facade cannot read is still a
    /// queue that exists, and dropping it would make a queue clients are sending
    /// to invisible to ListQueues. Anything unreadable is named in the log and
    /// defaulted, never dropped.
    fn from_value(name: &str, value: &serde_json::Value, version: i64) -> QueueRecord {
        let attributes = string_map(value.get("attributes"), name, "attributes");
        let partitions = partitions_of(name, &attributes);
        let created_ms = value
            .get("createdTs")
            .and_then(|t| t.as_i64())
            .unwrap_or_default();
        QueueRecord {
            name: name.to_string(),
            partitions,
            fifo: is_fifo(name),
            tags: string_map(value.get("tags"), name, "tags"),
            created_ms,
            modified_ms: value
                .get("modifiedTs")
                .and_then(|t| t.as_i64())
                .unwrap_or(created_ms),
            arn: value
                .get("arn")
                .and_then(|a| a.as_str())
                .unwrap_or_default()
                .to_string(),
            attributes,
            version,
        }
    }
}

/// A stored `{"k": "v"}` object as a map. A value that is not a string is
/// corruption — this module only ever writes strings — and is skipped with its
/// key named, rather than coerced into one. Shared with
/// [`crate::sns::registry`], whose topics and subscriptions store the same shape
/// and read it back under the same rule.
pub(crate) fn string_map(
    value: Option<&serde_json::Value>,
    name: &str,
    what: &str,
) -> BTreeMap<String, String> {
    let Some(object) = value.and_then(|v| v.as_object()) else {
        return BTreeMap::new();
    };
    let mut out = BTreeMap::new();
    for (key, value) in object {
        match value.as_str() {
            Some(v) => {
                out.insert(key.clone(), v.to_string());
            }
            None => {
                tracing::warn!(target: "sqs", queue = %name, what, key = %key, "unreadable registry value")
            }
        }
    }
    out
}

/// Whether a queue name is a FIFO queue's.
pub fn is_fifo(name: &str) -> bool {
    name.ends_with(FIFO_SUFFIX) && name.len() > FIFO_SUFFIX.len()
}

/// The lane width a record's attributes declare. 0 for FIFO, where the lane is
/// the `MessageGroupId` and no width is synthesized.
fn partitions_of(name: &str, attributes: &BTreeMap<String, String>) -> u32 {
    if is_fifo(name) {
        return 0;
    }
    match attributes.get(ATTR_PARTITIONS).map(|p| p.parse::<u32>()) {
        Some(Ok(p)) if p > 0 => p,
        // A standard queue whose record does not spell its own width: written by
        // something that is not this facade, or by a version of it that did not
        // stamp. The process default is the only answer available and it is
        // named, because popping a width the queue was not created with strands
        // whatever is on the lanes above it.
        other => {
            tracing::warn!(
                target: "sqs",
                queue = %name,
                found = ?other.map(|p| p.is_ok()),
                "queue record has no readable width; using the process default"
            );
            crate::config::DEFAULT_PARTITIONS
        }
    }
}

// The SNS records — `TopicRecord` and `SubscriptionRecord` — are in
// [`crate::sns::registry`], with the reads and writes that produce them.

/// What a conditional write answered: `Ok` is the record as it now stands in the
/// store, `Err` is the WINNER's — which the answer carries, so a caller that
/// lost has what it needs to compare or to re-merge without a second read.
///
/// The loser is BOXED. A `QueueRecord` is three maps and four scalars, so a
/// bare `Result` here makes every successful write pay the size of a record
/// nobody read; losing a compare-and-set is the cold path, and one allocation on
/// it is the right side of that trade.
pub type Cas = std::result::Result<QueueRecord, Box<QueueRecord>>;

/// One page of [`Registry::list`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Page {
    pub queues: Vec<QueueRecord>,
    /// The opaque cursor a client hands back as `NextToken`. `None` is the end
    /// of the listing, and is the ONLY thing that means it.
    pub next_token: Option<String>,
}

// ------------------------------------------------------------------- naming

/// The account and region every queue URL and ARN is minted from.
///
/// It lives here rather than in [`crate::config`] because the registry is the
/// module that has to run it BACKWARDS: `QueueUrl` is a client-supplied string
/// on every message action, so parsing one is parsing untrusted input against
/// the same two values that minted it. `Config::queue_url`, `queue_arn` and
/// `queue_name_of` are one-line delegations to the three methods below.
///
/// SNS's two ARN spellings — a topic's and a subscription's — are minted and
/// parsed from this same pair, by a second `impl` block in [`crate::sns`]: one
/// deployment is one region and one account whichever service is asking, and the
/// SNS shapes belong with the SNS vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Naming {
    /// A LABEL, not an AWS region: it identifies this deployment, and it appears
    /// in ARNs and in the SigV4 credential scope.
    pub region: String,
    pub account: String,
}

impl Naming {
    pub fn new(region: &str, account: &str) -> Naming {
        Naming {
            region: region.to_string(),
            account: account.to_string(),
        }
    }

    /// `arn:aws:sqs:<region>:<account>:<name>`.
    pub fn arn(&self, name: &str) -> String {
        format!("arn:aws:sqs:{}:{}:{name}", self.region, self.account)
    }

    /// `<scheme>://<host>/<account>/<name>`.
    ///
    /// The scheme and host are the CLIENT's, not this process's: behind a
    /// TLS-terminating load balancer the facade is plain HTTP on a private
    /// address while every client speaks https to a public name, and a URL minted
    /// from the bound socket would send the fleet somewhere it cannot reach.
    pub fn url(&self, scheme: &str, host: &str, name: &str) -> String {
        format!("{scheme}://{host}/{}/{name}", self.account)
    }

    /// The queue name inside an ARN, or `None` when the ARN is not one this
    /// deployment would have minted.
    ///
    /// The mirror of [`Naming::arn`], and it is run backwards on untrusted
    /// input: `RedrivePolicy.deadLetterTargetArn`, `SourceArn` and
    /// `DestinationArn` are all strings a client composed. Six segments
    /// exactly, `sqs` as the service, and the REGION and ACCOUNT must be this
    /// deployment's — which is also AWS's own rule, since a dead-letter target
    /// must live in the same account and Region as its source. The partition
    /// segment (`aws`, `aws-cn`, `aws-us-gov`) is accepted whatever it says: it
    /// names an AWS realm this deployment is not in, and refusing a client that
    /// composed the ARN from its own configured partition would fail a queue
    /// name that is otherwise exactly right.
    ///
    /// A queue name cannot contain a colon ([`name_is_wellformed`]), so the
    /// split is unambiguous and needs no rejoining.
    pub fn name_of_arn(&self, arn: &str) -> Option<String> {
        let segments: Vec<&str> = arn.split(':').collect();
        let ["arn", _partition, "sqs", region, account, name] = segments.as_slice() else {
            return None;
        };
        if *region != self.region || *account != self.account || !name_is_wellformed(name) {
            return None;
        }
        Some((*name).to_string())
    }

    /// The queue name inside a URL, or `None` when the URL is not one of ours.
    ///
    /// Tolerant about the HOST — any host, any scheme, any path prefix a reverse
    /// proxy prepended, because the client got the URL from us and may have
    /// rewritten the authority — and strict about the two segments that carry
    /// meaning: the account must be this deployment's, and the name must be a
    /// name AWS would have accepted. That pair is what makes `..`, a NUL, an
    /// absolute path and another account's queue all answer `None`, which the
    /// caller reports as `QueueDoesNotExist`.
    ///
    /// A bare name with no account segment is NOT accepted here: it is
    /// indistinguishable from a path this facade never minted, and the decision
    /// to treat one as a queue name belongs to the action layer.
    pub fn name_of(&self, queue_url: &str) -> Option<String> {
        let without_query = queue_url.split(['?', '#']).next().unwrap_or_default();
        let path = match without_query.split_once("://") {
            Some((_, authority_and_path)) => authority_and_path
                .split_once('/')
                .map(|(_, path)| path)
                .unwrap_or_default(),
            None => without_query,
        };
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let [.., account, name] = segments.as_slice() else {
            return None;
        };
        if *account != self.account || !name_is_wellformed(name) {
            return None;
        }
        Some((*name).to_string())
    }
}

// ---------------------------------------------------------------- the store

/// The store, plus the short-lived cache in front of it.
pub struct Registry {
    /// `pub(crate)` for the SNS half of this store ([`crate::sns::registry`]),
    /// which is a second `impl` block on this type rather than a second object:
    /// one connection to Queen, one credential, one place a facade's durable
    /// state is reached from.
    pub(crate) api: Arc<dyn QueenApi>,
    ttl: Duration,
    /// Keyed by CREDENTIAL as well as by name, and that is not tidiness: the
    /// namespace is per-tenant on the broker side, so two tenants have different
    /// queues under one name, and a cache keyed by name alone would serve one
    /// tenant the other's attributes. Never held across an await.
    cache: Mutex<HashMap<(CredentialKey, String), Cached>>,
    /// One topic's subscription LIST, for the publish path
    /// ([`crate::sns::registry::Registry::subscriptions_cached`]). A second map
    /// rather than a second kind of entry in the first, because the two are
    /// different objects with different invalidation: a queue record is
    /// forgotten when the queue is deleted, and a subscription list is forgotten
    /// whenever THIS instance changes one.
    subscriptions: Mutex<HashMap<(CredentialKey, String), CachedSubscriptions>>,
}

struct Cached {
    record: QueueRecord,
    at: Instant,
}

/// `pub(crate)` for the SNS half of this store, which fills and reads it.
pub(crate) struct CachedSubscriptions {
    pub(crate) records: Arc<Vec<crate::sns::registry::SubscriptionRecord>>,
    pub(crate) at: Instant,
}

impl Registry {
    pub fn new(api: Arc<dyn QueenApi>) -> Registry {
        Registry::with_ttl(api, CACHE_TTL)
    }

    /// Same, with an explicit TTL. `Duration::ZERO` turns the cache off, which is
    /// what a test that counts calls to Queen wants.
    pub fn with_ttl(api: Arc<dyn QueenApi>, ttl: Duration) -> Registry {
        Registry {
            api,
            ttl,
            cache: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(HashMap::new()),
        }
    }

    /// How long a cached read stays fresh. Read by the SNS half, which caches a
    /// subscription list under the same window rather than inventing a second
    /// one.
    pub(crate) fn ttl(&self) -> Duration {
        self.ttl
    }

    /// The subscription cache itself, for the two functions in
    /// [`crate::sns::registry`] that fill and clear it. The MAP is here because
    /// it is one more thing this object owns and must drop with it; the POLICY
    /// is there, with the records it holds.
    pub(crate) fn subscription_cache(
        &self,
    ) -> &Mutex<HashMap<(CredentialKey, String), CachedSubscriptions>> {
        &self.subscriptions
    }

    // ------------------------------------------------------------- key space

    pub fn key_queue(name: &str) -> String {
        format!("{QUEUE_PREFIX}{}", escape(name))
    }

    pub fn key_topic(name: &str) -> String {
        format!("{TOPIC_PREFIX}{}", escape(name))
    }

    /// Every topic, as a prefix. There is no per-account segment to add: one
    /// deployment is one account ([`Naming`]).
    pub fn key_topics() -> &'static str {
        TOPIC_PREFIX
    }

    /// The topic a `qs:t:` key names, or `None` if the key is not one of ours.
    pub fn topic_of_key(key: &str) -> Option<String> {
        key.strip_prefix(TOPIC_PREFIX).map(unescape)
    }

    pub fn key_subscription(topic: &str, id: &str) -> String {
        format!("{SUBSCRIPTION_PREFIX}{}:{}", escape(topic), escape(id))
    }

    /// Every subscription of ONE topic, as a prefix — the read behind
    /// `ListSubscriptionsByTopic`, and the reason no separate index exists. The
    /// trailing separator is what makes it exact: a topic name is escaped, so no
    /// name can contain the `:` that ends this prefix, and `events` cannot reach
    /// `events-2`'s subscriptions.
    pub fn key_subscriptions(topic: &str) -> String {
        format!("{SUBSCRIPTION_PREFIX}{}:", escape(topic))
    }

    /// Every subscription of every topic — `ListSubscriptions`.
    pub fn key_all_subscriptions() -> &'static str {
        SUBSCRIPTION_PREFIX
    }

    /// The `(topic, id)` a `qs:s:` key names. The two halves are each escaped, so
    /// the single unescaped `:` between them is the separator and the split is
    /// unambiguous.
    pub fn subscription_of_key(key: &str) -> Option<(String, String)> {
        let rest = key.strip_prefix(SUBSCRIPTION_PREFIX)?;
        let (topic, id) = rest.split_once(':')?;
        Some((unescape(topic), unescape(id)))
    }

    pub fn key_delete_set(partition_id: &str, lease_id: &str) -> String {
        format!(
            "{DELETE_SET_PREFIX}{}:{}",
            escape(partition_id),
            escape(lease_id)
        )
    }

    /// Every delete-set of ONE lane, as a prefix. The separator is what makes it
    /// exact: a partition id is escaped, so no id can contain the `:` that ends
    /// this prefix, and a lane's key range cannot reach into another lane whose
    /// id merely starts with the same bytes.
    pub fn key_delete_sets(partition_id: &str) -> String {
        format!("{DELETE_SET_PREFIX}{}:", escape(partition_id))
    }

    /// What one `ReceiveRequestAttemptId` answered. Keyed by QUEUE as well as by
    /// id: the id is the client's own string, two queues of one account may
    /// reuse it, and a receive that replayed another queue's answer would hand a
    /// consumer receipt handles for messages it cannot delete.
    pub fn key_receive_attempt(queue: &str, id: &str) -> String {
        format!("{RECEIVE_ATTEMPT_PREFIX}{}:{}", escape(queue), escape(id))
    }

    pub fn key_deleted_recently(name: &str) -> String {
        format!("{DELETED_PREFIX}{}", escape(name))
    }

    /// One message-move task, under `qs:mv:<source>:<countdown>:<id>`.
    ///
    /// PLAN_QUEEN_SQS.md writes the namespace as `qs:mv:` and says nothing
    /// about what follows it; the three segments are what
    /// `ListMessageMoveTasks` needs and a bare id cannot give:
    ///
    ///   * the SOURCE first, because a listing is per-source and a prefix read
    ///     is the only scan the store has — with an id-keyed space, listing one
    ///     queue's tasks would mean reading every task in the deployment;
    ///   * a COUNTDOWN — `i64::MAX - started_ms`, zero-padded hex — because the
    ///     store reads a prefix in BYTE order and AWS lists tasks most recent
    ///     first, so the order has to be in the key rather than in a sort of a
    ///     page that may have been truncated;
    ///   * the id LAST, so two tasks that started in the same millisecond are
    ///     two rows.
    ///
    /// A negative instant is clamped rather than subtracted: a clock before the
    /// epoch would overflow the countdown, and a key that panicked in a debug
    /// build over a host's clock is not a trade worth making for a case that
    /// orders nothing anyway.
    pub fn key_move_task(source: &str, started_ms: i64, id: &str) -> String {
        format!(
            "{MOVE_TASK_PREFIX}{}:{:016x}:{}",
            escape(source),
            (i64::MAX - started_ms.max(0)) as u64,
            escape(id)
        )
    }

    /// Every move task of ONE source, as a prefix. The trailing separator is
    /// what keeps `orders-dlq` from reading `orders-dlq-2`'s tasks: a source
    /// name is escaped, so no name can contain the `:` that ends this prefix.
    pub fn key_move_tasks(source: &str) -> String {
        format!("{MOVE_TASK_PREFIX}{}:", escape(source))
    }

    /// The one-task-per-source fence. A separate key from the task itself
    /// because it is claimed BEFORE the task exists and outlives no task: it is
    /// `putIfAbsent` at start and deleted at the end, which is what makes "only
    /// one active task per source" a property of the store rather than of one
    /// instance's memory.
    pub fn key_move_fence(source: &str) -> String {
        format!("{MOVE_FENCE_PREFIX}{}", escape(source))
    }

    pub fn key_purging(name: &str) -> String {
        format!("{PURGE_PREFIX}{}", escape(name))
    }

    /// The queue a `qs:q:` key names, or `None` if the key is not one of ours.
    pub fn queue_of_key(key: &str) -> Option<String> {
        key.strip_prefix(QUEUE_PREFIX).map(unescape)
    }

    // ------------------------------------------------------- the SQS surface

    /// `CreateQueue`.
    ///
    /// Three refusals and one subtlety, all AWS's:
    ///
    ///   * the name rules and the `.fifo`/`FifoQueue` agreement
    ///     ([`validate_name`]),
    ///   * the attribute catalog and its ranges ([`validate_attributes`]),
    ///   * the 60-second tombstone, which is `QueueDeletedRecently`.
    ///
    /// The subtlety is that **a name that is taken is usually a success**, and
    /// the comparison behind it is ONE-DIRECTIONAL. AWS's own description of the
    /// error is the specific sentence: `QueueNameExists` is returned *"only if
    /// the request includes attributes whose values differ from those of the
    /// existing queue"* (API_CreateQueue, Errors). So the request's attributes
    /// are read against the existing queue's and never the other way round: an
    /// attribute the request does not name cannot differ from anything, and a
    /// create that names none of them is the plain idempotent create every
    /// framework performs at worker start-up.
    ///
    /// Two consequences worth spelling, because both were defects
    /// (`compat/M0_SMOKE.md` D1):
    ///
    ///   * the stamped [`ATTR_PARTITIONS`] is NOT part of the comparison. It is a
    ///     default this facade implies, not a parameter the client supplied, and
    ///     comparing it would refuse every create that let the width default while
    ///     the process default moved.
    ///   * the existing side is the queue's EFFECTIVE attributes — what
    ///     `GetQueueAttributes` answers, defaults included — and not only what the
    ///     record stores. On AWS every queue has a value for `VisibilityTimeout`,
    ///     so a request that supplies AWS's own default against a queue created
    ///     bare supplies nothing that differs.
    ///
    /// `tags` are NOT compared and NOT applied when the queue is already there.
    /// They are not attributes (SQS gives them their own request member and their
    /// own three actions), the error's sentence names attributes only, and a
    /// create that silently retagged a queue Terraform owns would be a change
    /// nobody asked for. `TagQueue` is the action that changes tags.
    pub async fn create(
        &self,
        name: &str,
        attributes: &BTreeMap<String, String>,
        tags: &BTreeMap<String, String>,
        naming: &Naming,
        default_partitions: u32,
        token: Option<&str>,
    ) -> RegistryResult<QueueRecord> {
        // The ONE name refusal an existing queue answers instead of the client,
        // so the only one held rather than returned: a `.fifo` suffix with no
        // `FifoQueue=true` is a bad CREATE — a standard queue's name cannot hold
        // a dot, so the request describes nothing that could exist — and a fine
        // RE-CREATE, because the queue's type is already declared by its own name
        // and the request supplies no attribute that differs. It is the shape an
        // SDK that remembers the name but not the attribute sends, and the lookup
        // it costs is paid only on that shape.
        let deferred = match validate_name(name, attributes) {
            Ok(()) => None,
            Err(refusal) if is_fifo(name) && name_is_wellformed(name) => Some(refusal),
            Err(refusal) => return Err(refusal),
        };
        validate_attributes(attributes, When::Create)?;
        validate_tags(tags)?;
        if let Some(refusal) = deferred {
            return match self.queue(name, token).await? {
                Some(existing) => existing_or_conflict(existing, attributes),
                None => Err(refusal),
            };
        }

        let mut effective = attributes.clone();
        if !is_fifo(name) {
            effective
                .entry(ATTR_PARTITIONS.to_string())
                .or_insert_with(|| default_partitions.to_string());
        }
        let now = crate::obs::now_epoch_ms();
        let record = QueueRecord {
            name: name.to_string(),
            partitions: partitions_of(name, &effective),
            fifo: is_fifo(name),
            attributes: effective,
            tags: tags.clone(),
            created_ms: now,
            modified_ms: now,
            arn: naming.arn(name),
            version: 0,
        };
        // Before the first call, so a request the store would refuse costs no
        // round trip and reads as the client's error that it is.
        guard_size(&record)?;

        if self.deleted_recently(name, token).await? {
            return Err(refuse(
                ErrorKind::QueueDeletedRecently,
                format!(
                    "You must wait {} seconds after deleting a queue before you can create \
                     another with the same name: {name}",
                    DELETE_COOLDOWN.as_secs()
                ),
            ));
        }

        match self.create_queue(&record, token).await? {
            Ok(stored) => Ok(stored),
            // A winner with no version is a store that said "the key is taken"
            // and did not answer with what is under it ([`Registry::settle`]'s
            // own guard). There is nothing to compare, and answering the URL of a
            // record this call never read would hand the client a queue whose
            // options the action layer then re-`/configure`s from an empty
            // record — every column back to a default.
            Err(winner) if winner.version == 0 => Err(refuse(
                ErrorKind::QueueAlreadyExists,
                format!("A queue already exists with the name {name}"),
            )),
            Err(winner) => existing_or_conflict(*winner, attributes),
        }
    }

    /// The record for one queue. `QueueDoesNotExist` when it is not there, which
    /// is the answer every message action needs before it touches the broker.
    pub async fn require(&self, name: &str, token: Option<&str>) -> RegistryResult<QueueRecord> {
        match self.queue(name, token).await? {
            Some(record) => Ok(record),
            None => Err(missing(name)),
        }
    }

    /// `SetQueueAttributes`: merge, never replace. AWS has no way to REMOVE an
    /// attribute, so a change that names three attributes leaves the other seven
    /// exactly as they were.
    pub async fn set_attributes(
        &self,
        name: &str,
        changes: &BTreeMap<String, String>,
        token: Option<&str>,
    ) -> RegistryResult<QueueRecord> {
        validate_attributes(changes, When::Set)?;
        self.mutate(name, token, |record| {
            for (key, value) in changes {
                record.attributes.insert(key.clone(), value.clone());
            }
            Ok(())
        })
        .await
    }

    /// `ListQueueTags`.
    pub async fn tags(
        &self,
        name: &str,
        token: Option<&str>,
    ) -> RegistryResult<BTreeMap<String, String>> {
        Ok(self.require(name, token).await?.tags)
    }

    /// `TagQueue`. Tags live in the record and never reach Queen.
    pub async fn tag(
        &self,
        name: &str,
        tags: &BTreeMap<String, String>,
        token: Option<&str>,
    ) -> RegistryResult<QueueRecord> {
        self.mutate(name, token, |record| {
            for (key, value) in tags {
                record.tags.insert(key.clone(), value.clone());
            }
            validate_tags(&record.tags)
        })
        .await
    }

    /// `UntagQueue`. A key that is not there is not an error: AWS's untag is
    /// idempotent, and a fleet re-running its provisioning would otherwise fail
    /// the second time.
    pub async fn untag(
        &self,
        name: &str,
        keys: &[String],
        token: Option<&str>,
    ) -> RegistryResult<QueueRecord> {
        self.mutate(name, token, |record| {
            for key in keys {
                record.tags.remove(key);
            }
            Ok(())
        })
        .await
    }

    /// `DeleteQueue`, plus the tombstone that makes a too-fast re-create
    /// `QueueDeletedRecently`.
    pub async fn delete(&self, name: &str, token: Option<&str>) -> RegistryResult<()> {
        match self.delete_queue(name, token).await? {
            true => Ok(()),
            false => Err(missing(name)),
        }
    }

    /// `ListQueues`: one page, with the opaque `NextToken` a client hands back.
    ///
    /// The token is the base64 of the LAST NAME on the page rather than a store
    /// cursor: it survives this process, every other instance decodes it the same
    /// way, and it cannot be used to read outside the prefix — it is applied as an
    /// exclusive `after` on a prefix read that is itself scoped by the caller's
    /// own credential.
    pub async fn list(
        &self,
        prefix: &str,
        limit: usize,
        next_token: Option<&str>,
        token: Option<&str>,
    ) -> RegistryResult<Page> {
        let after = match next_token {
            None => None,
            Some(cursor) => {
                let name = decode_token(cursor).ok_or_else(|| {
                    refuse(
                        ErrorKind::InvalidParameterValue,
                        "Invalid or expired NextToken",
                    )
                })?;
                Some(Registry::key_queue(&name))
            }
        };
        let (queues, more) = self.walk(prefix, limit.max(1), after, token).await?;
        let next_token = more
            .then(|| queues.last().map(|q| encode_token(&q.name)))
            .flatten();
        Ok(Page { queues, next_token })
    }

    // --------------------------------------------------------- the raw store

    /// The record for one queue, or `None`. `Ok(None)` is a queue this facade
    /// does not know, which is `QueueDoesNotExist` — distinct from an `Err`,
    /// which is Queen being unreachable and must never read as "no such queue".
    ///
    /// Served from the cache when it is fresh. Never used by a write path, which
    /// needs a version that is CURRENT.
    pub async fn queue(&self, name: &str, token: Option<&str>) -> Result<Option<QueueRecord>> {
        match self.cached(token, name) {
            Some(hit) => Ok(Some(hit)),
            None => self.queue_fresh(name, token).await,
        }
    }

    /// The same read, always from Queen. This is what a CAS reads with: a cached
    /// version is a version that may already have been superseded, and starting a
    /// compare-and-set from one spends the retry budget losing a race it was
    /// never in.
    pub async fn queue_fresh(
        &self,
        name: &str,
        token: Option<&str>,
    ) -> Result<Option<QueueRecord>> {
        let key = Registry::key_queue(name);
        let answer = one(self.api.kv(&[KvOp::get(NS, &key)], token).await?)?;
        if !answer.found {
            // A MISS is never cached: an SDK creates a queue and sends to it in
            // the same second, often through a different instance, and a
            // negative entry would answer QueueDoesNotExist for a queue that is
            // demonstrably there.
            return Ok(None);
        }
        let record = QueueRecord::from_value(name, &answer.value, answer.version);
        self.remember(token, &record);
        Ok(Some(record))
    }

    /// The records for MANY queues, in as few round trips as the store allows.
    ///
    /// The fan-out's read ([`crate::sns::publish`]): a publish resolves one
    /// queue per matched subscription, and doing that one `get` at a time is N
    /// serial round trips inside one client request — a 200-subscriber topic
    /// paying two hundred of them, every time the cache turns over, with the
    /// publisher blocked on all of them. `getMany` takes a key list, so the
    /// whole resolution is normally ONE call.
    ///
    /// ALWAYS FRESH, and that is the other half of why it exists. A cached
    /// record for a queue another instance deleted would put that queue in the
    /// fan-out, and the broker's transaction LAZILY PROVISIONS a queue a push
    /// names (005_log_ack.sql: the `queen.queues` insert before the push loop) —
    /// so the delivery would re-create a Queen queue no registry record owns,
    /// which `CreateQueue` then refuses to adopt for ever and `ReceiveMessage`
    /// answers `QueueDoesNotExist` for. Every hit is REMEMBERED, so the send
    /// path does not pay for the read a second time.
    ///
    /// A name that is not there is simply absent from the answer, and no
    /// negative entry is cached ([`Registry::queue_fresh`]'s rule).
    pub async fn queues_fresh(
        &self,
        names: &[String],
        token: Option<&str>,
    ) -> Result<HashMap<String, QueueRecord>> {
        /// Keys one read asks for. Inside [`queen::MAX_KV_KEYS_PER_CALL`], and
        /// short of it because the call also has a byte budget: a record is
        /// small, but a `Policy` is client-supplied and can be 64 KiB.
        const KEYS_PER_READ: usize = 256;

        let mut pending: Vec<String> = names.to_vec();
        pending.sort_unstable();
        pending.dedup();
        let mut out = HashMap::with_capacity(pending.len());
        let mut window = KEYS_PER_READ;
        while !pending.is_empty() {
            let take = pending.len().min(window);
            let keys: Vec<String> = pending[..take]
                .iter()
                .map(|name| Registry::key_queue(name))
                .collect();
            let answer = one(self.api.kv(&[KvOp::get_many(NS, &keys)], token).await?)?;
            let mut answered: std::collections::HashSet<String> = answer
                .rows
                .iter()
                .map(|row| row.key.clone())
                .chain(answer.missing.iter().cloned())
                .collect();
            for row in &answer.rows {
                let Some(name) = Registry::queue_of_key(&row.key) else {
                    // A key this facade did not mint cannot be a queue, and
                    // leaving it in `pending` would loop for ever.
                    answered.insert(row.key.clone());
                    continue;
                };
                let record = QueueRecord::from_value(&name, &row.value, row.version);
                self.remember(token, &record);
                out.insert(name, record);
            }
            // Keys the call's byte budget cut are in NEITHER list
            // ([`KvAnswer::truncated`]), so a read that answered nothing at all
            // is a window too wide rather than an empty result.
            if answered.is_empty() {
                if window == 1 {
                    return Err(queen::Error::Body(
                        "kv answered no row for a single queue key".to_string(),
                    ));
                }
                window /= 2;
                continue;
            }
            pending.retain(|name| !answered.contains(&Registry::key_queue(name)));
        }
        Ok(out)
    }

    /// Create the record, refusing to overwrite one. Answers the WINNER's record
    /// when it loses, which is what `QueueAlreadyExists` needs in order to
    /// compare attributes without a second round trip — AWS only errors on an
    /// attribute MISMATCH.
    pub async fn create_queue(&self, record: &QueueRecord, token: Option<&str>) -> Result<Cas> {
        let key = Registry::key_queue(&record.name);
        let ops = [KvOp::put_if_absent(NS, &key, record.to_value())];
        let answer = one(self.api.kv(&ops, token).await?)?;
        Ok(self.settle(record, answer, token))
    }

    /// Overwrite under a CAS on `record.version`. A lost precondition is a
    /// concurrent SetQueueAttributes, and the answer carries the winner.
    ///
    /// MIND THE SENSE OF `version: 0`: `expect: 0` is "must NOT exist", so it
    /// CREATES the key when it is absent (`putIfAbsent` desugars to it,
    /// 024_kv.sql) — it is not a refusal to create. Every caller here reads the
    /// version off a key it just found, so a zero cannot arise; the sentence is
    /// what stops a future one from reading a 0 as "leave it alone".
    pub async fn put_queue(&self, record: &QueueRecord, token: Option<&str>) -> Result<Cas> {
        let key = Registry::key_queue(&record.name);
        let ops = [KvOp::put_expecting(
            NS,
            &key,
            record.to_value(),
            record.version,
        )];
        let answer = one(self.api.kv(&ops, token).await?)?;
        Ok(self.settle(record, answer, token))
    }

    /// Remove the record and, if it was there, lay the cooldown tombstone.
    /// Answers whether it existed.
    ///
    /// TWO calls, in that order, and not one: they would have to be one batch to
    /// be atomic, and a batch writes the tombstone even when the delete found
    /// nothing — which would refuse the creation of a queue that never existed.
    /// A crash between them loses the cooldown, which is an emulated AWS artifact
    /// and not data.
    pub async fn delete_queue(&self, name: &str, token: Option<&str>) -> Result<bool> {
        let key = Registry::key_queue(name);
        let answer = one(self.api.kv(&[KvOp::delete(NS, &key, None)], token).await?)?;
        self.forget(name);
        if !answer.applied() {
            return Ok(false);
        }
        let marker = Registry::key_deleted_recently(name);
        let ops = [
            KvOp::put_ttl(
                NS,
                &marker,
                serde_json::json!({"ts": crate::obs::now_epoch_ms()}),
                DELETE_COOLDOWN.as_secs(),
                None,
            ),
            // A queue that is GONE is not being purged. The purge window
            // outliving its own queue would answer `PurgeQueueInProgress` for a
            // queue created a minute later under the same name, and dropping it
            // costs nothing: this call is one the delete path already makes.
            KvOp::delete(NS, &Registry::key_purging(name), None),
        ];
        self.api.kv(&ops, token).await?;
        Ok(true)
    }

    /// Claim the purge window for one queue, atomically. `true` is "this call
    /// owns the window"; `false` is a purge inside the last [`PURGE_COOLDOWN`],
    /// which is `PurgeQueueInProgress`.
    ///
    /// `putIfAbsent` rather than read-then-write, and that is the difference
    /// between emulating AWS's window and having one: two instances behind one
    /// load balancer both read "no purge in progress", both delete the queue,
    /// and the second delete lands between the first's delete and its own
    /// recreate — which is a queue whose broker half is gone and whose registry
    /// record says it is there.
    pub async fn begin_purge(&self, name: &str, token: Option<&str>) -> Result<bool> {
        let key = Registry::key_purging(name);
        let ops = [KvOp::put_if_absent_ttl(
            NS,
            &key,
            serde_json::json!({"ts": crate::obs::now_epoch_ms()}),
            PURGE_COOLDOWN.as_secs(),
        )];
        Ok(one(self.api.kv(&ops, token).await?)?.applied())
    }

    /// Whether a queue is inside its purge window. The TTL IS the window — an
    /// expired row is never returned by the store — so nothing here computes an
    /// age.
    pub async fn purging(&self, name: &str, token: Option<&str>) -> Result<bool> {
        let key = Registry::key_purging(name);
        let answer = one(self.api.kv(&[KvOp::get(NS, &key)], token).await?)?;
        Ok(answer.found)
    }

    /// Every queue under `prefix`, up to `limit`, paging the store as needed. The
    /// `truncated` flag is never ignored: a ListQueues that dropped a page would
    /// under-report the account's queues.
    pub async fn list_queues(
        &self,
        prefix: &str,
        limit: i64,
        token: Option<&str>,
    ) -> Result<Vec<QueueRecord>> {
        let limit = limit.clamp(1, MAX_LISTED) as usize;
        Ok(self.walk(prefix, limit, None, token).await?.0)
    }

    /// Whether the name is inside its post-delete cooldown, which is
    /// `QueueDeletedRecently`. The TTL is the window: an expired row is never
    /// returned by the store, so nothing here computes an age.
    pub async fn deleted_recently(&self, name: &str, token: Option<&str>) -> Result<bool> {
        let key = Registry::key_deleted_recently(name);
        let answer = one(self.api.kv(&[KvOp::get(NS, &key)], token).await?)?;
        Ok(answer.found)
    }

    // ------------------------------------------------------------- internals

    /// Read, apply, write under a CAS; on a lost CAS re-apply onto the WINNER and
    /// write once more. Two attempts and no more: a third would be an unbounded
    /// loop under exactly the load that produced the contention, and the honest
    /// answer to "two other instances beat me to this row" is the retriable code
    /// an SDK already backs off on.
    async fn mutate<F>(
        &self,
        name: &str,
        token: Option<&str>,
        change: F,
    ) -> RegistryResult<QueueRecord>
    where
        F: Fn(&mut QueueRecord) -> RegistryResult<()>,
    {
        let mut current = self
            .queue_fresh(name, token)
            .await?
            .ok_or_else(|| missing(name))?;
        for _ in 0..2 {
            let mut next = current.clone();
            change(&mut next)?;
            // Every write through this function is a modification, which is the
            // only definition `LastModifiedTimestamp` can have here.
            next.modified_ms = crate::obs::now_epoch_ms();
            guard_size(&next)?;
            match self.put_queue(&next, token).await? {
                Ok(stored) => return Ok(stored),
                // Version 0 is not a competitor, it is an ABSENCE: the queue was
                // deleted between the read and the write, and re-applying would
                // resurrect it.
                Err(winner) if winner.version == 0 => return Err(missing(name)),
                Err(winner) => current = *winner,
            }
        }
        Err(refuse(
            ErrorKind::ServiceUnavailable,
            format!("Concurrent updates to queue {name}; please retry"),
        ))
    }

    /// One page walk over `qs:q:<prefix>`, up to `limit` records. Answers the
    /// records and whether the store had more to give.
    ///
    /// A row whose key is not one of ours is NAMED and SKIPPED, never invented
    /// into a queue name a client would then address — so a page may yield fewer
    /// records than rows, which costs a slot of the limit and nothing else.
    async fn walk(
        &self,
        prefix: &str,
        limit: usize,
        after: Option<String>,
        token: Option<&str>,
    ) -> Result<(Vec<QueueRecord>, bool)> {
        let key_prefix = format!("{QUEUE_PREFIX}{}", escape(prefix));
        let (rows, more) = self
            .walk_rows(&key_prefix, limit, after, token, "queues")
            .await?;
        let mut out: Vec<QueueRecord> = Vec::with_capacity(rows.len());
        for row in &rows {
            match Registry::queue_of_key(&row.key) {
                Some(name) => out.push(QueueRecord::from_value(&name, &row.value, row.version)),
                None => tracing::warn!(target: "sqs", key = %row.key, "unreadable queue key"),
            }
        }
        Ok((out, more))
    }

    /// The raw page walk every listing in this facade is built on: rows under
    /// `key_prefix`, up to `limit` of them, paging the store until it has that
    /// many or has run out.
    ///
    /// `pub(crate)` because [`crate::sns::registry`] walks `qs:t:` and `qs:s:`
    /// with it. ONE loop for all three, because the loop's termination depends on
    /// the broker's cursor advancing — a second copy is a second place that can
    /// spin — and the `truncated` flag is never ignored anywhere: a listing that
    /// dropped a page under-reports the tenant's own resources.
    ///
    /// `what` names the key space in the one failure message this can produce.
    pub(crate) async fn walk_rows(
        &self,
        key_prefix: &str,
        limit: usize,
        after: Option<String>,
        token: Option<&str>,
        what: &str,
    ) -> Result<(Vec<KvRow>, bool)> {
        let mut after = after;
        let mut out: Vec<KvRow> = Vec::new();
        for _ in 0..MAX_PAGES {
            let want = (limit - out.len()).min(queen::MAX_KV_PREFIX_LIMIT as usize) as i64;
            let ops = [KvOp::get_prefix(NS, key_prefix, want, after.as_deref())];
            let answer = one(self.api.kv(&ops, token).await?)?;
            out.extend(answer.rows.iter().cloned());
            if !answer.truncated {
                return Ok((out, false));
            }
            if out.len() >= limit {
                return Ok((out, true));
            }
            // The page was cut by the store's own byte budget rather than by the
            // limit asked for: continue from its cursor. Without one the walk
            // cannot advance, and asking again would return the same page for
            // ever.
            let Some(next) = answer.next_after.clone() else {
                return Err(queen::Error::Body(
                    "kv truncated a prefix page without a cursor to continue from".to_string(),
                ));
            };
            after = Some(next);
        }
        Err(queen::Error::Body(format!(
            "the {what} under {key_prefix:?} did not fit in {MAX_PAGES} pages"
        )))
    }

    /// Turn one write's answer into a [`Cas`], caching whichever record now
    /// stands — the one written, or the winner the store handed back.
    fn settle(&self, wrote: &QueueRecord, answer: KvAnswer, token: Option<&str>) -> Cas {
        if answer.applied() {
            let stored = QueueRecord {
                version: answer.version,
                ..wrote.clone()
            };
            self.remember(token, &stored);
            return Ok(stored);
        }
        let winner = QueueRecord::from_value(&wrote.name, &answer.value, answer.version);
        if winner.version != 0 {
            self.remember(token, &winner);
        }
        Err(Box::new(winner))
    }

    fn cached(&self, token: Option<&str>, name: &str) -> Option<QueueRecord> {
        let cache = self.cache.lock().unwrap();
        cache
            .get(&(CredentialKey::of(token), name.to_string()))
            .filter(|entry| entry.at.elapsed() < self.ttl)
            .map(|entry| entry.record.clone())
    }

    fn remember(&self, token: Option<&str>, record: &QueueRecord) {
        if self.ttl.is_zero() {
            return;
        }
        let key = (CredentialKey::of(token), record.name.clone());
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= MAX_CACHED && !cache.contains_key(&key) {
            if let Some(coldest) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.at)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&coldest);
            }
        }
        cache.insert(
            key,
            Cached {
                record: record.clone(),
                at: Instant::now(),
            },
        );
    }

    /// Forget one queue under EVERY credential. A deletion is the one event that
    /// must not be served stale — the next call would answer a deleted queue's
    /// attributes — and the cost of being wrong about which credential owns the
    /// name is one extra read.
    fn forget(&self, name: &str) {
        self.cache.lock().unwrap().retain(|(_, key), _| key != name);
    }
}

// THE SNS HALF OF THIS STORE IS IN [`crate::sns::registry`] — the records, the
// reads and the compare-and-set writes for `qs:t:` and `qs:s:`, as a second
// `impl Registry` block in that module. Its keys are minted HERE, beside the
// queue's, because the key space is one space and the property that the five
// prefixes cannot see each other is a property of the whole of it
// ([`tests::the_queue_key_spaces_cannot_see_each_other`]); everything else about
// a topic is SNS's own vocabulary and lives with the SNS actions.

// --------------------------------------------------------------- validation

/// Which call is asking. The two differ in exactly one thing: whether the
/// create-only attributes are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum When {
    Create,
    Set,
}

/// Whether a string is a queue name AWS would have accepted, `.fifo` included.
/// Charset only — the agreement with `FifoQueue` is [`validate_name`]'s.
fn name_is_wellformed(name: &str) -> bool {
    let stem = name.strip_suffix(FIFO_SUFFIX).unwrap_or(name);
    (1..=MAX_NAME_LEN).contains(&name.len())
        && !stem.is_empty()
        && stem
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The name rules, and the one rule that ties a name to an attribute.
///
/// The messages are AWS's own, verbatim: a client's log is where an operator
/// reads them, and a paraphrase is one more string to reconcile when someone
/// compares this facade against the real service.
fn validate_name(name: &str, attributes: &BTreeMap<String, String>) -> RegistryResult<()> {
    let fifo_attribute = attributes
        .get(ATTR_FIFO)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let fifo_name = is_fifo(name);
    if !name_is_wellformed(name) || (fifo_attribute && !fifo_name) {
        let sentence = match fifo_attribute || fifo_name {
            true => {
                "The name of a FIFO queue can only include alphanumeric characters, hyphens, or \
                 underscores, must end with .fifo suffix and be 1 to 80 in length"
            }
            false => {
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in \
                 length"
            }
        };
        return Err(refuse(ErrorKind::InvalidParameterValue, sentence));
    }
    if fifo_name && !fifo_attribute {
        return Err(refuse(
            ErrorKind::InvalidParameterValue,
            "The name of a queue ending in .fifo requires the FifoQueue attribute set to true",
        ));
    }
    Ok(())
}

/// The attribute catalog and its ranges.
///
/// An unknown name and an immutable one are both `InvalidAttributeName`, which is
/// what AWS answers for an attribute that cannot be set; a value outside its
/// range is `InvalidParameterValue`, because the closed catalog has no
/// `InvalidAttributeValue` and inventing one would be inventing a client
/// behaviour (crate::error's rule).
fn validate_attributes(attributes: &BTreeMap<String, String>, when: When) -> RegistryResult<()> {
    for (name, value) in attributes {
        let settable = MUTABLE.contains(&name.as_str())
            || (when == When::Create && CREATE_ONLY.contains(&name.as_str()));
        if !settable {
            let why = match CREATE_ONLY.contains(&name.as_str()) {
                true => format!("Attribute {name} is set at queue creation and cannot be changed"),
                false => format!("Unknown Attribute {name}"),
            };
            return Err(refuse(ErrorKind::InvalidAttributeName, why));
        }
        if let Some((_, low, high)) = RANGES.iter().find(|(a, _, _)| *a == name) {
            let parsed = value.trim().parse::<i64>().ok();
            if !parsed.is_some_and(|v| (*low..=*high).contains(&v)) {
                return Err(refuse(
                    ErrorKind::InvalidParameterValue,
                    format!("Invalid value for the parameter {name}: must be {low} to {high}"),
                ));
            }
        }
        if BOOLEANS.contains(&name.as_str())
            && !(value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false"))
        {
            return Err(refuse(
                ErrorKind::InvalidParameterValue,
                format!("Invalid value for the parameter {name}: must be true or false"),
            ));
        }
        // The one policy this facade ACTS on, so the one it validates. `Policy`
        // and `RedriveAllowPolicy` are stored verbatim and never enforced
        // (PLAN_QUEEN_SQS.md's first non-goal), and validating what we do not
        // enforce would refuse documents AWS accepts.
        if name == "RedrivePolicy"
            && serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .filter(|v| v.is_object())
                .is_none()
        {
            return Err(refuse(
                ErrorKind::InvalidParameterValue,
                "Invalid value for the parameter RedrivePolicy: must be a JSON object",
            ));
        }
    }
    Ok(())
}

fn validate_tags(tags: &BTreeMap<String, String>) -> RegistryResult<()> {
    if tags.len() > MAX_TAGS {
        return Err(refuse(
            ErrorKind::InvalidParameterValue,
            format!("A queue can have at most {MAX_TAGS} tags"),
        ));
    }
    Ok(())
}

/// Refuse a record the store would refuse anyway. `Policy` is an unbounded
/// client-supplied document, so this is reachable from a request rather than only
/// from a defect — and a 400 from the broker would surface as a server fault
/// instead of the client error it is.
fn guard_size(record: &QueueRecord) -> RegistryResult<()> {
    let bytes = serde_json::to_string(&record.to_value()).map_or(usize::MAX, |s| s.len());
    if bytes > queen::MAX_KV_VALUE_BYTES {
        return Err(refuse(
            ErrorKind::InvalidParameterValue,
            format!(
                "The queue's attributes and tags are {bytes} bytes, over the {} the registry \
                 stores",
                queen::MAX_KV_VALUE_BYTES
            ),
        ));
    }
    Ok(())
}

/// A `CreateQueue` for a name that is taken: the existing queue, or
/// `QueueAlreadyExists`.
///
/// AWS's rule, one-directional, in one place because two call sites reach it (a
/// lost `putIfAbsent` and the `.fifo` re-create above): the request wins the
/// existing queue unless one of the attributes it SUPPLIES differs from that
/// queue's current value.
fn existing_or_conflict(
    existing: QueueRecord,
    supplied: &BTreeMap<String, String>,
) -> RegistryResult<QueueRecord> {
    match first_conflict(&existing, supplied) {
        None => Ok(existing),
        // AWS's sentence, verbatim: it names the attribute, which is the whole
        // of what an operator needs to reconcile the two definitions.
        Some(attribute) => Err(refuse(
            ErrorKind::QueueAlreadyExists,
            format!(
                "A queue already exists with the same name and a different value for attribute \
                 {attribute}"
            ),
        )),
    }
}

/// The first SUPPLIED attribute whose value is not the queue's current one, in
/// name order, or `None` when the request describes the queue that is there.
///
/// The direction is the contract (see [`Registry::create`]): keys of `existing`
/// that `supplied` does not name are not consulted at all. The current value is
/// [`crate::actions::queues::effective_attributes`]'s — the READ catalog, which
/// is the other module's by the split both module headers describe — so that the
/// answer `GetQueueAttributes` gives for an attribute is exactly the value
/// `CreateQueue` accepts back for it. Anything else would refuse a client that
/// read a queue and re-created it from what it read.
fn first_conflict(existing: &QueueRecord, supplied: &BTreeMap<String, String>) -> Option<String> {
    let current = crate::actions::queues::effective_attributes(existing);
    supplied
        .iter()
        .find(|(name, value)| {
            !current
                .get(*name)
                .is_some_and(|current| same_attribute(name, value, current))
        })
        .map(|(name, _)| name.clone())
}

/// Whether a supplied attribute value DESCRIBES the current one.
///
/// Normalized over the two shapes the validator itself normalizes, and no
/// others: [`validate_attributes`] accepts a boolean in any casing and parses a
/// bounded integer with `trim`, while the RAW string is what gets stored
/// (`GetQueueAttributes` answers what was set). Comparing bytes would therefore
/// make a first create that spelled `FifoQueue="TRUE"` refuse — permanently,
/// with `QueueAlreadyExists` — every later create that spells it `"true"`, which
/// is the same D1 failure the effective-attributes half of this comparison
/// exists to prevent, reached from the other side.
///
/// Everything else is compared exactly: a `Policy` or a `RedrivePolicy` is a
/// document this facade does not normalize, and declaring two spellings of one
/// equal is a claim it cannot make.
fn same_attribute(name: &str, supplied: &str, current: &str) -> bool {
    if supplied == current {
        return true;
    }
    if BOOLEANS.contains(&name) {
        return supplied.eq_ignore_ascii_case(current);
    }
    if RANGES.iter().any(|(attribute, _, _)| *attribute == name) {
        return match (
            supplied.trim().parse::<i64>(),
            current.trim().parse::<i64>(),
        ) {
            (Ok(supplied), Ok(current)) => supplied == current,
            _ => false,
        };
    }
    false
}

// ------------------------------------------------------------ keys & tokens

/// One key component, escaped so a name containing the separator cannot address
/// another record. queen-kafka's rule (`offsets::escape`), byte for byte, and the
/// reason is the same one: `qs:s:a:b` and `qs:s:a:b` must mean one subscription,
/// whichever half the `:` came from.
///
/// A legal queue name is already made entirely of this set, so an ordinary name
/// is stored spelled exactly as it is and a key is readable in a database. What
/// the escaping is for is everything that reaches these functions BEFORE
/// validation — a `GetQueueUrl` for `../../etc`, an SNS subscription id, a
/// partition id — and the bytes Postgres `TEXT` cannot hold at all.
pub fn escape(component: &str) -> String {
    let mut out = String::with_capacity(component.len());
    for b in component.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The inverse. An escape that is not one is left as it stands rather than
/// dropped — this reads keys back, and a key that does not round-trip must still
/// be recognisable in a log.
fn unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(b) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `NextToken`: the last name on a page, base64. URL-safe and unpadded because it
/// travels in a query string on the Query protocol, where `+` is a space and `=`
/// is a separator.
pub fn encode_token(name: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(name)
}

/// The inverse. `None` for anything this facade did not mint — which the caller
/// answers `InvalidParameterValue` for rather than silently listing from the
/// start, because a client that paged past a bad token would loop over page one
/// for ever.
pub fn decode_token(cursor: &str) -> Option<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()?;
    let name = String::from_utf8(bytes).ok()?;
    name_is_wellformed(&name).then_some(name)
}

/// The one answer a single-operation KV call is expected to produce.
pub(crate) fn one(answers: Vec<KvAnswer>) -> Result<KvAnswer> {
    answers
        .into_iter()
        .next()
        .ok_or_else(|| queen::Error::Body("kv answered nothing for a single operation".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;

    /// The cache OFF, which is what a test that counts calls to Queen wants: a
    /// hit would make the count depend on which test ran before it.
    fn registry(api: &Arc<FakeQueen>) -> Registry {
        Registry::with_ttl(api.clone(), Duration::ZERO)
    }

    fn naming() -> Naming {
        Naming::new("queen-1", "000000000000")
    }

    fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn calls(api: &Arc<FakeQueen>) -> usize {
        api.kv_calls.lock().unwrap().len()
    }

    async fn create(reg: &Registry, name: &str, a: &[(&str, &str)]) -> RegistryResult<QueueRecord> {
        reg.create(name, &attrs(a), &BTreeMap::new(), &naming(), 64, None)
            .await
    }

    // -------------------------------------------------------------- the keys

    #[test]
    fn a_key_names_its_record() {
        assert_eq!(Registry::key_queue("orders"), "qs:q:orders");
        assert_eq!(Registry::key_queue("orders.fifo"), "qs:q:orders.fifo");
        assert_eq!(Registry::key_deleted_recently("orders"), "qs:qdel:orders");
        assert_eq!(Registry::key_purging("orders"), "qs:purge:orders");
        assert_eq!(Registry::key_topic("events"), "qs:t:events");
        assert_eq!(
            Registry::key_subscription("events", "s-1"),
            "qs:s:events:s-1"
        );
        assert_eq!(
            Registry::key_delete_set("pid-1", "lease-2"),
            "qs:ds:pid-1:lease-2"
        );
    }

    /// THE property the two queue key spaces rest on: `qs:qdel:` is not under
    /// `qs:q:`, so the prefix read behind ListQueues cannot see a tombstone and a
    /// queue named `del:orders` cannot forge one.
    #[test]
    fn the_queue_key_spaces_cannot_see_each_other() {
        let tombstone = Registry::key_deleted_recently("orders");
        assert!(!tombstone.starts_with(QUEUE_PREFIX), "{tombstone}");
        assert!(!Registry::key_queue("del:orders").starts_with(DELETED_PREFIX));
        // The purge window is a third space, and the same property holds for it:
        // a listing cannot see it and a name cannot forge one.
        let purge = Registry::key_purging("orders");
        assert!(!purge.starts_with(QUEUE_PREFIX), "{purge}");
        assert!(!purge.starts_with(DELETED_PREFIX), "{purge}");
        assert!(!Registry::key_queue("urge:orders").starts_with(PURGE_PREFIX));
        // ...and no name can compose one from the other: the escaping puts the
        // colon beyond a client's reach.
        assert_eq!(Registry::key_queue("del:orders"), "qs:q:del%3Aorders");
    }

    #[test]
    fn a_name_that_could_address_another_record_is_escaped() {
        for name in [
            "with space",
            "a:b",
            "new\nline",
            "nul\0byte",
            "100%",
            "../q",
        ] {
            let key = Registry::key_queue(name);
            assert!(key.bytes().all(|b| b.is_ascii_graphic()), "{name} -> {key}");
            assert_eq!(Registry::queue_of_key(&key).as_deref(), Some(name));
        }
        // A subscription's two halves cannot be confused for one another.
        assert_ne!(
            Registry::key_subscription("a", "b:c"),
            Registry::key_subscription("a:b", "c")
        );
    }

    // ---------------------------------------------------------- name matrix

    #[test]
    fn the_name_rules_are_awss() {
        let fifo = attrs(&[("FifoQueue", "true")]);
        let none = BTreeMap::new();
        for (name, attributes, ok) in [
            ("orders", &none, true),
            ("Orders_2-x", &none, true),
            ("o", &none, true),
            (&"q".repeat(80), &none, true),
            (&"q".repeat(81), &none, false),
            ("", &none, false),
            ("with space", &none, false),
            ("with.dot", &none, false),
            ("with/slash", &none, false),
            ("..", &none, false),
            ("orders.fifo", &fifo, true),
            // The suffix and the attribute must AGREE, in both directions.
            ("orders.fifo", &none, false),
            ("orders", &fifo, false),
            (".fifo", &fifo, false),
            // 80 characters counts the suffix.
            (&format!("{}.fifo", "q".repeat(75)), &fifo, true),
            (&format!("{}.fifo", "q".repeat(76)), &fifo, false),
        ] {
            let got = validate_name(name, attributes);
            assert_eq!(got.is_ok(), ok, "{name:?} with {attributes:?}: {got:?}");
        }
    }

    #[tokio::test]
    async fn a_name_that_breaks_the_rules_never_reaches_queen() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let e = create(&reg, "not a name", &[]).await.unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameterValue));
        assert_eq!(calls(&api), 0, "a refused name cost a round trip");
    }

    // -------------------------------------------------------------- create

    #[tokio::test]
    async fn create_stores_the_documented_row() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let record = reg
            .create(
                "orders",
                &attrs(&[("VisibilityTimeout", "45")]),
                &attrs(&[("team", "billing")]),
                &naming(),
                64,
                None,
            )
            .await
            .unwrap();

        assert_eq!(record.partitions, 64);
        assert!(!record.fifo);
        assert_eq!(record.arn, "arn:aws:sqs:queen-1:000000000000:orders");
        assert!(record.version > 0, "the record was not versioned");

        let stored = api.kv_get(NS, "qs:q:orders").unwrap();
        assert_eq!(
            stored.get("attributes").unwrap(),
            &serde_json::json!({"VisibilityTimeout": "45", "queen.partitions": "64"}),
            "the width is stamped so the record spells its own"
        );
        assert_eq!(
            stored.get("tags").unwrap(),
            &serde_json::json!({"team": "billing"})
        );
        assert_eq!(
            stored.get("arn").unwrap(),
            &serde_json::json!("arn:aws:sqs:queen-1:000000000000:orders")
        );
        assert!(stored.get("createdTs").unwrap().as_i64().unwrap() > 0);
        // The name is the KEY, and `fifo`/`partitions` are derived: a stored copy
        // of either would be a second source of truth for one fact.
        for absent in ["name", "fifo", "partitions"] {
            assert!(stored.get(absent).is_none(), "{absent} is stored");
        }
    }

    /// A FIFO queue synthesizes no width at all — the lane IS the
    /// `MessageGroupId` — so nothing stamps one.
    #[tokio::test]
    async fn a_fifo_queue_is_stamped_with_no_width() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let record = create(&reg, "orders.fifo", &[("FifoQueue", "true")])
            .await
            .unwrap();
        assert!(record.fifo);
        assert_eq!(record.partitions, 0);
        assert!(!record.attributes.contains_key(ATTR_PARTITIONS));
    }

    /// AWS's own rule: the same name with the same attributes is a SUCCESS, and
    /// answers the existing queue.
    #[tokio::test]
    async fn create_is_idempotent_on_identical_attributes() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let first = create(&reg, "orders", &[("VisibilityTimeout", "45")])
            .await
            .unwrap();
        let again = create(&reg, "orders", &[("VisibilityTimeout", "45")])
            .await
            .unwrap();
        assert_eq!(first.version, again.version, "the record was rewritten");
        assert_eq!(first.created_ms, again.created_ms);

        // ...including the case where neither call named a width: the stamp is
        // the same default both times.
        create(&reg, "clicks", &[]).await.unwrap();
        assert!(create(&reg, "clicks", &[]).await.is_ok());
    }

    #[tokio::test]
    async fn create_answers_queue_already_exists_on_a_mismatch() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[("VisibilityTimeout", "45")])
            .await
            .unwrap();

        let e = create(&reg, "orders", &[("VisibilityTimeout", "60")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueAlreadyExists));
        assert!(
            e.to_string().contains("VisibilityTimeout"),
            "the message does not name the attribute: {e}"
        );

        // ...and the mismatch is found wherever it sits in a request that is
        // otherwise right, which is the shape a client sends.
        let e = create(
            &reg,
            "orders",
            &[("VisibilityTimeout", "45"), ("DelaySeconds", "10")],
        )
        .await
        .unwrap_err();
        assert!(e.to_string().contains("DelaySeconds"), "{e}");
    }

    /// D1 (`compat/M0_SMOKE.md`), and the single highest-traffic call shape in
    /// the M0 surface: the idempotent create every framework performs at worker
    /// start-up, against a queue an operator or Terraform made with attributes it
    /// does not repeat. AWS returns the URL — `QueueNameExists` is returned *only
    /// if the request includes attributes whose values differ*, and a request
    /// that includes no attributes includes none that differ.
    #[tokio::test]
    async fn a_create_that_names_no_attribute_answers_the_existing_queue() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let first = create(
            &reg,
            "orders",
            &[
                ("VisibilityTimeout", "45"),
                ("MessageRetentionPeriod", "3600"),
                ("DelaySeconds", "5"),
                (ATTR_PARTITIONS, "8"),
            ],
        )
        .await
        .unwrap();

        let bare = create(&reg, "orders", &[]).await.unwrap();
        assert_eq!(bare.version, first.version, "the record was rewritten");
        assert_eq!(bare.attributes, first.attributes);
        assert_eq!(bare.partitions, 8, "and it is the EXISTING queue's width");

        // A SUBSET is the same rule: what it names matches, what it omits is not
        // consulted — including the width, which the first create named.
        let subset = create(&reg, "orders", &[("VisibilityTimeout", "45")])
            .await
            .unwrap();
        assert_eq!(subset.version, first.version);
    }

    /// The other half of the one-directional rule, and the other half of the same
    /// defect: the comparison is against what `GetQueueAttributes` ANSWERS, not
    /// against what the record stores. Every queue on AWS has a
    /// `VisibilityTimeout`, so supplying its default value describes the queue
    /// that is there.
    #[tokio::test]
    async fn a_supplied_default_matches_a_queue_that_stored_nothing() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let first = create(&reg, "orders", &[]).await.unwrap();
        assert!(!first.attributes.contains_key("VisibilityTimeout"));

        let again = create(
            &reg,
            "orders",
            &[
                ("VisibilityTimeout", "30"),
                ("MessageRetentionPeriod", "345600"),
                ("MaximumMessageSize", "262144"),
                ("DelaySeconds", "0"),
                ("ReceiveMessageWaitTimeSeconds", "0"),
            ],
        )
        .await
        .unwrap();
        assert_eq!(again.version, first.version);

        // ...and a value that is not the default still conflicts.
        let e = create(&reg, "orders", &[("VisibilityTimeout", "31")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueAlreadyExists));
    }

    /// The comparison is NORMALIZED over the two shapes the validator itself
    /// normalizes: the raw string is what gets stored, so a first create that
    /// spelled `FifoQueue="TRUE"` must not refuse every later create that spells
    /// it `"true"` — permanently, with `QueueAlreadyExists`.
    #[tokio::test]
    async fn a_re_create_that_spells_a_value_differently_is_the_same_queue() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders.fifo", &[("FifoQueue", "TRUE")])
            .await
            .unwrap();
        create(&reg, "orders.fifo", &[("FifoQueue", "true")])
            .await
            .expect("one declaration, two spellings");
        create(&reg, "orders.fifo", &[("FifoQueue", "True")])
            .await
            .expect("one declaration, three spellings");

        // ...and the same for a bounded integer, which the validator parses with
        // `trim` and the record stores verbatim.
        create(&reg, "orders", &[("VisibilityTimeout", " 30")])
            .await
            .unwrap();
        create(&reg, "orders", &[("VisibilityTimeout", "30")])
            .await
            .expect("one number, two spellings");
        // A DIFFERENT value still conflicts, and so does a document nobody can
        // normalize.
        assert_eq!(
            create(&reg, "orders", &[("VisibilityTimeout", "31")])
                .await
                .unwrap_err()
                .kind(),
            Some(ErrorKind::QueueAlreadyExists)
        );
        create(&reg, "audit", &[("Policy", r#"{"a":1}"#)])
            .await
            .unwrap();
        assert_eq!(
            create(&reg, "audit", &[("Policy", r#"{ "a": 1 }"#)])
                .await
                .unwrap_err()
                .kind(),
            Some(ErrorKind::QueueAlreadyExists),
            "a document has no normal form this facade can claim to know"
        );
    }

    /// The width is a default this facade IMPLIES and not a parameter a client
    /// supplied, so it is never compared — but a client that supplies one is
    /// asking for a queue that is not this one.
    #[tokio::test]
    async fn the_stamped_width_is_not_a_supplied_attribute() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[(ATTR_PARTITIONS, "8")])
            .await
            .unwrap();

        assert!(create(&reg, "orders", &[]).await.is_ok());
        let e = create(&reg, "orders", &[(ATTR_PARTITIONS, "64")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueAlreadyExists));
        assert!(e.to_string().contains(ATTR_PARTITIONS), "{e}");
    }

    /// Tags are not attributes: SQS gives them their own request member and their
    /// own three actions, and the error's own sentence names attributes only. So a
    /// re-create with different tags SUCCEEDS and changes nothing — the tags on the
    /// queue stay the ones `TagQueue` and the first create put there.
    #[tokio::test]
    async fn tags_on_a_re_create_are_neither_compared_nor_applied() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create(
            "orders",
            &BTreeMap::new(),
            &attrs(&[("team", "billing")]),
            &naming(),
            64,
            None,
        )
        .await
        .unwrap();

        let again = reg
            .create(
                "orders",
                &BTreeMap::new(),
                &attrs(&[("team", "platform"), ("env", "prod")]),
                &naming(),
                64,
                None,
            )
            .await
            .unwrap();
        assert_eq!(again.tags, attrs(&[("team", "billing")]));
        assert_eq!(
            reg.tags("orders", None).await.unwrap(),
            attrs(&[("team", "billing")]),
            "a create retagged a queue it did not create"
        );
    }

    /// The `.fifo` edge. The suffix DECLARES the type, so an existing FIFO queue
    /// re-created without the attribute is a request that supplies nothing that
    /// differs; the same call for a queue that is NOT there is still the bad
    /// create it always was.
    #[tokio::test]
    async fn a_fifo_queue_is_re_created_without_repeating_the_attribute() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let e = create(&reg, "orders.fifo", &[]).await.unwrap_err();
        assert_eq!(
            e.kind(),
            Some(ErrorKind::InvalidParameterValue),
            "a queue that does not exist is created by the request alone"
        );

        let first = create(&reg, "orders.fifo", &[("FifoQueue", "true")])
            .await
            .unwrap();
        let again = create(&reg, "orders.fifo", &[]).await.unwrap();
        assert_eq!(again.version, first.version);
        assert!(again.fifo);

        // The attribute is still COMPARED when it is supplied, and `false`
        // against a `.fifo` queue is a different queue.
        let e = create(&reg, "orders.fifo", &[("FifoQueue", "false")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueAlreadyExists));

        // ...and the two names are two queues: a standard `orders` cannot collide
        // with `orders.fifo`, in either direction.
        create(&reg, "orders", &[]).await.unwrap();
        assert!(create(&reg, "orders", &[]).await.is_ok());
        let e = create(&reg, "orders", &[("FifoQueue", "true")])
            .await
            .unwrap_err();
        assert_eq!(
            e.kind(),
            Some(ErrorKind::InvalidParameterValue),
            "a FIFO queue named without the suffix is a name error, not a collision"
        );
    }

    /// The relaxation above is for a queue that EXISTS, so a bad attribute in the
    /// same request is still the client's error and still costs no create.
    #[tokio::test]
    async fn a_fifo_re_create_still_validates_its_attributes() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders.fifo", &[("FifoQueue", "true")])
            .await
            .unwrap();
        let e = create(&reg, "orders.fifo", &[("Nonsense", "1")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::InvalidAttributeName));
        let e = create(&reg, "orders.fifo", &[("VisibilityTimeout", "99999")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameterValue));
    }

    /// Two instances behind one load balancer, the normal case: the loser sees
    /// the WINNER's record in the same answer and decides from it, with no second
    /// read.
    #[tokio::test]
    async fn two_creators_race_and_exactly_one_record_survives() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let other = QueueRecord {
            name: "orders".to_string(),
            attributes: attrs(&[("VisibilityTimeout", "45"), ("queen.partitions", "64")]),
            created_ms: 1,
            arn: naming().arn("orders"),
            ..QueueRecord::default()
        };
        // Lands between the tombstone read and the putIfAbsent.
        api.kv_interpose
            .lock()
            .unwrap()
            .extend([None, Some(KvOp::put(NS, "qs:q:orders", other.to_value()))]);

        // Identical attributes: the loser answers the winner, not an error.
        let got = create(&reg, "orders", &[("VisibilityTimeout", "45")])
            .await
            .unwrap();
        assert_eq!(got.created_ms, 1, "the loser overwrote the winner");

        // ...and a losing creator with DIFFERENT attributes is the error.
        api.kv_interpose
            .lock()
            .unwrap()
            .extend([None, Some(KvOp::put(NS, "qs:q:clicks", other.to_value()))]);
        let e = create(&reg, "clicks", &[("VisibilityTimeout", "1")])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueAlreadyExists));
    }

    #[tokio::test]
    async fn an_oversized_record_is_refused_before_the_broker_sees_it() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let policy = "x".repeat(queen::MAX_KV_VALUE_BYTES + 1);
        let e = create(&reg, "orders", &[("Policy", &policy)])
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameterValue));
        assert_eq!(calls(&api), 0);
    }

    // -------------------------------------------------------- the batch read

    /// Many queues, ONE call — and a name that is not there is simply absent,
    /// never a hole the caller computes by difference.
    #[tokio::test]
    async fn many_queues_resolve_in_one_read() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        for name in ["orders", "audit", "billing"] {
            create(&reg, name, &[]).await.unwrap();
        }
        api.kv_calls.lock().unwrap().clear();

        let names: Vec<String> = ["orders", "audit", "billing", "gone", "orders"]
            .iter()
            .map(|n| (*n).to_string())
            .collect();
        let found = reg.queues_fresh(&names, None).await.unwrap();
        assert_eq!(calls(&api), 1, "one round trip for the whole fan-out");
        assert_eq!(found.len(), 3);
        assert_eq!(found["orders"].name, "orders");
        assert!(!found.contains_key("gone"));

        // ...and an empty ask costs nothing at all.
        assert!(reg.queues_fresh(&[], None).await.unwrap().is_empty());
        assert_eq!(calls(&api), 1);
    }

    /// A read the store's byte budget cut leaves keys in NEITHER `rows` nor
    /// `missing`, so the walk narrows its window and asks again rather than
    /// reporting the queues it never heard about as deleted — which the publish
    /// path would act on by skipping their subscribers.
    #[tokio::test]
    async fn a_truncated_batch_read_finishes_in_more_calls_and_loses_nothing() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let names: Vec<String> = (0..5).map(|i| format!("q{i}")).collect();
        for name in &names {
            create(&reg, name, &[]).await.unwrap();
        }
        api.kv_truncate_reads_at(1);
        api.kv_calls.lock().unwrap().clear();

        let found = reg.queues_fresh(&names, None).await.unwrap();
        assert_eq!(found.len(), 5, "every queue, however many calls it took");
        assert!(calls(&api) > 1);

        // A store that answers NOTHING narrows its window to one key and then
        // says so — the walk terminates rather than asking for ever.
        api.kv_truncate_reads_at(0);
        assert!(reg.queues_fresh(&names, None).await.is_err());
    }

    // ------------------------------------------------------ the 60s window

    #[tokio::test]
    async fn a_queue_deleted_a_moment_ago_cannot_be_recreated() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        reg.delete("orders", None).await.unwrap();

        assert!(reg.deleted_recently("orders", None).await.unwrap());
        let e = create(&reg, "orders", &[]).await.unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueDeletedRecently));

        // The window is the TTL, and nothing here computes an age.
        api.advance(DELETE_COOLDOWN + Duration::from_secs(1));
        assert!(!reg.deleted_recently("orders", None).await.unwrap());
        assert!(create(&reg, "orders", &[]).await.is_ok());
    }

    #[tokio::test]
    async fn deleting_a_queue_that_is_not_there_lays_no_tombstone() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let e = reg.delete("orders", None).await.unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueDoesNotExist));
        assert!(
            !reg.deleted_recently("orders", None).await.unwrap(),
            "a queue that never existed was put in cooldown"
        );
    }

    // ---------------------------------------------------- the purge window

    /// The window is CLAIMED, not read: the first caller owns it and every
    /// other one inside the minute is refused, which is what
    /// `PurgeQueueInProgress` is.
    #[tokio::test]
    async fn only_one_purge_owns_the_window_and_it_expires_with_its_ttl() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();

        assert!(!reg.purging("orders", None).await.unwrap());
        assert!(reg.begin_purge("orders", None).await.unwrap());
        assert!(reg.purging("orders", None).await.unwrap());
        assert!(
            !reg.begin_purge("orders", None).await.unwrap(),
            "a second purge inside the window won it"
        );
        // Another queue's window is another key.
        create(&reg, "clicks", &[]).await.unwrap();
        assert!(reg.begin_purge("clicks", None).await.unwrap());

        // The TTL is the window; nothing computes an age.
        api.advance(PURGE_COOLDOWN + Duration::from_secs(1));
        assert!(!reg.purging("orders", None).await.unwrap());
        assert!(reg.begin_purge("orders", None).await.unwrap());
    }

    /// A store that cannot be reached is never a purge window somebody owns.
    /// The failure has to reach the caller as a failure: reading it as "the
    /// window is free" would let every instance in a fleet purge the same queue
    /// at the same moment, which is the one thing the window exists to stop.
    #[tokio::test]
    async fn a_store_failure_is_never_a_free_purge_window() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        assert!(reg.begin_purge("orders", None).await.is_err());
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        assert!(reg.purging("orders", None).await.is_err());
    }

    /// The window may not outlive its queue: a name recreated after a delete
    /// would otherwise answer `PurgeQueueInProgress` for a purge nobody ran.
    #[tokio::test]
    async fn deleting_a_queue_closes_its_purge_window() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        assert!(reg.begin_purge("orders", None).await.unwrap());

        reg.delete("orders", None).await.unwrap();
        assert!(!reg.purging("orders", None).await.unwrap());
        assert!(api.kv_get(NS, "qs:purge:orders").is_none());
        // …and the delete's own tombstone is untouched by that.
        assert!(reg.deleted_recently("orders", None).await.unwrap());
    }

    #[tokio::test]
    async fn delete_removes_the_record_and_the_queue_stops_resolving() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        reg.delete("orders", None).await.unwrap();
        assert!(api.kv_get(NS, "qs:q:orders").is_none());
        assert_eq!(
            reg.require("orders", None).await.unwrap_err().kind(),
            Some(ErrorKind::QueueDoesNotExist)
        );
    }

    #[tokio::test]
    async fn require_of_a_missing_queue_is_queue_does_not_exist() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let e = reg.require("nope", None).await.unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueDoesNotExist));
        assert!(reg.queue("nope", None).await.unwrap().is_none());
    }

    // --------------------------------------------------------------- listing

    #[tokio::test]
    async fn list_filters_by_prefix_and_pages_with_an_opaque_token() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        for n in ["a-1", "a-2", "a-3", "b-1"] {
            create(&reg, n, &[]).await.unwrap();
        }

        let first = reg.list("a-", 2, None, None).await.unwrap();
        assert_eq!(names(&first), ["a-1", "a-2"]);
        let cursor = first.next_token.clone().expect("a page was dropped");
        assert_eq!(decode_token(&cursor).as_deref(), Some("a-2"));

        let second = reg.list("a-", 2, Some(&cursor), None).await.unwrap();
        assert_eq!(names(&second), ["a-3"]);
        assert!(second.next_token.is_none(), "the listing did not end");

        // The prefix is a filter and not a suggestion.
        let all = reg.list("", 10, None, None).await.unwrap();
        assert_eq!(names(&all), ["a-1", "a-2", "a-3", "b-1"]);
        assert!(all.next_token.is_none());
    }

    #[tokio::test]
    async fn a_forged_page_token_is_refused_rather_than_restarting_the_listing() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        let e = reg
            .list("", 10, Some("!!not base64!!"), None)
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameterValue));
    }

    /// A page the store's own byte budget cut short is CONTINUED, not returned as
    /// a short answer: a ListQueues that dropped rows under-reports the account.
    #[tokio::test]
    async fn a_truncated_page_is_walked_to_the_limit() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        for i in 0..7 {
            create(&reg, &format!("q-{i}"), &[]).await.unwrap();
        }
        api.kv_truncate_reads_at(2);
        api.kv_calls.lock().unwrap().clear();

        let page = reg.list("q-", 5, None, None).await.unwrap();
        assert_eq!(page.queues.len(), 5, "the walk stopped at the first page");
        assert!(page.next_token.is_some());
        assert!(
            calls(&api) >= 3,
            "the pages were not walked: {}",
            calls(&api)
        );

        // The raw walker answers everything under the prefix, budget or not.
        let all = reg.list_queues("q-", MAX_LISTED, None).await.unwrap();
        assert_eq!(all.len(), 7);
    }

    fn names(page: &Page) -> Vec<String> {
        page.queues.iter().map(|q| q.name.clone()).collect()
    }

    // ------------------------------------------------------------ attributes

    #[tokio::test]
    async fn set_attributes_merges_rather_than_replacing() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[("VisibilityTimeout", "45")])
            .await
            .unwrap();

        let record = reg
            .set_attributes("orders", &attrs(&[("DelaySeconds", "10")]), None)
            .await
            .unwrap();
        assert_eq!(record.attributes.get("VisibilityTimeout").unwrap(), "45");
        assert_eq!(record.attributes.get("DelaySeconds").unwrap(), "10");
        assert_eq!(record.attributes.get(ATTR_PARTITIONS).unwrap(), "64");
    }

    #[tokio::test]
    async fn an_immutable_or_unknown_attribute_is_refused_by_name() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        for attribute in [ATTR_FIFO, ATTR_PARTITIONS, "NoSuchAttribute"] {
            let e = reg
                .set_attributes("orders", &attrs(&[(attribute, "1")]), None)
                .await
                .unwrap_err();
            assert_eq!(
                e.kind(),
                Some(ErrorKind::InvalidAttributeName),
                "{attribute} was accepted"
            );
        }
        // ...and the create-only pair IS settable at create.
        assert!(create(&reg, "wide", &[(ATTR_PARTITIONS, "8")])
            .await
            .is_ok());
    }

    #[test]
    fn a_value_outside_its_documented_range_is_refused() {
        for (name, value, ok) in [
            ("VisibilityTimeout", "0", true),
            ("VisibilityTimeout", "43200", true),
            ("VisibilityTimeout", "43201", false),
            ("VisibilityTimeout", "-1", false),
            ("VisibilityTimeout", "half an hour", false),
            ("MaximumMessageSize", "1048576", true),
            ("ReceiveMessageWaitTimeSeconds", "21", false),
            ("queen.partitions", "0", false),
            ("ContentBasedDeduplication", "true", true),
            ("ContentBasedDeduplication", "yes", false),
            ("RedrivePolicy", r#"{"maxReceiveCount":5}"#, true),
            ("RedrivePolicy", "not json", false),
            // Stored, never enforced, never validated.
            ("Policy", "{not json at all", true),
        ] {
            let got = validate_attributes(&attrs(&[(name, value)]), When::Set);
            assert_eq!(got.is_ok(), ok, "{name}={value}: {got:?}");
        }
    }

    /// `LastModifiedTimestamp` is a stored fact and not a guess: every write
    /// through the mutate path stamps it, and a record written before the field
    /// existed reads back as its own creation time — which is the truth for a
    /// queue nobody has changed and the only honest answer for one that was.
    #[tokio::test]
    async fn a_write_stamps_the_modified_timestamp_and_an_old_row_falls_back() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let created = create(&reg, "orders", &[]).await.unwrap();
        assert_eq!(
            created.modified_ms, created.created_ms,
            "a queue nobody has changed was last modified when it was created"
        );

        // A row from before the field existed, and from long ago: what a write
        // does to it is visible without waiting for a wall clock to move.
        let ancient = QueueRecord {
            name: "orders".to_string(),
            attributes: attrs(&[("queen.partitions", "64")]),
            created_ms: 1_600_000_000_000,
            arn: naming().arn("orders"),
            ..QueueRecord::default()
        };
        let mut stored = ancient.to_value();
        stored
            .as_object_mut()
            .expect("an object")
            .remove("modifiedTs");
        api.kv(&[KvOp::put(NS, "qs:q:orders", stored)], None)
            .await
            .expect("written");
        let read = reg.queue_fresh("orders", None).await.unwrap().unwrap();
        assert_eq!(
            read.modified_ms, 1_600_000_000_000,
            "no field, no invention"
        );

        let after = reg
            .set_attributes("orders", &attrs(&[("VisibilityTimeout", "45")]), None)
            .await
            .unwrap();
        assert!(
            after.modified_ms > read.modified_ms,
            "{} is not after {}",
            after.modified_ms,
            read.modified_ms
        );
        assert_eq!(after.created_ms, read.created_ms, "creation is not a write");
    }

    // ---------------------------------------------------------------- CAS

    /// A concurrent SetQueueAttributes from another instance: the loser re-merges
    /// onto the WINNER's record and writes once more, so neither client's change
    /// is dropped.
    #[tokio::test]
    async fn a_lost_cas_is_retried_once_onto_the_winner() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();

        let competitor = QueueRecord {
            name: "orders".to_string(),
            attributes: attrs(&[("queen.partitions", "64"), ("DelaySeconds", "5")]),
            arn: naming().arn("orders"),
            ..QueueRecord::default()
        };
        // The read passes, the first write loses to this.
        api.kv_interpose.lock().unwrap().extend([
            None,
            Some(KvOp::put(NS, "qs:q:orders", competitor.to_value())),
        ]);

        let record = reg
            .set_attributes("orders", &attrs(&[("VisibilityTimeout", "45")]), None)
            .await
            .unwrap();
        assert_eq!(record.attributes.get("VisibilityTimeout").unwrap(), "45");
        assert_eq!(
            record.attributes.get("DelaySeconds").unwrap(),
            "5",
            "the retry overwrote the winner instead of merging onto it"
        );
    }

    #[tokio::test]
    async fn losing_twice_is_a_retriable_error_and_not_a_silent_overwrite() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        let competitor = QueueRecord {
            name: "orders".to_string(),
            arn: naming().arn("orders"),
            ..QueueRecord::default()
        };
        let op = KvOp::put(NS, "qs:q:orders", competitor.to_value());
        api.kv_interpose
            .lock()
            .unwrap()
            .extend([None, Some(op.clone()), Some(op)]);

        let e = reg
            .set_attributes("orders", &attrs(&[("DelaySeconds", "1")]), None)
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::ServiceUnavailable));
    }

    /// A queue deleted between the read and the write is an ABSENCE, not a
    /// competitor: re-applying would resurrect it.
    #[tokio::test]
    async fn a_queue_deleted_mid_update_is_not_resurrected() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        api.kv_interpose
            .lock()
            .unwrap()
            .extend([None, Some(KvOp::delete(NS, "qs:q:orders", None))]);

        let e = reg
            .set_attributes("orders", &attrs(&[("DelaySeconds", "1")]), None)
            .await
            .unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::QueueDoesNotExist));
        assert!(api.kv_get(NS, "qs:q:orders").is_none());
    }

    // ---------------------------------------------------------------- tags

    #[tokio::test]
    async fn tags_round_trip_and_untag_is_idempotent() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();

        reg.tag(
            "orders",
            &attrs(&[("team", "billing"), ("env", "prod")]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            reg.tags("orders", None).await.unwrap(),
            attrs(&[("team", "billing"), ("env", "prod")])
        );

        reg.untag("orders", &["env".to_string(), "absent".to_string()], None)
            .await
            .unwrap();
        assert_eq!(
            reg.tags("orders", None).await.unwrap(),
            attrs(&[("team", "billing")])
        );

        // Tagging never touches the attributes.
        assert_eq!(
            reg.require("orders", None)
                .await
                .unwrap()
                .attributes
                .get(ATTR_PARTITIONS)
                .unwrap(),
            "64"
        );
    }

    #[tokio::test]
    async fn a_queue_cannot_carry_more_tags_than_aws_allows() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        create(&reg, "orders", &[]).await.unwrap();
        let many: BTreeMap<String, String> = (0..=MAX_TAGS)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        let e = reg.tag("orders", &many, None).await.unwrap_err();
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameterValue));
    }

    // --------------------------------------------------------------- cache

    #[tokio::test]
    async fn a_hot_read_is_served_without_a_call_but_a_write_is_never() {
        let api = FakeQueen::with(&[]);
        let reg = Registry::new(api.clone());
        create(&reg, "orders", &[]).await.unwrap();
        let after_create = calls(&api);

        reg.require("orders", None).await.unwrap();
        reg.require("orders", None).await.unwrap();
        assert_eq!(calls(&api), after_create, "a hot read went to Queen");

        // The write path reads FRESH: a CAS on a cached version would lose a race
        // it was never in.
        reg.set_attributes("orders", &attrs(&[("DelaySeconds", "1")]), None)
            .await
            .unwrap();
        assert_eq!(calls(&api), after_create + 2, "the CAS read from the cache");
    }

    /// A cache keyed by name alone would serve one tenant another's attributes:
    /// the namespace is per-tenant on the broker side.
    #[tokio::test]
    async fn the_cache_is_keyed_by_credential() {
        let api = FakeQueen::with(&[]);
        let reg = Registry::new(api.clone());
        reg.create(
            "orders",
            &BTreeMap::new(),
            &BTreeMap::new(),
            &naming(),
            64,
            Some("tenant-a"),
        )
        .await
        .unwrap();

        let before = calls(&api);
        reg.queue("orders", Some("tenant-a")).await.unwrap();
        assert_eq!(calls(&api), before, "tenant-a was not cached");
        reg.queue("orders", Some("tenant-b")).await.unwrap();
        assert_eq!(calls(&api), before + 1, "tenant-b was served from a cache");
    }

    #[tokio::test]
    async fn a_delete_forgets_the_record_immediately() {
        let api = FakeQueen::with(&[]);
        let reg = Registry::new(api.clone());
        create(&reg, "orders", &[]).await.unwrap();
        reg.require("orders", None).await.unwrap();
        reg.delete("orders", None).await.unwrap();
        assert_eq!(
            reg.require("orders", None).await.unwrap_err().kind(),
            Some(ErrorKind::QueueDoesNotExist),
            "the deleted queue was served from the cache"
        );
    }

    // ------------------------------------------------------------ failures

    /// The distinction the whole error type exists for: Queen being unreachable
    /// must never read as "no such queue", or an SDK stops instead of retrying.
    #[tokio::test]
    async fn a_store_failure_is_never_a_missing_queue() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        let e = reg.require("orders", None).await.unwrap_err();
        assert_eq!(
            e.kind(),
            None,
            "a transport failure was given a client code"
        );
        assert!(matches!(e, RegistryError::Store(_)), "{e:?}");
    }

    // ------------------------------------------------------ rows and naming

    /// The KEY is the existence claim: a row missing a field is still a queue,
    /// and dropping it would hide a queue clients are sending to.
    #[test]
    fn a_record_that_lost_a_field_still_names_a_queue() {
        let record = QueueRecord::from_value("orders", &serde_json::json!({}), 7);
        assert_eq!(record.name, "orders");
        assert_eq!(record.version, 7);
        assert_eq!(record.partitions, crate::config::DEFAULT_PARTITIONS);
        assert!(record.attributes.is_empty());

        // A non-string attribute value is corruption; it is skipped, never
        // coerced into a value a client would then read back as its own.
        let odd = QueueRecord::from_value(
            "orders",
            &serde_json::json!({"attributes": {"VisibilityTimeout": 45, "DelaySeconds": "3"}}),
            1,
        );
        assert_eq!(odd.attributes, attrs(&[("DelaySeconds", "3")]));
    }

    #[test]
    fn a_record_round_trips_through_its_stored_shape() {
        let record = QueueRecord {
            name: "orders".to_string(),
            attributes: attrs(&[("VisibilityTimeout", "45"), ("queen.partitions", "8")]),
            tags: attrs(&[("team", "billing")]),
            partitions: 8,
            fifo: false,
            created_ms: 1_787_011_200_000,
            modified_ms: 1_787_097_600_000,
            arn: "arn:aws:sqs:queen-1:000000000000:orders".to_string(),
            version: 12,
        };
        assert_eq!(
            QueueRecord::from_value("orders", &record.to_value(), 12),
            record
        );
    }

    #[test]
    fn a_url_and_an_arn_are_the_documented_shapes() {
        let naming = naming();
        assert_eq!(
            naming.url("http", "localhost:9324", "orders"),
            "http://localhost:9324/000000000000/orders"
        );
        assert_eq!(
            naming.url("https", "sqs.example.com", "orders.fifo"),
            "https://sqs.example.com/000000000000/orders.fifo"
        );
        assert_eq!(
            naming.arn("orders"),
            "arn:aws:sqs:queen-1:000000000000:orders"
        );
    }

    /// `QueueUrl` is a client-supplied string on every message action, so this is
    /// a parser of untrusted input: tolerant about the host, strict about the two
    /// segments that mean something.
    #[test]
    fn a_queue_url_parses_back_to_its_name() {
        let naming = naming();
        for (url, want) in [
            ("http://localhost:9324/000000000000/orders", Some("orders")),
            (
                "https://sqs.example.com/000000000000/orders",
                Some("orders"),
            ),
            // A reverse proxy's own path prefix, and a trailing slash.
            ("https://x/sqs/000000000000/orders/", Some("orders")),
            ("http://h/000000000000/orders?Action=X", Some("orders")),
            ("000000000000/orders", Some("orders")),
            ("http://h/000000000000/orders.fifo", Some("orders.fifo")),
            // Another account's queue is not this deployment's.
            ("http://h/999999999999/orders", None),
            // Traversal, in both positions.
            ("http://h/000000000000/..", None),
            ("http://h/000000000000/../../etc/passwd", None),
            ("http://h/000000000000/or ders", None),
            // A bare name has no account segment to check.
            ("orders", None),
            ("", None),
            ("http://h/000000000000/", None),
        ] {
            assert_eq!(naming.name_of(url).as_deref(), want, "{url}");
        }
    }

    /// The ARN mirror, run backwards on untrusted input: `deadLetterTargetArn`,
    /// `SourceArn` and `DestinationArn` are all strings a client composed.
    #[test]
    fn a_queue_arn_parses_back_to_its_name() {
        let naming = naming();
        for (arn, want) in [
            ("arn:aws:sqs:queen-1:000000000000:orders", Some("orders")),
            (
                "arn:aws:sqs:queen-1:000000000000:orders.fifo",
                Some("orders.fifo"),
            ),
            // Another AWS realm, with a queue name that is otherwise exactly
            // right: the partition names a world this deployment is not in, and
            // refusing it would fail a client that composed the ARN from its own
            // configured partition.
            ("arn:aws-cn:sqs:queen-1:000000000000:orders", Some("orders")),
            // A dead-letter target must share the account AND the region with
            // its source, which is AWS's own rule.
            ("arn:aws:sqs:queen-1:999999999999:orders", None),
            ("arn:aws:sqs:eu-west-1:000000000000:orders", None),
            // Another service's ARN, and a queue name with a colon in it —
            // which no queue name has, so the split is unambiguous.
            ("arn:aws:sns:queen-1:000000000000:orders", None),
            ("arn:aws:sqs:queen-1:000000000000:or:ders", None),
            ("arn:aws:sqs:queen-1:000000000000:or ders", None),
            ("arn:aws:sqs:queen-1:000000000000:", None),
            ("arn:aws:sqs:queen-1:000000000000:orders:extra", None),
            ("orders", None),
            ("", None),
        ] {
            assert_eq!(naming.name_of_arn(arn).as_deref(), want, "{arn}");
        }
        // Every ARN this deployment mints is one it reads back.
        for name in ["orders", "orders.fifo", "a-1_2"] {
            assert_eq!(naming.name_of_arn(&naming.arn(name)).as_deref(), Some(name));
        }
    }

    /// The move-task key space: one prefix per SOURCE, ordered newest first, and
    /// a source's prefix that cannot reach into another source's tasks.
    #[test]
    fn the_move_task_keys_are_per_source_and_newest_first() {
        let older = Registry::key_move_task("orders-dlq", 1_000, "a");
        let newer = Registry::key_move_task("orders-dlq", 2_000, "a");
        let same_instant = Registry::key_move_task("orders-dlq", 2_000, "b");
        assert!(newer < older, "{newer} must sort before {older}");
        assert_ne!(newer, same_instant, "two tasks, two rows");

        let prefix = Registry::key_move_tasks("orders-dlq");
        assert!(older.starts_with(&prefix));
        assert!(!Registry::key_move_task("orders-dlq-2", 1_000, "a").starts_with(&prefix));
        // A name that would otherwise escape the key space is escaped into it.
        assert!(
            Registry::key_move_task("a:b", 1, "x").starts_with(&Registry::key_move_tasks("a:b"))
        );
        assert_ne!(
            Registry::key_move_fence("orders-dlq"),
            Registry::key_move_fence("orders-dlq-2")
        );
    }

    #[test]
    fn a_page_token_survives_the_wire_it_travels_on() {
        for name in ["orders", "a-1", "orders.fifo", &"q".repeat(80)] {
            let cursor = encode_token(name);
            assert!(
                cursor
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "{cursor} needs escaping in a query string"
            );
            assert_eq!(decode_token(&cursor).as_deref(), Some(name));
        }
        // Anything this facade did not mint.
        for forged in ["", "!!!", &encode_token("../etc"), &encode_token("")] {
            assert_eq!(decode_token(forged), None, "{forged}");
        }
    }
}
