//! The closed error catalog, and the only place an SQS error code is written.
//!
//! CONTRACT. Every failure this facade reports is one [`ErrorKind`] below. The
//! set is CLOSED, kafka's discipline (`ERRORS.md`: the file is the contract, a
//! new code is a reviewed event) and for the same reason: SDK retry behaviour is
//! keyed off these strings, so inventing one at a call site is inventing a
//! client behaviour. Adding a variant means adding it here, giving it BOTH
//! renderings and a status, and saying in one line what a client does about it.
//!
//! Two renderings, because there are two protocols on one listener:
//!
//!   * **AWS JSON 1.0** — `__type: "com.amazonaws.sqs#QueueDoesNotExist"` in the
//!     body, plus the `x-amzn-query-error` header some SDK majors still read
//!     when they speak JSON to a Query-era service model.
//!   * **Query/XML** — `<ErrorResponse><Error><Type>Sender</Type><Code>AWS.
//!     SimpleQueueService.NonExistentQueue</Code>…`.
//!
//! The two spellings of one error are NOT the same string, and that is the whole
//! reason this type exists rather than a `&'static str` at each call site: the
//! Query code for a missing queue is `AWS.SimpleQueueService.NonExistentQueue`
//! while its JSON type is `QueueDoesNotExist`, and a client that gets the wrong
//! one treats a permanent failure as retriable.
//!
//! Where the two differ they differ in AWS's own way, which is not one rule but
//! two eras of one model: the JSON spelling is the SHAPE name of the current
//! service model, the Query spelling is the `error.code` the 2012-11-05 model
//! carried. For most errors those coincide; for six they do not, and for
//! `QueueAlreadyExists` the pair is even INVERTED relative to
//! `QueueDoesNotExist` — JSON says `QueueNameExists` (the shape) while Query
//! says `QueueAlreadyExists` (the legacy code). None of it is derivable, which
//! is why it is a table.
//!
//! `Sender` vs `Receiver` is not decoration either: it is what decides whether
//! an SDK retries at all.
//!
//! ## Two services, one catalog
//!
//! SNS shares this listener and does NOT share SQS's codes. Its half of the set
//! is marked below, and three of its facts are not SQS's:
//!
//!   * a missing topic is `NotFound` with **HTTP 404**, where every SQS "does not
//!     exist" is a 400;
//!   * a bad parameter is `InvalidParameter`, not `InvalidParameterValue` —
//!     different string, and a client's catch block is written against one of
//!     them;
//!   * the JSON `__type` prefix is `com.amazonaws.sns#`
//!     ([`ErrorKind::json_namespace`]), which is why the renderer asks the error
//!     rather than carrying one namespace constant.
//!
//! ## The one place the catalog leaves AWS's own numbers
//!
//! Nothing. Every code below is a real AWS code with AWS's own status. What is
//! a DECISION is which of them a Queen failure becomes, and one of those is
//! worth stating here: a 429 from Queen is [`ErrorKind::RequestThrottled`] and
//! not [`ErrorKind::OverLimit`], because retry is driven by the CODE and not by
//! the status — botocore, aws-sdk-go-v2 and the rest carry a list of throttling
//! code strings, `RequestThrottled` is on it and `OverLimit` is not. Answering
//! `OverLimit` to a rate cap would be answering "your request was wrong" to a
//! client whose request was right, and every SDK would stop instead of backing
//! off. `OverLimit` stays in the set for AWS's own condition (a limit the
//! REQUEST violates), which this facade does not emulate today.

use crate::queen;

/// The `__type` prefix of an SQS error in the JSON 1.0 rendering.
pub const JSON_NAMESPACE_SQS: &str = "com.amazonaws.sqs#";
/// SNS's, for the half of the catalog that is SNS's. It is a different service
/// model, and a client that matched on the prefix would not recognise its own
/// exception under the other one.
pub const JSON_NAMESPACE_SNS: &str = "com.amazonaws.sns#";

/// Which side is at fault, in AWS's own words. `Sender` is a request that will
/// fail the same way if repeated; `Receiver` is one that may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    Sender,
    Receiver,
}

impl Fault {
    /// The word AWS writes in `<Type>` and after the semicolon in
    /// `x-amzn-query-error`.
    pub fn as_str(self) -> &'static str {
        match self {
            Fault::Sender => "Sender",
            Fault::Receiver => "Receiver",
        }
    }
}

/// The closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    // ------------------------------------------------ the plan's own catalog
    /// The queue named by the URL is not there. Terminal: an SDK stops.
    QueueDoesNotExist,
    /// The handle does not decode, its tag does not verify, or it has expired.
    ReceiptHandleIsInvalid,
    /// The handle is well-formed but names a lease that is gone — the message
    /// was already deleted, or its visibility timeout lapsed and it was
    /// redelivered under a new lease.
    MessageNotInflight,
    PurgeQueueInProgress,
    BatchEntryIdsNotDistinct,
    TooManyEntriesInBatchRequest,
    EmptyBatchRequest,
    InvalidBatchEntryId,
    BatchRequestTooLong,
    InvalidAttributeName,
    /// Emulated for the documented 60 seconds. SDK retry behaviour depends on
    /// it, which is why the window is emulated rather than skipped.
    QueueDeletedRecently,
    /// Only on an ATTRIBUTE MISMATCH, per AWS: creating a queue that already
    /// exists with the same attributes is a success.
    QueueAlreadyExists,

    // ------------------------------------- the wire cannot function without
    /// No `Action`/`X-Amz-Target`, or one this facade does not implement.
    InvalidAction,
    /// A parameter is missing, malformed, or outside its documented range.
    InvalidParameterValue,
    MissingParameter,
    /// The body is not valid for the queue: over `MaximumMessageSize`, or
    /// carrying characters the SQS charset forbids.
    InvalidMessageContents,
    /// The request carried no credential at all.
    MissingAuthenticationToken,
    /// The signature did not verify, or its clock is outside the skew window.
    SignatureDoesNotMatch,
    IncompleteSignature,
    /// The access key id is not one this deployment knows.
    InvalidClientTokenId,
    /// The credential verified and the principal may not do this.
    AccessDenied,
    /// AWS's own limit error: the REQUEST violates a limit (its in-flight cap,
    /// its permission count). This facade does not emulate those caps — quota
    /// theater is a stated non-goal — so nothing maps onto it today; it is in
    /// the set because the code is AWS's and a future cap would use it.
    OverLimit,
    /// The named resource is not there, for the three actions that answer it
    /// instead of `QueueDoesNotExist`: the message-move-task trio, which AWS
    /// added in 2023 with the modern shape rather than the 2012 one. A
    /// `TaskHandle` naming no task and a `SourceArn` naming no queue are both
    /// this, and an SDK's `StartMessageMoveTask` models exactly this code — the
    /// 2012 spelling would reach a client's catch block for a queue URL nobody
    /// sent.
    ResourceNotFoundException,
    /// The request is well-formed, names things that exist, and asks for
    /// something this service does not do for them: a move task on a queue that
    /// is not a dead-letter target, a second task on a source that already has
    /// one running, a cancel of a task that has already finished. AWS's own code
    /// for all three.
    UnsupportedOperation,
    /// Queen answered 429 — a rate cap or a freeze. Carries the backoff, and is
    /// the code every SDK's throttling list recognizes (see the module header).
    RequestThrottled,
    /// Queen could not be reached, or answered something this facade cannot
    /// turn into an answer. The only `Receiver` faults in the set.
    ServiceUnavailable,
    InternalFailure,

    // -------------------------------------------------------------- sns (M4)
    /// SNS's "that resource is not there": a `TopicArn` naming no topic, a
    /// `SubscriptionArn` naming no subscription. **404**, which is not a typo
    /// and not a copy of `QueueDoesNotExist`'s 400 — SNS answers a status SQS
    /// never does, and an SDK's retry classifier reads the status before it
    /// reads the code.
    NotFound,
    /// SNS's parameter refusal. NOT `InvalidParameterValue`: the two services
    /// spell it differently, SNS's messages are `Invalid parameter: <Member>
    /// Reason: <why>`, and it is the code every SNS client catches.
    InvalidParameter,
    /// The answer the three TAG actions give for a topic that is not there.
    /// `NotFound` would be the neighbouring code and it is the wrong one:
    /// `TagResource`/`UntagResource`/`ListTagsForResource` were modelled in 2020
    /// with the modern shape and answer `ResourceNotFound`, which is what an
    /// SDK's exception mapping is generated from. Same reasoning as
    /// [`ErrorKind::ResourceNotFoundException`] on the SQS side, and — like it —
    /// the same spelling in both protocols.
    ResourceNotFound,
    /// More tags on one topic than SNS allows.
    TagLimitExceeded,

    // The five batch refusals `PublishBatch` shares with SQS's three batch
    // actions BY NAME and not by spelling. SQS's 2012-era Query codes carry the
    // `AWS.SimpleQueueService.` prefix; SNS was never in that namespace and
    // answers the bare name, and SNS is the Query-only service, so the prefix is
    // exactly the byte an SNS client's error mapping reads. Reusing the SQS
    // variants would have put an SQS service prefix in an SNS document — one
    // more reason the catalog is a closed set with a row per failure rather than
    // a set of strings shared by whoever needs one.
    SnsEmptyBatchRequest,
    SnsTooManyEntriesInBatchRequest,
    SnsBatchEntryIdsNotDistinct,
    SnsInvalidBatchEntryId,
    SnsBatchRequestTooLong,
}

/// One row of the catalog. Everything that must be decided TOGETHER for one
/// failure sits in one literal, so the two spellings, the status and the fault
/// cannot drift apart in separate matches.
struct Entry {
    json: &'static str,
    query: &'static str,
    status: u16,
    fault: Fault,
    message: &'static str,
}

impl ErrorKind {
    /// Every variant, for the tests that walk the catalog and for the reference
    /// page generated from it. The compiler enforces the set on
    /// [`ErrorKind::entry`] — a new variant does not compile without a row —
    /// and [`tests::the_catalog_is_whole`] enforces it on this list.
    pub const ALL: [ErrorKind; 36] = [
        ErrorKind::QueueDoesNotExist,
        ErrorKind::ReceiptHandleIsInvalid,
        ErrorKind::MessageNotInflight,
        ErrorKind::PurgeQueueInProgress,
        ErrorKind::BatchEntryIdsNotDistinct,
        ErrorKind::TooManyEntriesInBatchRequest,
        ErrorKind::EmptyBatchRequest,
        ErrorKind::InvalidBatchEntryId,
        ErrorKind::BatchRequestTooLong,
        ErrorKind::InvalidAttributeName,
        ErrorKind::QueueDeletedRecently,
        ErrorKind::QueueAlreadyExists,
        ErrorKind::InvalidAction,
        ErrorKind::InvalidParameterValue,
        ErrorKind::MissingParameter,
        ErrorKind::InvalidMessageContents,
        ErrorKind::MissingAuthenticationToken,
        ErrorKind::SignatureDoesNotMatch,
        ErrorKind::IncompleteSignature,
        ErrorKind::InvalidClientTokenId,
        ErrorKind::AccessDenied,
        ErrorKind::OverLimit,
        ErrorKind::ResourceNotFoundException,
        ErrorKind::UnsupportedOperation,
        ErrorKind::RequestThrottled,
        ErrorKind::ServiceUnavailable,
        ErrorKind::InternalFailure,
        ErrorKind::NotFound,
        ErrorKind::InvalidParameter,
        ErrorKind::ResourceNotFound,
        ErrorKind::TagLimitExceeded,
        ErrorKind::SnsEmptyBatchRequest,
        ErrorKind::SnsTooManyEntriesInBatchRequest,
        ErrorKind::SnsBatchEntryIdsNotDistinct,
        ErrorKind::SnsInvalidBatchEntryId,
        ErrorKind::SnsBatchRequestTooLong,
    ];

    /// The `__type` of the JSON 1.0 rendering, WITHOUT the namespace prefix the
    /// renderer adds ([`ErrorKind::json_namespace`]).
    pub fn json_type(self) -> &'static str {
        self.entry().json
    }

    /// Which service model this code belongs to, as the `__type` prefix.
    ///
    /// A METHOD and not a constant in the renderer, because the two services
    /// share one listener and one codec: the request that raised the error is
    /// the only thing that knows which API was addressed, and by the time an
    /// error is rendered the codec has deliberately forgotten
    /// ([`crate::proto`]'s contract). The error itself is what remembers.
    pub fn json_namespace(self) -> &'static str {
        match self {
            ErrorKind::NotFound
            | ErrorKind::InvalidParameter
            | ErrorKind::ResourceNotFound
            | ErrorKind::TagLimitExceeded
            | ErrorKind::SnsEmptyBatchRequest
            | ErrorKind::SnsTooManyEntriesInBatchRequest
            | ErrorKind::SnsBatchEntryIdsNotDistinct
            | ErrorKind::SnsInvalidBatchEntryId
            | ErrorKind::SnsBatchRequestTooLong => JSON_NAMESPACE_SNS,
            _ => JSON_NAMESPACE_SQS,
        }
    }

    /// The `<Code>` of the Query/XML rendering, which for several of these is
    /// NOT the JSON spelling.
    pub fn query_code(self) -> &'static str {
        self.entry().query
    }

    /// The kind a JSON `__type` spells, or `None` for a string this catalog did
    /// not mint.
    ///
    /// The action layer is protocol-blind ([`crate::actions`]'s contract), so a
    /// `BatchResultErrorEntry` is built with one spelling of its code and the
    /// Query codec translates it back through here — the only place that knows
    /// both halves. The scan is over [`ErrorKind::ALL`], on the failure path of
    /// a batch entry.
    ///
    /// The NAMESPACE is a parameter because five codes are spelled identically
    /// by the two services and rendered differently
    /// (`EmptyBatchRequest` is `AWS.SimpleQueueService.EmptyBatchRequest` in
    /// SQS's Query rendering and bare in SNS's), so the answer depends on which
    /// API the request addressed. A `json` that no kind of that service has
    /// falls back to the other's, because a per-entry failure of an SNS action
    /// can still carry a shared kind like `ServiceUnavailable`.
    pub fn of_json_type(json: &str, sns: bool) -> Option<ErrorKind> {
        let want = match sns {
            true => JSON_NAMESPACE_SNS,
            false => JSON_NAMESPACE_SQS,
        };
        ErrorKind::ALL
            .into_iter()
            .find(|k| k.json_type() == json && k.json_namespace() == want)
            .or_else(|| ErrorKind::ALL.into_iter().find(|k| k.json_type() == json))
    }

    pub fn http_status(self) -> u16 {
        self.entry().status
    }

    pub fn fault(self) -> Fault {
        self.entry().fault
    }

    /// What a client is told when the call site has nothing more specific. Every
    /// variant has one: an error with an empty message is one an operator cannot
    /// act on from a client's log.
    pub fn default_message(self) -> &'static str {
        self.entry().message
    }

    fn entry(self) -> Entry {
        match self {
            ErrorKind::QueueDoesNotExist => Entry {
                json: "QueueDoesNotExist",
                query: "AWS.SimpleQueueService.NonExistentQueue",
                status: 400,
                fault: Fault::Sender,
                message: "The specified queue does not exist.",
            },
            ErrorKind::ReceiptHandleIsInvalid => Entry {
                json: "ReceiptHandleIsInvalid",
                query: "ReceiptHandleIsInvalid",
                status: 400,
                fault: Fault::Sender,
                message: "The specified receipt handle isn't valid.",
            },
            ErrorKind::MessageNotInflight => Entry {
                json: "MessageNotInflight",
                query: "AWS.SimpleQueueService.MessageNotInflight",
                status: 400,
                fault: Fault::Sender,
                message: "The specified message isn't in flight.",
            },
            ErrorKind::PurgeQueueInProgress => Entry {
                json: "PurgeQueueInProgress",
                query: "AWS.SimpleQueueService.PurgeQueueInProgress",
                // 403, which reads like a typo and is not: it is the status AWS
                // documents for this one, and an SDK that special-cases the
                // cooldown branches on the pair.
                status: 403,
                fault: Fault::Sender,
                message: "Only one PurgeQueue operation on each queue is allowed every 60 seconds.",
            },
            ErrorKind::BatchEntryIdsNotDistinct => Entry {
                json: "BatchEntryIdsNotDistinct",
                query: "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                status: 400,
                fault: Fault::Sender,
                message: "Two or more batch entries in the request have the same Id.",
            },
            ErrorKind::TooManyEntriesInBatchRequest => Entry {
                json: "TooManyEntriesInBatchRequest",
                query: "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
                status: 400,
                fault: Fault::Sender,
                message: "The batch request contains more entries than permissible.",
            },
            ErrorKind::EmptyBatchRequest => Entry {
                json: "EmptyBatchRequest",
                query: "AWS.SimpleQueueService.EmptyBatchRequest",
                status: 400,
                fault: Fault::Sender,
                message: "The batch request doesn't contain any entries.",
            },
            ErrorKind::InvalidBatchEntryId => Entry {
                json: "InvalidBatchEntryId",
                query: "AWS.SimpleQueueService.InvalidBatchEntryId",
                status: 400,
                fault: Fault::Sender,
                message: "The Id of a batch entry in a batch request doesn't abide by the \
                          specification.",
            },
            ErrorKind::BatchRequestTooLong => Entry {
                json: "BatchRequestTooLong",
                query: "AWS.SimpleQueueService.BatchRequestTooLong",
                status: 400,
                fault: Fault::Sender,
                message: "The length of all the messages put together is more than the limit.",
            },
            ErrorKind::InvalidAttributeName => Entry {
                json: "InvalidAttributeName",
                query: "InvalidAttributeName",
                status: 400,
                fault: Fault::Sender,
                message: "The specified attribute doesn't exist.",
            },
            ErrorKind::QueueDeletedRecently => Entry {
                json: "QueueDeletedRecently",
                query: "AWS.SimpleQueueService.QueueDeletedRecently",
                status: 400,
                fault: Fault::Sender,
                message: "You must wait 60 seconds after deleting a queue before you can create \
                          another with the same name.",
            },
            ErrorKind::QueueAlreadyExists => Entry {
                // The inverted pair; see the module header.
                json: "QueueNameExists",
                query: "QueueAlreadyExists",
                status: 400,
                fault: Fault::Sender,
                message: "A queue with this name already exists with different attributes.",
            },
            ErrorKind::InvalidAction => Entry {
                json: "InvalidAction",
                query: "InvalidAction",
                status: 400,
                fault: Fault::Sender,
                message: "The action or operation requested is invalid. Verify that the action \
                          is typed correctly.",
            },
            ErrorKind::InvalidParameterValue => Entry {
                json: "InvalidParameterValue",
                query: "InvalidParameterValue",
                status: 400,
                fault: Fault::Sender,
                message: "An invalid or out-of-range value was supplied for the input parameter.",
            },
            ErrorKind::MissingParameter => Entry {
                json: "MissingParameter",
                query: "MissingParameter",
                status: 400,
                fault: Fault::Sender,
                message: "The request is missing a required parameter.",
            },
            ErrorKind::InvalidMessageContents => Entry {
                json: "InvalidMessageContents",
                query: "InvalidMessageContents",
                status: 400,
                fault: Fault::Sender,
                message: "The message contains characters outside the allowed set.",
            },
            ErrorKind::MissingAuthenticationToken => Entry {
                json: "MissingAuthenticationToken",
                query: "MissingAuthenticationToken",
                status: 403,
                fault: Fault::Sender,
                message: "The request is unsigned: it carries neither an Authorization header \
                          nor presigned query parameters.",
            },
            ErrorKind::SignatureDoesNotMatch => Entry {
                json: "SignatureDoesNotMatch",
                query: "SignatureDoesNotMatch",
                status: 403,
                fault: Fault::Sender,
                message: "The request signature we calculated does not match the signature you \
                          provided. Check your secret access key and signing method.",
            },
            ErrorKind::IncompleteSignature => Entry {
                json: "IncompleteSignature",
                query: "IncompleteSignature",
                status: 400,
                fault: Fault::Sender,
                message: "The request signature does not conform to AWS standards.",
            },
            ErrorKind::InvalidClientTokenId => Entry {
                json: "InvalidClientTokenId",
                query: "InvalidClientTokenId",
                status: 403,
                fault: Fault::Sender,
                message: "The security token included in the request is invalid.",
            },
            ErrorKind::AccessDenied => Entry {
                json: "AccessDenied",
                query: "AccessDenied",
                status: 403,
                fault: Fault::Sender,
                message: "Access to the requested resource is denied.",
            },
            ErrorKind::OverLimit => Entry {
                json: "OverLimit",
                query: "OverLimit",
                status: 403,
                fault: Fault::Sender,
                message: "The specified action violates a limit.",
            },
            ErrorKind::ResourceNotFoundException => Entry {
                // The SAME spelling in both protocols, unlike the 2012-era
                // codes above: the three actions that raise it were modelled
                // after SQS had moved to JSON, so there is no
                // `AWS.SimpleQueueService.` prefix to mirror in the Query
                // rendering. Guessing one would invent a string no client
                // branches on.
                json: "ResourceNotFoundException",
                query: "ResourceNotFoundException",
                status: 404,
                fault: Fault::Sender,
                message: "One or more specified resources don't exist.",
            },
            ErrorKind::UnsupportedOperation => Entry {
                json: "UnsupportedOperation",
                query: "AWS.SimpleQueueService.UnsupportedOperation",
                status: 400,
                fault: Fault::Sender,
                message: "Error code 400. Unsupported operation.",
            },
            ErrorKind::RequestThrottled => Entry {
                json: "RequestThrottled",
                query: "RequestThrottled",
                status: 403,
                fault: Fault::Sender,
                message: "The request was denied due to request throttling.",
            },
            ErrorKind::ServiceUnavailable => Entry {
                json: "ServiceUnavailable",
                query: "ServiceUnavailable",
                status: 503,
                fault: Fault::Receiver,
                message: "The request has failed due to a temporary failure of the server.",
            },
            ErrorKind::InternalFailure => Entry {
                json: "InternalFailure",
                query: "InternalFailure",
                status: 500,
                fault: Fault::Receiver,
                message: "The request processing has failed because of an unknown error, \
                          exception or failure.",
            },
            ErrorKind::NotFound => Entry {
                json: "NotFound",
                query: "NotFound",
                // 404. SNS's own status for this code, and the one difference
                // from SQS that a client notices before it has parsed anything.
                status: 404,
                fault: Fault::Sender,
                message: "The requested resource does not exist.",
            },
            ErrorKind::InvalidParameter => Entry {
                json: "InvalidParameter",
                query: "InvalidParameter",
                status: 400,
                fault: Fault::Sender,
                message: "The request contains an invalid parameter.",
            },
            ErrorKind::ResourceNotFound => Entry {
                json: "ResourceNotFound",
                query: "ResourceNotFound",
                status: 404,
                fault: Fault::Sender,
                message: "Can't tag resource. Verify that the topic exists.",
            },
            ErrorKind::TagLimitExceeded => Entry {
                json: "TagLimitExceeded",
                query: "TagLimitExceeded",
                status: 400,
                fault: Fault::Sender,
                message: "Can't add more than 50 tags to a resource.",
            },
            // The five below carry SQS's JSON spelling and NOT its Query
            // spelling; see the variants' own comment.
            ErrorKind::SnsEmptyBatchRequest => Entry {
                json: "EmptyBatchRequest",
                query: "EmptyBatchRequest",
                status: 400,
                fault: Fault::Sender,
                message: "The batch request doesn't contain any entries.",
            },
            ErrorKind::SnsTooManyEntriesInBatchRequest => Entry {
                json: "TooManyEntriesInBatchRequest",
                query: "TooManyEntriesInBatchRequest",
                status: 400,
                fault: Fault::Sender,
                message: "The batch request contains more entries than permissible.",
            },
            ErrorKind::SnsBatchEntryIdsNotDistinct => Entry {
                json: "BatchEntryIdsNotDistinct",
                query: "BatchEntryIdsNotDistinct",
                status: 400,
                fault: Fault::Sender,
                message: "Two or more batch entries in the request have the same Id.",
            },
            ErrorKind::SnsInvalidBatchEntryId => Entry {
                json: "InvalidBatchEntryId",
                query: "InvalidBatchEntryId",
                status: 400,
                fault: Fault::Sender,
                message: "The Id of a batch entry in a batch request doesn't abide by the \
                          specification.",
            },
            ErrorKind::SnsBatchRequestTooLong => Entry {
                json: "BatchRequestTooLong",
                query: "BatchRequestTooLong",
                status: 400,
                fault: Fault::Sender,
                message: "The length of all the messages put together is more than the limit.",
            },
        }
    }
}

/// One error, on its way to a client.
#[derive(Debug, Clone, PartialEq)]
pub struct SqsError {
    pub kind: ErrorKind,
    /// The sentence a client sees. Never a broker body verbatim: Queen's error
    /// text names queues, partitions and stored procedures, and a facade that
    /// echoes it leaks another tenant's vocabulary into this one's logs.
    pub message: String,
    /// Set on [`ErrorKind::RequestThrottled`], from Queen's own `Retry-After`.
    pub retry_after_ms: Option<i64>,
}

pub type SqsResult<T> = std::result::Result<T, SqsError>;

impl SqsError {
    pub fn new(kind: ErrorKind) -> SqsError {
        SqsError {
            kind,
            message: kind.default_message().to_string(),
            retry_after_ms: None,
        }
    }

    pub fn with(kind: ErrorKind, message: impl Into<String>) -> SqsError {
        SqsError {
            kind,
            message: message.into(),
            retry_after_ms: None,
        }
    }

    /// Attach the backoff a client must honour. `None` clears it, so the
    /// builder can be fed an upstream `Option` directly.
    pub fn retry_after(mut self, ms: Option<i64>) -> SqsError {
        self.retry_after_ms = ms;
        self
    }

    /// The one mapping from a broker failure to a client-visible error.
    ///
    /// It lives here and not at the call sites because the mapping is a POLICY:
    /// a 404 from the depth route is `QueueDoesNotExist`, a 429 is
    /// `RequestThrottled` with the backoff Queen named, a 5xx and a transport
    /// failure are both `ServiceUnavailable` (a client must retry, and the
    /// distinction is the operator's not the client's), and a body this facade
    /// could not parse is `InternalFailure` — never a success with missing
    /// fields.
    ///
    /// No branch carries Queen's own words. The upstream STATUS travels on the
    /// one branch that has nothing better to say, because a number names no
    /// queue and is the difference between an operator reading a client's log
    /// and an operator guessing from it.
    pub fn from_queen(e: &queen::Error) -> SqsError {
        match e {
            queen::Error::Transport(_) => SqsError::new(ErrorKind::ServiceUnavailable),
            queen::Error::Status { code, .. } => match *code {
                404 => SqsError::new(ErrorKind::QueueDoesNotExist),
                429 => SqsError::new(ErrorKind::RequestThrottled).retry_after(e.retry_after_ms()),
                // Queen refused the token THIS FACADE presented, not the
                // signature the client presented: the client cannot fix it and
                // the message says whose problem it is.
                401 | 403 => SqsError::with(
                    ErrorKind::AccessDenied,
                    "The queue service refused this endpoint's own credential; the operator must \
                     check the facade's Queen token.",
                ),
                code if code >= 500 => SqsError::new(ErrorKind::ServiceUnavailable),
                code => SqsError::with(
                    ErrorKind::InvalidParameterValue,
                    format!("The queue service refused the request (upstream status {code})."),
                ),
            },
            queen::Error::Body(_) => SqsError::new(ErrorKind::InternalFailure),
            // Nothing was written, and the reason is that everything in the
            // bundle was already there under the caller's own dedup key. The
            // caller that can PRODUCE it answers it as a success
            // ([`crate::sns::publish`]); a caller that reaches this mapping did
            // not expect it, and a repeat will roll back the same way — so the
            // answer names the parameter that decided it rather than inviting a
            // retry.
            queen::Error::Duplicate(_) => SqsError::with(
                ErrorKind::InvalidParameterValue,
                "The message was already written under this deduplication id and the request \
                 wrote nothing.",
            ),
            // A lost precondition means NOTHING was written (024_kv.sql), so the
            // request is safe to repeat and the answer must say retry rather
            // than fail: a Sender fault here would stop an SDK on a race it
            // would win the second time.
            queen::Error::Precondition { .. } => SqsError::with(
                ErrorKind::ServiceUnavailable,
                "A concurrent administrative change won this request's precondition and nothing \
                 was written. Retry.",
            ),
        }
    }
}

impl std::fmt::Display for SqsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.json_type(), self.message)
    }
}

impl std::error::Error for SqsError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walks the whole catalog. What it protects is not spelling but the
    /// INVARIANTS a client depends on: a fault and a status that agree, and two
    /// codes that are each unique, so no two failures are indistinguishable on
    /// the wire.
    #[test]
    fn the_catalog_is_whole() {
        let mut json_types = Vec::new();
        let mut query_codes = Vec::new();
        for kind in ErrorKind::ALL {
            let entry = kind.entry();
            assert!(!entry.json.is_empty(), "{kind:?} has no JSON type");
            assert!(!entry.query.is_empty(), "{kind:?} has no Query code");
            assert!(
                entry.message.len() > 12 && entry.message.ends_with('.'),
                "{kind:?} has no sentence a client can act on: {:?}",
                entry.message
            );
            match entry.fault {
                Fault::Sender => assert!(
                    (400..500).contains(&entry.status),
                    "{kind:?} blames the sender with status {}",
                    entry.status
                ),
                Fault::Receiver => assert!(
                    entry.status >= 500,
                    "{kind:?} blames the receiver with status {}",
                    entry.status
                ),
            }
            // Uniqueness is per SERVICE and not across the whole catalog. Two
            // services share this listener, `EmptyBatchRequest` is a code in
            // BOTH of them, and the two are told apart by the namespace: the
            // `__type` prefix in JSON, the document's own `xmlns` in Query. A
            // catalog-wide uniqueness rule would be a rule against SNS spelling
            // its own errors ([`ErrorKind::SnsEmptyBatchRequest`]).
            json_types.push((kind.json_namespace(), entry.json));
            query_codes.push((kind.json_namespace(), entry.query));
        }
        assert_eq!(ErrorKind::ALL.len(), 36);
        let unique_json: std::collections::BTreeSet<_> = json_types.iter().collect();
        let unique_query: std::collections::BTreeSet<_> = query_codes.iter().collect();
        assert_eq!(unique_json.len(), json_types.len(), "duplicate JSON type");
        assert_eq!(
            unique_query.len(),
            query_codes.len(),
            "duplicate Query code"
        );
    }

    /// The five SNS batch codes are the SQS ones without SQS's service prefix,
    /// and that prefix is the whole point of their existing.
    #[test]
    fn snss_batch_codes_carry_no_sqs_service_prefix() {
        for (sqs, sns) in [
            (
                ErrorKind::EmptyBatchRequest,
                ErrorKind::SnsEmptyBatchRequest,
            ),
            (
                ErrorKind::TooManyEntriesInBatchRequest,
                ErrorKind::SnsTooManyEntriesInBatchRequest,
            ),
            (
                ErrorKind::BatchEntryIdsNotDistinct,
                ErrorKind::SnsBatchEntryIdsNotDistinct,
            ),
            (
                ErrorKind::InvalidBatchEntryId,
                ErrorKind::SnsInvalidBatchEntryId,
            ),
            (
                ErrorKind::BatchRequestTooLong,
                ErrorKind::SnsBatchRequestTooLong,
            ),
        ] {
            assert_eq!(sqs.json_type(), sns.json_type(), "{sqs:?}");
            assert_eq!(sns.query_code(), sns.json_type(), "{sns:?}");
            assert!(
                sqs.query_code().starts_with("AWS.SimpleQueueService."),
                "{sqs:?}"
            );
            assert_eq!(sqs.http_status(), sns.http_status());
            assert_eq!(sns.json_namespace(), JSON_NAMESPACE_SNS);
            assert_eq!(sqs.json_namespace(), JSON_NAMESPACE_SQS);
        }
    }

    /// The six where the two protocols disagree, pinned one by one. These are
    /// the strings a client branches on, and getting one wrong turns a
    /// permanent failure into an infinite retry loop.
    #[test]
    fn the_two_spellings_differ_exactly_where_aws_differs() {
        let pairs = [
            (
                ErrorKind::QueueDoesNotExist,
                "QueueDoesNotExist",
                "AWS.SimpleQueueService.NonExistentQueue",
            ),
            (
                ErrorKind::MessageNotInflight,
                "MessageNotInflight",
                "AWS.SimpleQueueService.MessageNotInflight",
            ),
            (
                ErrorKind::QueueDeletedRecently,
                "QueueDeletedRecently",
                "AWS.SimpleQueueService.QueueDeletedRecently",
            ),
            // ...and the inverted one.
            (
                ErrorKind::QueueAlreadyExists,
                "QueueNameExists",
                "QueueAlreadyExists",
            ),
        ];
        for (kind, json, query) in pairs {
            assert_eq!(kind.json_type(), json, "{kind:?}");
            assert_eq!(kind.query_code(), query, "{kind:?}");
            assert_ne!(kind.json_type(), kind.query_code(), "{kind:?}");
        }
        // The one that has NO prefix, though its neighbours in the batch family
        // all do: copying the pattern would be wrong here.
        assert_eq!(
            ErrorKind::ReceiptHandleIsInvalid.query_code(),
            "ReceiptHandleIsInvalid"
        );
        assert_eq!(
            ErrorKind::InvalidAttributeName.query_code(),
            "InvalidAttributeName"
        );
    }

    /// The two codes the 2023 message-move API brought in. They are AWS's, not
    /// this facade's: `UnsupportedOperation` keeps the `AWS.SimpleQueueService.`
    /// prefix its Query rendering has always had, and
    /// `ResourceNotFoundException` has none in either protocol because it was
    /// modelled after SQS moved to JSON — copying the prefix pattern would
    /// invent a string no client branches on.
    #[test]
    fn the_message_move_codes_are_aws_own_in_both_protocols() {
        assert_eq!(
            ErrorKind::ResourceNotFoundException.json_type(),
            "ResourceNotFoundException"
        );
        assert_eq!(
            ErrorKind::ResourceNotFoundException.query_code(),
            "ResourceNotFoundException"
        );
        // 404 and not 400: it is the one Sender fault in this catalog that
        // names a resource rather than a request.
        assert_eq!(ErrorKind::ResourceNotFoundException.http_status(), 404);
        assert_eq!(ErrorKind::ResourceNotFoundException.fault(), Fault::Sender);

        assert_eq!(
            ErrorKind::UnsupportedOperation.json_type(),
            "UnsupportedOperation"
        );
        assert_eq!(
            ErrorKind::UnsupportedOperation.query_code(),
            "AWS.SimpleQueueService.UnsupportedOperation"
        );
        assert_eq!(ErrorKind::UnsupportedOperation.http_status(), 400);
    }

    /// The SNS half. Three facts, each of which a client reads before it reads
    /// anything else: the STATUS of a missing topic (404, where SQS says 400),
    /// the SPELLING of a bad parameter (`InvalidParameter`, not
    /// `InvalidParameterValue`), and the NAMESPACE the JSON rendering puts them
    /// under.
    #[test]
    fn the_sns_half_is_not_the_sqs_half() {
        assert_eq!(ErrorKind::NotFound.http_status(), 404);
        assert_eq!(ErrorKind::QueueDoesNotExist.http_status(), 400);
        assert_eq!(ErrorKind::ResourceNotFound.http_status(), 404);
        assert_eq!(ErrorKind::TagLimitExceeded.http_status(), 400);
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::InvalidParameter,
            ErrorKind::ResourceNotFound,
            ErrorKind::TagLimitExceeded,
        ] {
            // Both protocols spell these the same: they carry no
            // `AWS.SimpleQueueService.` legacy prefix to mirror.
            assert_eq!(kind.json_type(), kind.query_code(), "{kind:?}");
            assert_eq!(kind.json_namespace(), JSON_NAMESPACE_SNS, "{kind:?}");
            assert_eq!(kind.fault(), Fault::Sender, "{kind:?}");
        }
        assert_eq!(ErrorKind::InvalidParameter.json_type(), "InvalidParameter");
        assert_ne!(
            ErrorKind::InvalidParameter.json_type(),
            ErrorKind::InvalidParameterValue.json_type()
        );
        // ...and every SQS code stays in SQS's namespace, including the two
        // that share this listener's error path with SNS.
        assert_eq!(
            ErrorKind::QueueDoesNotExist.json_namespace(),
            JSON_NAMESPACE_SQS
        );
        assert_eq!(
            ErrorKind::InternalFailure.json_namespace(),
            JSON_NAMESPACE_SQS
        );
    }

    #[test]
    fn purge_and_the_credential_errors_keep_their_own_statuses() {
        assert_eq!(ErrorKind::PurgeQueueInProgress.http_status(), 403);
        assert_eq!(ErrorKind::SignatureDoesNotMatch.http_status(), 403);
        assert_eq!(ErrorKind::MissingAuthenticationToken.http_status(), 403);
        assert_eq!(ErrorKind::InvalidClientTokenId.http_status(), 403);
        // ...against the ones that are plain 400s.
        assert_eq!(ErrorKind::IncompleteSignature.http_status(), 400);
        assert_eq!(ErrorKind::QueueDoesNotExist.http_status(), 400);
        // The only two the receiver owns.
        assert_eq!(ErrorKind::ServiceUnavailable.http_status(), 503);
        assert_eq!(ErrorKind::InternalFailure.http_status(), 500);
    }

    #[test]
    fn a_missing_queue_upstream_is_a_missing_queue_downstream() {
        let e = SqsError::from_queen(&queen::Error::status(404, "queue not found"));
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
        // The broker's own words never travel.
        assert!(!e.message.contains("queue not found"));
    }

    /// The mapping that decides whether a client backs off or gives up.
    #[test]
    fn a_rate_cap_is_a_throttle_with_the_backoff_queen_named() {
        let e = SqsError::from_queen(&queen::Error::Status {
            code: 429,
            body: "{\"error\":\"tenant frozen\"}".into(),
            retry_after_ms: Some(2500),
        });
        assert_eq!(e.kind, ErrorKind::RequestThrottled);
        assert_eq!(e.retry_after_ms, Some(2500));
        // Not OverLimit: no SDK's throttling list carries that string.
        assert_ne!(e.kind, ErrorKind::OverLimit);
        assert!(!e.message.contains("frozen"));
    }

    #[test]
    fn everything_else_lands_where_the_client_can_act_on_it() {
        let cases = [
            (
                queen::Error::Transport("dns failure for broker.internal".into()),
                ErrorKind::ServiceUnavailable,
            ),
            (
                queen::Error::status(503, "upstream"),
                ErrorKind::ServiceUnavailable,
            ),
            (
                queen::Error::status(403, "bad token"),
                ErrorKind::AccessDenied,
            ),
            (
                queen::Error::status(400, "partition 'x' is not a number"),
                ErrorKind::InvalidParameterValue,
            ),
            (
                queen::Error::Body("missing field `messages`".into()),
                ErrorKind::InternalFailure,
            ),
            (
                queen::Error::Precondition {
                    failed_index: 0,
                    reason: "version".into(),
                    version: 7,
                    value: serde_json::json!({"name": "orders"}),
                },
                ErrorKind::ServiceUnavailable,
            ),
        ];
        for (upstream, expected) in cases {
            let e = SqsError::from_queen(&upstream);
            assert_eq!(e.kind, expected, "{upstream}");
            assert!(!e.message.is_empty());
        }
        // The upstream STATUS is the one upstream detail that travels, on the
        // one branch that would otherwise say nothing.
        let e = SqsError::from_queen(&queen::Error::status(422, "unprocessable"));
        assert!(e.message.contains("422"), "{}", e.message);
        assert!(!e.message.contains("unprocessable"));
    }

    #[test]
    fn a_call_site_message_replaces_the_default_and_display_names_the_code() {
        let e = SqsError::with(
            ErrorKind::InvalidParameterValue,
            "MaxNumberOfMessages is 11",
        );
        assert_eq!(e.message, "MaxNumberOfMessages is 11");
        assert_eq!(
            e.to_string(),
            "InvalidParameterValue: MaxNumberOfMessages is 11"
        );
        assert_eq!(
            SqsError::new(ErrorKind::EmptyBatchRequest).message,
            ErrorKind::EmptyBatchRequest.default_message()
        );
    }
}
