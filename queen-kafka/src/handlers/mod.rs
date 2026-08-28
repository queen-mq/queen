//! One module per Kafka API. A handler owns the semantics of its API and
//! nothing else: it takes the decoded request and returns the response body
//! together with the version that body must be ENCODED at (normally the request
//! version — see `api_versions` for the one case where it is not). The framing,
//! the headers and the dispatch live in `conn`.

pub mod api_versions;
pub mod metadata;
