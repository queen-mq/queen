//! Client tokens on the Rust client: a [`TokenSource`] refreshed after a 401
//! (the request sent once more), a fixed credential never retried on one, and
//! `tokens().mint()` on the wire. Against [`support::FakeBroker`]: no stack.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use queen_mq::{Config, Error, MintTokenRequest, Queen, TokenSource};
use support::{FakeBroker, Reply};

fn expired() -> Reply {
    Reply::json(
        401,
        r#"{"error":"client token expired","code":"token_expired"}"#,
    )
}

/// A source answering `token-0`, `token-1`, ... and counting its calls.
fn counting_source() -> (TokenSource, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let source = TokenSource::new(move || {
        let n = c.fetch_add(1, Ordering::SeqCst);
        async move { Ok(format!("token-{n}")) }
    });
    (source, calls)
}

async fn pop(queen: &Queen) -> queen_mq::Result<usize> {
    queen
        .queue("commands")
        .partition("dev1")
        .wait(false)
        .pop()
        .await
        .map(|m| m.len())
}

#[tokio::test]
async fn a_source_fetches_a_new_token_after_a_401_and_sends_the_request_again() {
    let broker = FakeBroker::start(vec![expired(), Reply::ok(r#"{"messages":[]}"#)]).await;
    let (source, calls) = counting_source();
    let queen = Queen::connect(Config::new(broker.url()).token_source(source)).unwrap();

    assert_eq!(pop(&queen).await.unwrap(), 0);
    let hits = broker.hits();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].header("authorization"), Some("Bearer token-0"));
    assert_eq!(
        hits[1].header("authorization"),
        Some("Bearer token-1"),
        "the retry carries the new token"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // The new token is kept: the next request fetches nothing.
    pop(&queen).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_second_401_is_the_callers() {
    let broker = FakeBroker::start(vec![expired(), expired(), expired()]).await;
    let (source, _) = counting_source();
    let queen = Queen::connect(Config::new(broker.url()).token_source(source)).unwrap();
    let e = pop(&queen).await.unwrap_err();
    assert_eq!(e.status(), Some(401), "{e}");
    assert_eq!(broker.hit_count(), 2, "one refresh, one retry");
}

#[tokio::test]
async fn a_fixed_credential_is_not_retried_on_a_401() {
    let broker = FakeBroker::start(vec![expired()]).await;
    let queen = Queen::connect(Config::new(broker.url()).bearer_token("qk_live_static")).unwrap();
    assert_eq!(pop(&queen).await.unwrap_err().status(), Some(401));
    let hits = broker.hits();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].header("authorization"), Some("Bearer qk_live_static"));
}

#[tokio::test]
async fn a_source_that_fails_is_a_token_error_and_sends_nothing() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;
    let source =
        TokenSource::new(|| async { Err::<String, queen_mq::TokenError>("backend down".into()) });
    let queen = Queen::connect(Config::new(broker.url()).token_source(source)).unwrap();
    let e = pop(&queen).await.unwrap_err();
    assert!(matches!(&e, Error::Token(m) if m.contains("backend down")), "{e}");
    assert_eq!(broker.hit_count(), 0);
}

#[tokio::test]
async fn a_fixed_credential_and_a_source_together_are_refused() {
    let (source, _) = counting_source();
    let e = Queen::connect(
        Config::new("http://127.0.0.1:1")
            .bearer_token("qk_live_x")
            .token_source(source),
    )
    .err()
    .expect("a config error");
    assert!(matches!(e, Error::Config(_)), "{e}");
}

#[tokio::test]
async fn a_mint_posts_the_entity_and_reads_the_answer() {
    let broker = FakeBroker::start(vec![Reply::json(
        201,
        r#"{"token":"qt_a.b.c","sub":"dev-8f3a","key":"k1","expiresAt":"2026-11-11T12:00:00+00:00","expiresIn":600}"#,
    )])
    .await;
    let queen = Queen::connect(Config::new(broker.url()).bearer_token("qk_live_role")).unwrap();
    let minted = queen
        .tokens()
        .mint(&MintTokenRequest::new("dev-8f3a").ttl_seconds(600))
        .await
        .unwrap();
    assert_eq!(minted.token, "qt_a.b.c");
    assert_eq!(minted.expires_in, 600);
    let hit = &broker.hits()[0];
    assert_eq!((hit.method.as_str(), hit.route()), ("POST", "/api/v1/tokens"));
    assert_eq!(
        hit.json(),
        serde_json::json!({"sub": "dev-8f3a", "ttlSeconds": 600})
    );
    assert_eq!(hit.header("authorization"), Some("Bearer qk_live_role"));
    assert!(matches!(
        queen.tokens().mint(&MintTokenRequest::new("")).await,
        Err(Error::Invalid(_))
    ));
}
