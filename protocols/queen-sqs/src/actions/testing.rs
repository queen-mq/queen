//! The rig every action test is written against: a facade over the broker
//! double, with its queues created through the REAL registry.
//!
//! It is a module and not a copy in each test file because a second one would
//! drift: the queues these tests read are the ones the queue actions write, and
//! a rig that stubbed a registry record would be testing a record no action
//! ever stores. `#[cfg(test)]` throughout — nothing here is compiled into the
//! binary.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::actions::messages::{delete_message, receive_message, send_message};
use crate::actions::{Ctx, Principal};
use crate::config::{
    AuthMode, Config, ReceiveMode, DEFAULT_ACCOUNT, DEFAULT_LISTEN, DEFAULT_REGION,
};
use crate::credentials::Directory;
use crate::error::SqsResult;
use crate::queen::testing::FakeQueen;
use crate::queen::QueenApi;
use crate::registry::{Naming, ATTR_PARTITIONS};
use crate::Facade;

pub const HOST: &str = "sqs.queen.test:9324";
/// Narrow enough that a spread test can prove the hash uses all of it, wide
/// enough that two sends colliding is not the norm.
pub const LANES: u32 = 8;

pub fn config() -> Config {
    Config {
        listen: DEFAULT_LISTEN.to_string(),
        auth: AuthMode::Off,
        credentials: Directory::empty(),
        region: DEFAULT_REGION.to_string(),
        account: DEFAULT_ACCOUNT.to_string(),
        receive_mode: ReceiveMode::Exact,
        default_partitions: LANES,
        handle_secret: b"a conformance secret".to_vec(),
        handle_secret_generated: false,
        queen_url: "http://localhost:6789".to_string(),
        queen_token: None,
        embedded: false,
        shutdown_grace_ms: 5_000,
        tls: None,
    }
}

/// A facade over a broker double, with `queues` created through the REAL
/// registry: a message action reads the record the queue action writes, and
/// a test that stubbed the record would not be testing that.
pub struct Rig {
    pub ctx: Ctx,
    pub fake: Arc<FakeQueen>,
}

impl Rig {
    pub async fn new(queues: &[(&str, &[(&str, &str)])]) -> Rig {
        let fake = FakeQueen::empty();
        let facade = Arc::new(Facade::new(
            config(),
            Arc::clone(&fake) as Arc<dyn QueenApi>,
        ));
        let naming = Naming::new(DEFAULT_REGION, DEFAULT_ACCOUNT);
        for (name, attributes) in queues {
            // The broker half: `deadLetterQueue` off is what every
            // SQS-created queue sets, and a 30 second lease matches the
            // default visibility a receive falls back to.
            fake.add_queue(
                name,
                json!({"deadLetterQueue": false, "retryLimit": 3, "leaseTime": 30}),
            );
            let mut bag: BTreeMap<String, String> = attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            if crate::registry::is_fifo(name) {
                bag.insert("FifoQueue".to_string(), "true".to_string());
            } else {
                bag.entry(ATTR_PARTITIONS.to_string())
                    .or_insert_with(|| LANES.to_string());
            }
            facade
                .registry
                .create(name, &bag, &BTreeMap::new(), &naming, LANES, None)
                .await
                .expect("the queue is created");
        }
        let ctx = Ctx {
            facade,
            principal: Principal::default(),
            host: HOST.to_string(),
            request_id: "00000000-0000-4000-8000-00000000cafe".to_string(),
        };
        Rig { ctx, fake }
    }

    pub async fn standard() -> Rig {
        Rig::new(&[("orders", &[])]).await
    }

    /// A SECOND facade over the same broker: another replica behind the same
    /// load balancer, with its own registry cache and its own state, which is to
    /// say none. It is how a test proves the sentence the whole design protects
    /// — any instance answers any request — instead of assuming it. The handle
    /// secret is [`config`]'s, shared, because two instances that minted
    /// receipt handles the other refuses would fail that test for the wrong
    /// reason (and an operator who forgets `QUEEN_SQS_HANDLE_SECRET` gets
    /// exactly that, loudly, which is [`crate::handle`]'s own decision).
    pub fn sibling(&self) -> Rig {
        self.sibling_with(|_| {})
    }

    /// [`Rig::sibling`] with one knob turned. It exists for the settings an
    /// operator cannot set — `Config::from_source` refuses
    /// `QUEEN_SQS_RECEIVE_MODE=amortized` at boot — where a `Config` built in
    /// process is the only way the facade can be in that state and the
    /// behaviour there still has to be pinned rather than assumed.
    pub fn sibling_with(&self, tweak: impl FnOnce(&mut Config)) -> Rig {
        let mut config = config();
        tweak(&mut config);
        let facade = Arc::new(Facade::new(
            config,
            Arc::clone(&self.fake) as Arc<dyn QueenApi>,
        ));
        Rig {
            ctx: Ctx {
                facade,
                principal: Principal::default(),
                host: HOST.to_string(),
                request_id: "00000000-0000-4000-8000-00000000beef".to_string(),
            },
            fake: Arc::clone(&self.fake),
        }
    }

    pub fn url(&self, queue: &str) -> String {
        format!("http://{HOST}/{DEFAULT_ACCOUNT}/{queue}")
    }

    /// `{"QueueUrl": …}` plus whatever the case adds.
    pub fn params(&self, queue: &str, extra: Value) -> Value {
        let mut params = json!({"QueueUrl": self.url(queue)});
        if let Some(fields) = extra.as_object() {
            for (name, value) in fields {
                params[name] = value.clone();
            }
        }
        params
    }

    pub async fn send(&self, queue: &str, extra: Value) -> SqsResult<Value> {
        send_message(&self.ctx, &self.params(queue, extra)).await
    }

    pub async fn receive(&self, queue: &str, extra: Value) -> SqsResult<Value> {
        receive_message(&self.ctx, &self.params(queue, extra)).await
    }

    /// The messages of one receive, as a list (absent is empty).
    pub async fn receive_list(&self, queue: &str, extra: Value) -> Vec<Value> {
        let answer = self
            .receive(queue, extra)
            .await
            .expect("the receive answers");
        answer
            .get("Messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn receive_one(&self, queue: &str) -> Value {
        let mut messages = self.receive_list(queue, json!({})).await;
        assert_eq!(messages.len(), 1, "expected exactly one message");
        messages.remove(0)
    }

    /// One message on each of `lanes` lanes, written as this facade writes
    /// them.
    ///
    /// A test that wants N messages BACK cannot get there by sending N: a
    /// send hashes a fresh uuid across the width, two of them can land on
    /// one lane, and a claimed lane holds its second message until the
    /// first is gone (`two_messages_on_one_lane_are_delivered_one_at_a_time`)
    /// — so a send-based fixture would fail on the runs where the uuids
    /// collided and pass on the others.
    pub fn seed_lanes(&self, queue: &str, lanes: usize) {
        for lane in 0..lanes {
            self.fake.seed(
                queue,
                &lane.to_string(),
                0,
                &[json!({"b": format!("m{lane}")})],
            );
        }
    }

    pub async fn delete(&self, queue: &str, handle: &str) -> SqsResult<Value> {
        delete_message(
            &self.ctx,
            &self.params(queue, json!({"ReceiptHandle": handle})),
        )
        .await
    }

    /// Receive everything the queue will give and abandon it, `times` over, so
    /// that the NEXT delivery is attempt `times + 1`.
    ///
    /// Abandoning is letting the lease lapse rather than releasing it, because
    /// that is what a consumer that crashed does and it is the only path that
    /// charges `attempt_count` — a `ChangeMessageVisibility(0)` is a `retry`
    /// ack, which releases the claim and charges nothing (M1).
    pub async fn burn(&self, queue: &str, times: usize) {
        for _ in 0..times {
            self.receive_list(queue, json!({"MaxNumberOfMessages": 10}))
                .await;
            // Past the default visibility a receive falls back to, which is the
            // 30 seconds the rig's queues are configured with.
            self.fake.advance(Duration::from_secs(31));
        }
    }
}

/// The ARN this deployment mints for a queue name.
pub fn arn(queue: &str) -> String {
    Naming::new(DEFAULT_REGION, DEFAULT_ACCOUNT).arn(queue)
}

/// A `RedrivePolicy` document naming `dead_letter`, as a client would write
/// one.
pub fn redrive_policy(dead_letter: &str, max_receive_count: i64) -> String {
    crate::actions::dlq::policy_document(
        &Naming::new(DEFAULT_REGION, DEFAULT_ACCOUNT),
        dead_letter,
        max_receive_count,
    )
}

/// A stored payload that already carries a dead-letter receive count, which is
/// how a test reaches the over-threshold state without spending a delivery per
/// count. It is the shape the forward move itself writes
/// ([`crate::actions::dlq`]).
pub fn carrying(body: &str, received: i64) -> Value {
    json!({"b": body, "s": {"queen.receiveCount": received.to_string()}})
}

pub fn field<'a>(value: &'a Value, name: &str) -> &'a str {
    value
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{name} is missing from {value}"))
}

pub fn attribute(message: &Value, name: &str) -> Option<String> {
    message
        .get("Attributes")?
        .get(name)?
        .as_str()
        .map(str::to_string)
}
