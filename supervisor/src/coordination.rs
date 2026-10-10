//! Lets replicas of one autoscaling pool, on several hosts or pods, find each
//! other through the broker's key/value store, so each one runs a share of the
//! fleet target instead of all of it (see `desired_share` in main.rs).
//!
//! ```text
//! coordination/v1/<scope>/<instance_id>   {instance_id, hostname}, TTL
//! ```
//!
//! The scope names the work, not the deployment: FNV-1a 64 of the broker
//! endpoints, the consumer group and the sorted queue names. Replicas
//! coordinate only when all three are identical; a pool on another broker or
//! queue set, or a fixed pool, keeps its own sizing. The Laravel package's `ReplicaCoordinator` writes the same keys, so
//! replicas of both engines coordinate with each other.
//!
//! Every poll renews this instance's key and lists the scope in the same call.
//! The key expires after the TTL, the control-loop bound (at most the heartbeat
//! timeout): a live supervisor renews it on every iteration, while a crashed
//! one drops out within it.
//! A supervisor that pauses or stops deletes its keys at once.
//!
//! Coordination is best effort. When the broker cannot be reached, the last
//! view is used until it is as old as the TTL, and then the supervisor sizes
//! its pools alone: more workers than needed, never fewer. A failure is
//! reported once per failure streak.

use crate::remote_status::post_kv;
use crate::{validate_connection, validate_identifier, QueenConfig, MAX_CONTROL_TTL_SECONDS};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(crate) const PREFIX: &str = "coordination/v1/";
/// Replicas listed per pool. The broker counts a getPrefix limit against
/// QUEEN_KV_MAX_KEYS_PER_CALL (1024 by default), so POOLS_PER_CALL pools fit
/// in one call.
pub(crate) const MEMBER_LIMIT: usize = 100;
pub(crate) const POOLS_PER_CALL: usize = 4;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoordinationConfig {
    pub(crate) connection: QueenConfig,
    pub(crate) namespace: String,
    pub(crate) ttl: u64,
}

impl CoordinationConfig {
    /// A live replica renews its key once per control-loop iteration, so the
    /// TTL must outlast the loop bound.
    pub(crate) fn validate(&self, loop_budget: u64) -> Result<(), Box<dyn std::error::Error>> {
        validate_connection("coordination", &self.connection)?;
        validate_identifier(&self.namespace, "coordination namespace")?;
        if self.ttl <= loop_budget || self.ttl > MAX_CONTROL_TTL_SECONDS {
            return Err(format!(
                "coordination ttl must exceed the control-loop budget [{loop_budget}] and be at most {MAX_CONTROL_TTL_SECONDS} seconds"
            )
            .into());
        }
        Ok(())
    }

    /// Every control-loop iteration renews and lists the replicas of each
    /// scope: one call per POOLS_PER_CALL scopes, one attempt per endpoint.
    pub(crate) fn heartbeat_budget(
        &self,
        scopes: usize,
        http_timeout: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        u64::try_from(scopes.div_ceil(POOLS_PER_CALL))?
            .checked_mul(u64::try_from(
                crate::connection_endpoints(&self.connection).len(),
            )?)
            .and_then(|value| value.checked_mul(http_timeout))
            .ok_or_else(|| "coordination heartbeat budget overflowed".into())
    }
}

/// The scope of a pool: FNV-1a 64 of the sorted endpoint URLs separated by
/// spaces, then the consumer group and the sorted queue names, one per line.
pub(crate) fn scope(endpoints: &[&str], consumer_group: &str, queues: &[String]) -> String {
    let mut endpoints = endpoints.to_vec();
    endpoints.sort_unstable();
    let mut sorted: Vec<&str> = queues.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    feed(endpoints.join(" ").as_bytes());
    feed(b"\n");
    feed(consumer_group.as_bytes());
    for queue in sorted {
        feed(b"\n");
        feed(queue.as_bytes());
    }
    format!("{hash:016x}")
}

pub(crate) struct Coordinator<'a> {
    config: &'a CoordinationConfig,
    instance_id: String,
    hostname: Option<String>,
    views: HashMap<String, (Vec<String>, Instant)>,
    failing: bool,
}

impl<'a> Coordinator<'a> {
    pub(crate) fn new(
        config: &'a CoordinationConfig,
        instance_id: &str,
        hostname: Option<&str>,
    ) -> Self {
        Self {
            config,
            instance_id: instance_id.to_owned(),
            hostname: hostname.map(str::to_owned),
            views: HashMap::new(),
            failing: false,
        }
    }

    /// Renew this instance in every scope and read the replicas of each.
    /// Nothing is sent once a stop is asked for, and a stop abandons a
    /// request in flight: see unless_stopped.
    pub(crate) fn heartbeat(
        &mut self,
        client: &reqwest::blocking::Client,
        scopes: &[String],
        running: &AtomicBool,
    ) {
        if !running.load(Ordering::SeqCst) {
            return;
        }
        let config = self.config;
        self.heartbeat_at(Instant::now(), scopes, |body, operations| {
            let (client, connection, body) =
                (client.clone(), config.connection.clone(), body.to_vec());
            crate::unless_stopped(running, move || {
                post_kv(&client, &connection, &body, |response| {
                    results(response, operations).map(|_| ())
                })
                .map_err(|error| error.to_string())
            })
            .unwrap_or_else(|| Err("abandoned: the supervisor is stopping".to_owned()))
            .map_err(Into::into)
        });
    }

    fn heartbeat_at<F>(&mut self, now: Instant, scopes: &[String], mut send: F)
    where
        F: FnMut(&[u8], usize) -> Result<serde_json::Value, Box<dyn std::error::Error>>,
    {
        let scopes: Vec<&String> = scopes.iter().collect::<BTreeSet<_>>().into_iter().collect();
        let result = scopes.chunks(POOLS_PER_CALL).try_for_each(|chunk| {
            let mut operations = Vec::with_capacity(chunk.len() * 2);
            for scope in chunk {
                operations.push(serde_json::json!({
                    "op": "put",
                    "ns": &self.config.namespace,
                    "key": self.member_key(scope),
                    "value": {"instance_id": &self.instance_id, "hostname": &self.hostname},
                    "ttlSeconds": self.config.ttl,
                }));
                operations.push(serde_json::json!({
                    "op": "getPrefix",
                    "ns": &self.config.namespace,
                    "prefix": scope_prefix(scope),
                    "limit": MEMBER_LIMIT,
                    "keysOnly": true,
                }));
            }
            let body = serde_json::to_vec(&serde_json::json!({ "operations": operations }))?;
            let response = send(&body, operations.len())?;
            let results = results(&response, operations.len())?;
            for (index, scope) in chunk.iter().enumerate() {
                if results[2 * index].get("applied") != Some(&serde_json::Value::Bool(true)) {
                    return Err("the broker did not renew this replica".into());
                }
                let members = self.members(scope, &results[2 * index + 1])?;
                self.views.insert((*scope).clone(), (members, now));
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        });
        match result {
            Ok(()) if self.failing => {
                self.failing = false;
                eprintln!("Queen supervisor replica coordination recovered.");
            }
            Ok(()) => {}
            Err(error) if !self.failing => {
                self.failing = true;
                eprintln!(
                    "Queen supervisor replica coordination failed: {error}. Pools keep the last known replicas for up to {} seconds, then size themselves alone.",
                    self.config.ttl
                );
            }
            Err(_) => {}
        }
    }

    /// Leave every scope at once, so the other replicas take over this share
    /// without waiting for the TTL. Best effort: a key left behind expires.
    /// With `running`, a stop abandons the request, as it does a heartbeat's;
    /// the stop leaves on its own once every worker has its SIGTERM.
    pub(crate) fn leave(
        &mut self,
        client: &reqwest::blocking::Client,
        scopes: &[String],
        running: Option<&AtomicBool>,
    ) {
        for scope in scopes {
            self.views.remove(scope);
        }
        let Some(body) = self.leave_body(scopes) else {
            return;
        };
        match running {
            None => {
                let _ = post_kv(client, &self.config.connection, &body, |_| Ok(()));
            }
            Some(running) => {
                let (client, connection) = (client.clone(), self.config.connection.clone());
                let _ = crate::unless_stopped(running, move || {
                    post_kv(&client, &connection, &body, |_| Ok(())).is_ok()
                });
            }
        }
    }

    fn leave_body(&self, scopes: &[String]) -> Option<Vec<u8>> {
        let scopes: BTreeSet<&String> = scopes.iter().collect();
        if scopes.is_empty() {
            return None;
        }
        let operations: Vec<_> = scopes
            .into_iter()
            .map(|scope| {
                serde_json::json!({
                    "op": "delete",
                    "ns": &self.config.namespace,
                    "key": self.member_key(scope),
                })
            })
            .collect();
        serde_json::to_vec(&serde_json::json!({ "operations": operations })).ok()
    }

    /// This instance's position among the live replicas of a scope, as
    /// `desired_share` takes it; alone when no current view exists.
    pub(crate) fn position(&self, scope: &str) -> (usize, usize) {
        self.position_at(Instant::now(), scope)
    }

    /// Whether a current view of the scope exists; without one, `position`
    /// answers as if this replica were alone.
    pub(crate) fn has_view(&self, scope: &str) -> bool {
        self.has_view_at(Instant::now(), scope)
    }

    fn has_view_at(&self, now: Instant, scope: &str) -> bool {
        self.views.get(scope).is_some_and(|(_, at)| {
            now.saturating_duration_since(*at) <= Duration::from_secs(self.config.ttl)
        })
    }

    fn position_at(&self, now: Instant, scope: &str) -> (usize, usize) {
        match self.views.get(scope) {
            Some((members, at))
                if now.saturating_duration_since(*at) <= Duration::from_secs(self.config.ttl) =>
            {
                let replica = members
                    .iter()
                    .position(|member| member == &self.instance_id)
                    .unwrap_or(0);
                (replica, members.len())
            }
            _ => (0, 1),
        }
    }

    /// Sorted instance ids of a getPrefix answer, this instance included even
    /// when the listing ran before its own renewal.
    fn members(
        &self,
        scope: &str,
        result: &serde_json::Value,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let rows = result
            .get("rows")
            .and_then(serde_json::Value::as_array)
            .ok_or("the broker returned a malformed replica listing")?;
        let prefix = scope_prefix(scope);
        let mut members = BTreeSet::from([self.instance_id.clone()]);
        for row in rows {
            let instance = row
                .get("key")
                .and_then(serde_json::Value::as_str)
                .and_then(|key| key.strip_prefix(prefix.as_str()));
            if let Some(instance) = instance.filter(|instance| is_instance_id(instance)) {
                members.insert(instance.to_owned());
            }
        }
        Ok(members.into_iter().collect())
    }

    fn member_key(&self, scope: &str) -> String {
        format!("{}{}", scope_prefix(scope), self.instance_id)
    }
}

fn scope_prefix(scope: &str) -> String {
    format!("{PREFIX}{scope}/")
}

fn is_instance_id(value: &str) -> bool {
    (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// A batch answers `{results: [...]}` index-aligned to its operations.
fn results(
    response: &serde_json::Value,
    operations: usize,
) -> Result<&Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    response
        .get("results")
        .and_then(serde_json::Value::as_array)
        .filter(|results| results.len() == operations)
        .ok_or_else(|| "the broker did not answer every coordination operation".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SELF: &str = "000000000000000018da146e6dc7d0d900000002";
    const OTHER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn config() -> CoordinationConfig {
        CoordinationConfig {
            connection: QueenConfig {
                url: "http://127.0.0.1:6632".into(),
                urls: vec!["http://127.0.0.1:6632".into()],
                bearer_token: None,
                headers: HashMap::new(),
            },
            namespace: "queen-supervisor".into(),
            ttl: 30,
        }
    }

    fn answer(scopes: &[&str], members: &[&[&str]]) -> serde_json::Value {
        let mut results = Vec::new();
        for (scope, members) in scopes.iter().zip(members) {
            results.push(serde_json::json!({"applied": true}));
            let rows: Vec<_> = members
                .iter()
                .map(|id| serde_json::json!({"key": format!("coordination/v1/{scope}/{id}")}))
                .collect();
            results.push(serde_json::json!({"rows": rows, "truncated": false}));
        }
        serde_json::json!({"results": results})
    }

    #[test]
    fn the_scope_matches_the_laravel_package() {
        let broker = ["http://queen.test:6632"];
        let queues = vec!["high".to_owned(), "default".to_owned()];
        assert_eq!(scope(&broker, "laravel", &queues), "60e032fdac129da0");
        let reversed = vec!["default".to_owned(), "high".to_owned()];
        assert_eq!(scope(&broker, "laravel", &reversed), "60e032fdac129da0");
        assert_ne!(scope(&broker, "emails", &queues), "60e032fdac129da0");
        assert_ne!(
            scope(&["http://other.test:6632"], "laravel", &queues),
            "60e032fdac129da0"
        );
    }

    #[test]
    fn a_heartbeat_renews_and_lists_each_scope_in_one_call() {
        let config = config();
        let mut coordinator = Coordinator::new(&config, SELF, Some("pod-a"));
        let scope = scope(&["http://queen.test:6632"], "laravel", &["high".to_owned()]);
        let mut bodies = Vec::new();

        coordinator.heartbeat_at(
            Instant::now(),
            &[scope.clone(), scope.clone()],
            |body, _| {
                bodies.push(serde_json::from_slice::<serde_json::Value>(body).unwrap());
                Ok(answer(&[&scope], &[&[OTHER, SELF]]))
            },
        );

        assert_eq!(bodies.len(), 1);
        assert_eq!(
            bodies[0]["operations"],
            serde_json::json!([
                {
                    "op": "put",
                    "ns": "queen-supervisor",
                    "key": format!("coordination/v1/{scope}/{SELF}"),
                    "value": {"instance_id": SELF, "hostname": "pod-a"},
                    "ttlSeconds": 30,
                },
                {
                    "op": "getPrefix",
                    "ns": "queen-supervisor",
                    "prefix": format!("coordination/v1/{scope}/"),
                    "limit": MEMBER_LIMIT,
                    "keysOnly": true,
                },
            ])
        );
        // Sorted: the Rust id (leading zeros) comes first.
        assert_eq!(coordinator.position(&scope), (0, 2));
    }

    #[test]
    fn this_replica_counts_before_its_key_is_listed_and_foreign_keys_do_not() {
        let config = config();
        let mut coordinator = Coordinator::new(&config, OTHER, None);
        let scope = scope(&["http://queen.test:6632"], "laravel", &["high".to_owned()]);
        let prefix = format!("coordination/v1/{scope}/");

        coordinator.heartbeat_at(Instant::now(), std::slice::from_ref(&scope), |_, _| {
            Ok(serde_json::json!({"results": [
                {"applied": true},
                {"rows": [
                    {"key": format!("{prefix}{SELF}")},
                    {"key": format!("{prefix}not-an-instance")},
                    {"key": format!("{prefix}{SELF}/nested")},
                    {"value": "no key"},
                ]},
            ]}))
        });

        assert_eq!(coordinator.position(&scope), (1, 2));
    }

    #[test]
    fn many_pools_are_spread_over_calls_the_broker_accepts() {
        let config = config();
        let mut coordinator = Coordinator::new(&config, SELF, None);
        let scopes: Vec<String> = (1..=5)
            .map(|index| {
                scope(
                    &["http://queen.test:6632"],
                    "laravel",
                    &[format!("queue-{index}")],
                )
            })
            .collect();
        let mut calls = Vec::new();

        coordinator.heartbeat_at(Instant::now(), &scopes, |body, operations| {
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let listed: Vec<String> = body["operations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|operation| operation["op"] == "getPrefix")
                .map(|operation| {
                    operation["prefix"].as_str().unwrap()["coordination/v1/".len()..]
                        .trim_end_matches('/')
                        .to_owned()
                })
                .collect();
            calls.push(operations);
            let scopes: Vec<&str> = listed.iter().map(String::as_str).collect();
            let members: Vec<&[&str]> = listed.iter().map(|_| &[OTHER][..]).collect();
            Ok(answer(&scopes, &members))
        });

        assert_eq!(calls, vec![8, 2]);
        assert!(scopes
            .iter()
            .all(|scope| coordinator.position(scope) == (0, 2)));
    }

    #[test]
    fn an_outage_keeps_the_last_view_until_the_ttl_then_sizes_alone() {
        let config = config();
        let mut coordinator = Coordinator::new(&config, OTHER, None);
        let scope = scope(&["http://queen.test:6632"], "laravel", &["high".to_owned()]);
        let start = Instant::now();
        coordinator.heartbeat_at(start, std::slice::from_ref(&scope), |_, _| {
            Ok(answer(&[&scope], &[&[SELF, OTHER]]))
        });

        coordinator.heartbeat_at(
            start + Duration::from_secs(10),
            std::slice::from_ref(&scope),
            |_, _| Err("broker down".into()),
        );
        assert!(coordinator.failing);
        assert_eq!(
            coordinator.position_at(start + Duration::from_secs(30), &scope),
            (1, 2)
        );
        assert!(coordinator.has_view_at(start + Duration::from_secs(30), &scope));
        assert_eq!(
            coordinator.position_at(start + Duration::from_secs(31), &scope),
            (0, 1)
        );
        // Without a view an event-driven step waits for the next heartbeat.
        assert!(!coordinator.has_view_at(start + Duration::from_secs(31), &scope));

        coordinator.heartbeat_at(
            start + Duration::from_secs(32),
            std::slice::from_ref(&scope),
            |_, _| Ok(answer(&[&scope], &[&[OTHER]])),
        );
        assert!(!coordinator.failing);
    }

    #[test]
    fn a_malformed_answer_is_a_failure() {
        let config = config();
        let mut coordinator = Coordinator::new(&config, SELF, None);
        let scope = scope(&["http://queen.test:6632"], "laravel", &["high".to_owned()]);

        coordinator.heartbeat_at(Instant::now(), std::slice::from_ref(&scope), |_, _| {
            Ok(serde_json::json!({"ok": false, "reason": "kv_precondition"}))
        });
        assert!(coordinator.failing);

        let mut coordinator = Coordinator::new(&config, SELF, None);
        coordinator.heartbeat_at(Instant::now(), std::slice::from_ref(&scope), |_, _| {
            Ok(serde_json::json!({"results": [{"applied": false}, {"rows": []}]}))
        });
        assert!(coordinator.failing);
        assert_eq!(coordinator.position(&scope), (0, 1));
    }

    #[test]
    fn leaving_deletes_this_replica_in_every_scope_and_forgets_the_views() {
        let config = config();
        let mut coordinator = Coordinator::new(&config, SELF, None);
        let scope = scope(&["http://queen.test:6632"], "laravel", &["high".to_owned()]);
        coordinator.heartbeat_at(Instant::now(), std::slice::from_ref(&scope), |_, _| {
            Ok(answer(&[&scope], &[&[OTHER]]))
        });

        let body: serde_json::Value = serde_json::from_slice(
            &coordinator
                .leave_body(&[scope.clone(), scope.clone()])
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            body["operations"],
            serde_json::json!([{
                "op": "delete",
                "ns": "queen-supervisor",
                "key": format!("coordination/v1/{scope}/{SELF}"),
            }])
        );
        assert!(coordinator.leave_body(&[]).is_none());
    }

    #[test]
    fn validation_mirrors_the_laravel_resolver() {
        assert!(config().validate(29).is_ok());
        assert!(config().validate(30).is_err());
        let mut blank = config();
        blank.namespace = String::new();
        assert!(blank.validate(30).is_err());
        let mut too_long = config();
        too_long.ttl = MAX_CONTROL_TTL_SECONDS + 1;
        assert!(too_long.validate(30).is_err());
        assert_eq!(config().heartbeat_budget(5, 5).unwrap(), 10);
        assert_eq!(config().heartbeat_budget(0, 5).unwrap(), 0);
    }
}
