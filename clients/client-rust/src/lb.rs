//! Backend selection across a set of broker replicas.
//!
//! Brokers are stateless and interchangeable, so any of them can serve any
//! request — but *consumers* should not bounce between them. Two replicas
//! polling the same partition for the same group contend for the same claim,
//! which costs a round-trip and an aborted transaction every time. Hence
//! [`Strategy::Affinity`]: a consistent-hash ring keyed by the poll's grouping
//! key pins each (queue, partition, group) to one replica for as long as that
//! replica is healthy.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How a request picks its backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Strategy {
    /// Even spread, no stickiness. Fine for producers, wasteful for consumers.
    RoundRobin,
    /// One backend per client instance, chosen once.
    Session,
    /// Consistent hashing on the request's affinity key. The default, and the
    /// only one that keeps a consumer group off its own toes.
    #[default]
    Affinity,
}

#[derive(Debug)]
struct Health {
    healthy: bool,
    failures: u32,
    last_failure: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct VNode {
    hash: u32,
    server: usize,
}

#[derive(Debug)]
struct State {
    health: Vec<Health>,
    ring: Vec<VNode>,
    next: usize,
    session: Option<usize>,
}

/// Picks a backend per request and tracks which ones are answering.
#[derive(Debug)]
pub struct LoadBalancer {
    urls: Vec<String>,
    strategy: Strategy,
    replicas: usize,
    retry_after: Duration,
    state: Mutex<State>,
}

impl LoadBalancer {
    /// `replicas` is the number of virtual nodes each backend gets on the ring.
    /// More replicas means a more even spread and a smaller reshuffle when a
    /// backend drops out, at a linear memory cost.
    pub fn new(
        urls: Vec<String>,
        strategy: Strategy,
        replicas: usize,
        retry_after: Duration,
    ) -> Self {
        let urls: Vec<String> = urls
            .into_iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
        let health = urls
            .iter()
            .map(|_| Health {
                healthy: true,
                failures: 0,
                last_failure: None,
            })
            .collect();
        let lb = Self {
            replicas: replicas.max(1),
            urls,
            strategy,
            retry_after,
            state: Mutex::new(State {
                health,
                ring: Vec::new(),
                next: 0,
                session: None,
            }),
        };
        let all: Vec<usize> = (0..lb.urls.len()).collect();
        let ring = lb.build_ring(&all);
        lb.state.lock().unwrap().ring = ring;
        lb
    }

    fn build_ring(&self, servers: &[usize]) -> Vec<VNode> {
        let mut ring = Vec::with_capacity(servers.len() * self.replicas);
        for &s in servers {
            for i in 0..self.replicas {
                ring.push(VNode {
                    hash: fnv1a(&format!("{}#vnode{}", self.urls[s], i)),
                    server: s,
                });
            }
        }
        ring.sort_unstable_by_key(|v| v.hash);
        ring
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    pub fn strategy(&self) -> Strategy {
        self.strategy
    }

    /// Pick a backend for a request with the given affinity key.
    ///
    /// Returns the index into [`LoadBalancer::urls`]. Never fails: if every
    /// backend is currently marked unhealthy it returns one anyway, because
    /// refusing to send is strictly worse than sending to a backend that might
    /// have recovered.
    pub fn pick(&self, affinity_key: Option<&str>) -> usize {
        let mut st = self.state.lock().unwrap();

        // A backend that failed long enough ago goes back in the pool. Without
        // this a transient blip would take a replica out permanently.
        let now = Instant::now();
        let mut readmitted = false;
        for h in st.health.iter_mut() {
            if !h.healthy {
                if let Some(t) = h.last_failure {
                    if now.duration_since(t) >= self.retry_after {
                        h.healthy = true;
                        readmitted = true;
                    }
                }
            }
        }

        let live: Vec<usize> = (0..self.urls.len())
            .filter(|&i| st.health[i].healthy)
            .collect();

        if live.is_empty() {
            let i = st.next % self.urls.len().max(1);
            st.next = st.next.wrapping_add(1);
            return i;
        }

        if readmitted {
            st.ring = self.build_ring(&live);
        }

        match self.strategy {
            Strategy::Affinity => match affinity_key {
                Some(key) if !st.ring.is_empty() => {
                    let target = fnv1a(key);
                    // First vnode clockwise from the key's position.
                    let start = st
                        .ring
                        .partition_point(|v| v.hash < target)
                        .min(st.ring.len().saturating_sub(1));
                    for off in 0..st.ring.len() {
                        let v = st.ring[(start + off) % st.ring.len()];
                        if st.health[v.server].healthy {
                            return v.server;
                        }
                    }
                    live[0]
                }
                // No key to hash on (a plain admin call, say): nothing to be
                // sticky about, so spread instead of always hitting live[0].
                _ => {
                    let i = live[st.next % live.len()];
                    st.next = st.next.wrapping_add(1);
                    i
                }
            },
            Strategy::Session => {
                if let Some(s) = st.session {
                    if st.health[s].healthy {
                        return s;
                    }
                }
                let i = live[st.next % live.len()];
                st.next = st.next.wrapping_add(1);
                st.session = Some(i);
                i
            }
            Strategy::RoundRobin => {
                let i = live[st.next % live.len()];
                st.next = st.next.wrapping_add(1);
                i
            }
        }
    }

    /// Record that a backend answered. Clears any failure history and, if it
    /// had been out, puts its virtual nodes back on the ring.
    pub fn mark_healthy(&self, idx: usize) {
        let mut st = self.state.lock().unwrap();
        let Some(h) = st.health.get_mut(idx) else {
            return;
        };
        let was_down = !h.healthy;
        h.healthy = true;
        h.failures = 0;
        h.last_failure = None;
        if was_down {
            let live: Vec<usize> = (0..self.urls.len())
                .filter(|&i| st.health[i].healthy)
                .collect();
            st.ring = self.build_ring(&live);
        }
    }

    /// Record that a backend failed in a way that suggests it is down (5xx or a
    /// transport fault — never a 4xx, which is the caller's fault, and never a
    /// 429, which means the backend is alive and saying so).
    pub fn mark_unhealthy(&self, idx: usize) {
        let mut st = self.state.lock().unwrap();
        let Some(h) = st.health.get_mut(idx) else {
            return;
        };
        h.healthy = false;
        h.failures += 1;
        h.last_failure = Some(Instant::now());
        let live: Vec<usize> = (0..self.urls.len())
            .filter(|&i| st.health[i].healthy)
            .collect();
        st.ring = self.build_ring(&live);
    }

    #[cfg(test)]
    fn healthy_count(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .health
            .iter()
            .filter(|h| h.healthy)
            .count()
    }
}

/// FNV-1a, 32-bit. Chosen to match the JS/Go/Python clients byte for byte: the
/// ring only does its job if every SDK maps a given key to the same backend, so
/// this hash is part of the wire contract even though it never travels.
fn fnv1a(s: &str) -> u32 {
    let mut hash: u32 = 2_166_136_261;
    for &b in s.as_bytes() {
        hash ^= b as u32;
        // The JS client writes this as the shift-and-add form of the FNV prime
        // to stay in 32-bit integer range; the multiply below is the same
        // thing, wrapped.
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lb(n: usize, strategy: Strategy) -> LoadBalancer {
        let urls = (0..n).map(|i| format!("http://h{i}:6789")).collect();
        LoadBalancer::new(urls, strategy, 128, Duration::from_millis(50))
    }

    #[test]
    fn trailing_slashes_are_stripped() {
        let lb = LoadBalancer::new(
            vec!["http://a:1/".into(), "http://b:2".into()],
            Strategy::RoundRobin,
            8,
            Duration::from_secs(5),
        );
        assert_eq!(lb.urls(), ["http://a:1", "http://b:2"]);
    }

    #[test]
    fn round_robin_cycles() {
        let lb = lb(3, Strategy::RoundRobin);
        let picks: Vec<usize> = (0..6).map(|_| lb.pick(None)).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn session_sticks_to_one_backend() {
        let lb = lb(3, Strategy::Session);
        let first = lb.pick(None);
        for _ in 0..20 {
            assert_eq!(lb.pick(None), first);
        }
    }

    #[test]
    fn affinity_is_stable_for_a_key() {
        let lb = lb(4, Strategy::Affinity);
        let key = "orders:eu:workers";
        let first = lb.pick(Some(key));
        for _ in 0..50 {
            assert_eq!(lb.pick(Some(key)), first);
        }
    }

    #[test]
    fn affinity_spreads_distinct_keys() {
        let lb = lb(4, Strategy::Affinity);
        let mut seen = std::collections::HashSet::new();
        for i in 0..200 {
            seen.insert(lb.pick(Some(&format!("queue{i}:*:g"))));
        }
        assert!(
            seen.len() > 1,
            "every key landed on one backend — the ring is not spreading"
        );
    }

    #[test]
    fn affinity_without_a_key_spreads_instead_of_pinning() {
        // A keyless call has nothing to be sticky about; always returning the
        // first live backend would quietly serialize every admin call onto one.
        let lb = lb(3, Strategy::Affinity);
        let picks: std::collections::HashSet<usize> = (0..9).map(|_| lb.pick(None)).collect();
        assert!(
            picks.len() > 1,
            "keyless affinity picks pinned to one backend"
        );
    }

    #[test]
    fn an_unhealthy_backend_is_skipped_then_readmitted() {
        let lb = lb(3, Strategy::RoundRobin);
        lb.mark_unhealthy(1);
        assert_eq!(lb.healthy_count(), 2);
        for _ in 0..10 {
            assert_ne!(lb.pick(None), 1);
        }

        // retry_after is 50ms in these tests.
        std::thread::sleep(Duration::from_millis(60));
        let picks: std::collections::HashSet<usize> = (0..9).map(|_| lb.pick(None)).collect();
        assert!(
            picks.contains(&1),
            "backend was never given a second chance"
        );
    }

    #[test]
    fn affinity_fails_over_and_returns_when_the_backend_recovers() {
        let lb = lb(4, Strategy::Affinity);
        let key = "orders:eu:workers";
        let home = lb.pick(Some(key));

        lb.mark_unhealthy(home);
        let failover = lb.pick(Some(key));
        assert_ne!(failover, home);
        // and it is stable while home is down
        assert_eq!(lb.pick(Some(key)), failover);

        lb.mark_healthy(home);
        assert_eq!(lb.pick(Some(key)), home, "key did not return to its home");
    }

    #[test]
    fn all_backends_down_still_returns_something() {
        let lb = lb(2, Strategy::Affinity);
        lb.mark_unhealthy(0);
        lb.mark_unhealthy(1);
        assert_eq!(lb.healthy_count(), 0);
        // Refusing to send would be worse than trying a backend that may have
        // come back since.
        let i = lb.pick(Some("k"));
        assert!(i < 2);
    }

    #[test]
    fn single_backend_is_always_chosen() {
        let lb = lb(1, Strategy::Affinity);
        for _ in 0..10 {
            assert_eq!(lb.pick(Some("anything")), 0);
        }
    }

    #[test]
    fn fnv1a_matches_the_reference_vectors() {
        // The canonical FNV-1a 32-bit test vectors. If these drift, this
        // client's ring stops agreeing with the JS/Go/Python ones.
        assert_eq!(fnv1a(""), 2_166_136_261);
        assert_eq!(fnv1a("a"), 0xe40c_292c);
        assert_eq!(fnv1a("foobar"), 0xbf9c_f968);
    }
}
