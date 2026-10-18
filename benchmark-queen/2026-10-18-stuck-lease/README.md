# Leases left active by pops that timed out (Queen 2.4.0, local Docker, 2026-10-18)

Found by the OMB driver's smoke run (`omb-smoke.log`): the three-node cluster of the driver's
`deploy/docker/compose.yaml` (ghcr.io/queen-mq/queen:2.4.0) on Alice's Mac, `simple-workload.yaml` cut to
1 minute (1 topic, 10 partitions, 10,000 msg/s), the driver's `queen.yaml` settings (`pollers: 8`) plus
`verify: true`.

- About 80 s in (06:36:54Z and 06:37:01Z) the leader logged failed raft heartbeats to both followers
  (`queen-omb-queen-1-1.log`), and the driver got 8 pop errors, one logged as
  `HTTP 503: {"error":"the request deadline elapsed","code":"timeout"}`.
- From then on the consumer took about 7,000 of 10,000 msg/s: partitions 3, 4 and 5 were no longer
  delivered. 279 s later each still showed `leaseActive: true` under its own worker id, about 48,000
  messages behind (`group-partitions.json`; the queue's lease is 300 s).
- Everything that was delivered was exact (verify: 0 duplicates, 0 holes). The driver acked every pop it
  received (pops 67,324 = 67,284 acked + 32 empty + 8 errors).

**The queue's lease is 300 s.** The driver configures each queue with `dedupWindowSeconds` alone, so it
takes `configure`'s defaults (`lease_time: 300`; a queue a push creates gets 60), and its pops send no
`leaseSeconds`. The driver renews only the leases it received.

**Reading (Alice's, which the evidence fits better than mine):** the designed fallback, not a bug. The 503s
came while the leader could not reach either follower (heartbeat network errors), and two pops in three go
to a follower, which forwards them to the leader. The leader made the claims and answered; the answers were
lost on the broken link, so no node knew nobody received them, and a lost answer costs one lease: 300 s.
`concepts/consuming.mdx`'s hand-back ("its client left or it ran past its deadline") covers what the node
that holds the claim can see. Not checked: which node answered each 503.

**What it means for the driver:** a lost answer stalls a partition for the queue's lease. A shorter lease
(`leaseSeconds`, or `queueOptions.leaseTime`) would bound that, and `renewLeases` keeps live batches alive.
