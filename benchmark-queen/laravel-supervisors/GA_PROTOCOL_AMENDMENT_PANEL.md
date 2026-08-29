# GA protocol amendment — supervisor dashboard

Recorded on August 29, 2026 at 05:44:11 UTC, base commit
`245165e0df4012f6fbc11a0d827511056087d57b`, after the product owner's explicit
request and before dashboard implementation or testing.

This amendment adds a feature-parity requirement; it does not change the
workloads, endpoints, or statistical rules of the frozen performance protocol.

## Initial state

The Queen broker dashboard shows queues, consumer groups, messages, analytics,
and the DLQ. The PHP/Rust Laravel supervisors instead publish state and control
only in their `state_directory`, which is accessible through
`queen:supervisor`. No panel equivalent to Horizon's control surface exists
yet.

## Release-candidate gate

The Laravel client must provide an application panel, separate from the broker
dashboard, with at least:

- live/stale status, engine, instance ID, PID, and last heartbeat;
- active and draining workers per supervisor/queue;
- restart state/failure and next retry per pool;
- non-sensitive resolved configuration and configured queue depths;
- a summary of and access to the Laravel/Queen DLQ failed-job lifecycle;
- `pause`, `continue`, and `terminate` actions through authenticated POST
  requests;
- periodic refresh and explicit representation of unavailable data.

It must be possible to disable the panel and configure its prefix and
middleware. In production, access is denied by default until the application
registers explicit authorization. Mutating actions require the same authorization,
CSRF protection, and `instance_id` fencing already used by CLI control.
Credentials, bearer tokens, headers, and private paths are not returned.

## Declared scope and limitations

The first version is Laravel-native and reads the local supervisor state. This
matches the currently supported topology, which requires one master per
application/consumer group. It is not presented as a multi-host view.

A future centralized view requires shared heartbeats and fenced leadership; a
simple KV key with a TTL is not enough to make two supervisors active-active.
The existing broker dashboard remains the panel for queue metrics and
operations, while the new panel covers the Laravel control plane.

## Minimum tests

- routes are absent when the panel is disabled;
- production denies access by default and explicitly authorized access works;
- data is escaped, secrets are not exposed, and a Content Security Policy is
  present;
- live, paused, stale, draining, and multiple-pool states;
- control POSTs contain the correct command and `instance_id`;
- unauthorized commands, missing CSRF, and replaced supervisors are rejected;
- available/unavailable failed-job providers and an unreachable broker;
- rendering and API contracts work without CDN or remote asset dependencies.
