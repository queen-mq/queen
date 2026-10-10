# Terraform provider for Queen MQ

Declares what lives inside a Queen broker, beside the infrastructure it runs on: the configuration
of its queues, and a cluster's S3 sink. It works with Terraform and OpenTofu, 1.11 or later for
the sink (its secret is a write-only argument).

```hcl
provider "queen" {
  endpoint    = "http://queen.queen.svc.cluster.local:6632"
  cp_endpoint = "http://queen-proxy.queen.svc.cluster.local:6711" # for queen_s3_sink
}

resource "queen_queue" "orders" {
  name              = "orders"
  lease_time        = 30
  retry_limit       = 5
  retention_enabled = true
  retention_seconds = 604800
}
```

`examples/main.tf` has both resources.

## The provider

| Argument | Environment | |
|---|---|---|
| `endpoint` | `QUEEN_ENDPOINT` | The broker, or the proxy in front of it. Required. |
| `token` | `QUEEN_TOKEN` | Sent as `Authorization: Bearer`: a proxy API key or a JWT. Not needed on a broker port with no authentication. |
| `cp_endpoint` | `QUEEN_CP_ENDPOINT` | The proxy, for `queen_s3_sink`. |
| `cp_token` | `QUEEN_CP_TOKEN` | The control-plane token (`QUEEN_PROXY_CP_TOKEN` on the broker). |

Tokens given through the environment are in no file and in no state.

## queen_queue

The configuration of one queue. The first push that names a queue creates it; this resource
creates it ahead of that, or takes over one that exists.

- An option you leave out stays as the broker has it: the broker merges, so taking over a queue
  changes only what you name. Its labels (`namespace`, `task`) stay too.
- The options are `lease_time`, `retry_limit`, `dead_letter_queue`, `dlq_after_max_retries`,
  `dedup_window_seconds`, `delayed_processing`, `window_buffer`, `retention_enabled`,
  `retention_seconds`, `completed_retention_seconds`, `max_wait_time_seconds` and
  `encryption_enabled`, as [queue options](https://queenmq.com/concepts/partitions/#queue-options)
  describes them.
- **Destroying the resource leaves the queue and its messages**, and only forgets it, unless
  `delete_on_destroy = true`. Deleting a queue deletes every message in it.
- Import by name: `terraform import queen_queue.orders orders`.

## queen_s3_sink

The S3 sink of one cluster, set through the proxy's control plane. The broker has to run with
`QUEEN_S3_EMBEDDED=true` and an encryption key.

- `cluster`, `queues`, `endpoint`, `region`, `bucket` and `access_key` are required. The other
  settings (`prefix`, `path_style`, `sse`, `sse_kms_key_id`, `format`, `compression`,
  `parquet_codec`, `layout`, `align`, `start`, `target_mb`, `max_window_ms`, `sink`) are the
  per-sink ones of the [S3 guide](https://queenmq.com/guides/s3/#reference), and one you leave out
  takes the broker's default.
- `secret_key` is write-only. It is sent to the broker, which seals it with its own encryption
  key, and it is never written to the plan or the state. It is sent when the sink is created, and
  again whenever `secret_key_version` changes; otherwise the broker keeps the one it has.
- `enabled = false` stops the sink and keeps its settings and its secret.
- Import by the cluster's slug: `terraform import queen_s3_sink.lake acme`.

## Not here yet

Tenants, API keys and the PostgreSQL connectors. The control plane creates and deletes tenants and
keys but has no route that reads one back, which a provider needs to see drift.

## Develop

```bash
GOWORK=off go build -o terraform-provider-queen .
TF_ACC=1 QUEEN_ENDPOINT=http://localhost:6632 GOWORK=off go test ./internal/provider/
```

The tests run real plans and applies against a live broker. The S3 sink's also need
`QUEEN_CP_ENDPOINT`, `QUEEN_CP_TOKEN` and a cluster (`QUEEN_TEST_CLUSTER`, default `acme`), and are
skipped without them. `TF_ACC_TERRAFORM_PATH=$(command -v tofu)
TF_ACC_PROVIDER_HOST=registry.opentofu.org` runs them with OpenTofu.

To try a build without a registry, point the CLI at it:

```hcl
# ~/.terraformrc, or the file TF_CLI_CONFIG_FILE names
provider_installation {
  dev_overrides {
    "queen-mq/queen" = "/path/to/the/directory/of/the/binary"
  }
  direct {}
}
```

The Terraform and OpenTofu registries take a provider only from a repository of its own named
`terraform-provider-queen`, with signed releases. This directory is written to move there as it
is: the module path already is `github.com/queen-mq/terraform-provider-queen`.
