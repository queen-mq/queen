terraform {
  required_providers {
    queen = {
      source = "queen-mq/queen"
    }
  }
}

# The broker for queues; the proxy's control plane for the S3 sink. The
# control-plane token is read from QUEEN_CP_TOKEN, so it is in no file here.
provider "queen" {
  endpoint    = "http://queen.queen.svc.cluster.local:6632"
  cp_endpoint = "http://queen-proxy.queen.svc.cluster.local:6711"
}

resource "queen_queue" "orders" {
  name              = "orders"
  lease_time        = 30
  retry_limit       = 5
  retention_enabled = true
  retention_seconds = 604800
}

# Handed in for one run and never stored: `secret_key` is write-only.
variable "s3_secret_key" {
  type      = string
  sensitive = true
  ephemeral = true
}

resource "queen_s3_sink" "lake" {
  cluster            = "acme"
  queues             = queen_queue.orders.name
  endpoint           = "https://storage.googleapis.com"
  region             = "auto"
  bucket             = "acme-queen-lake"
  access_key         = "GOOG1EXAMPLE"
  secret_key         = var.s3_secret_key
  secret_key_version = 1
  format             = "parquet"
}
