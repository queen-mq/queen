#!/bin/sh
# The connector workloads' services on the controller, in Docker, bound to its
# private address (the nodes reach it as `ctl`, --services-host):
#   PostgreSQL 17, wal_level=logical: database jepsen, user jepsen, password jepsen, :5432
#   RustFS 1.0.4 (S3, tested by the sink's own suite): keys jepsen / jepsen-secret-key,
#   bucket jepsen, :9000
# Both start empty; each connector test resets what it uses (jepsen.queen.services).
set -e
IP=${CTL_IP:-$(hostname -I | awk '{print $1}')}
docker rm -f jepsen-pg jepsen-s3 >/dev/null 2>&1 || true
docker run -d --name jepsen-pg --restart unless-stopped -p "$IP:5432:5432" \
  -e POSTGRES_USER=jepsen -e POSTGRES_PASSWORD=jepsen -e POSTGRES_DB=jepsen \
  postgres:17 -c wal_level=logical -c max_replication_slots=32 -c max_wal_senders=32 >/dev/null
docker run -d --name jepsen-s3 --restart unless-stopped -p "$IP:9000:9000" \
  -e RUSTFS_ACCESS_KEY=jepsen -e RUSTFS_SECRET_KEY=jepsen-secret-key \
  rustfs/rustfs:1.0.4 >/dev/null
for i in $(seq 1 60); do
  docker exec jepsen-pg pg_isready -U jepsen >/dev/null 2>&1 && curl -sf "http://$IP:9000/health" >/dev/null && break
  sleep 1
done
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 aws:amz:us-east-1:s3 \
  --user jepsen:jepsen-secret-key "http://$IP:9000/jepsen")
echo "CreateBucket jepsen: $code (200 made, 409 there already)"
docker exec jepsen-pg psql -U jepsen -tAc "SHOW wal_level"
echo "services ready on $IP: PostgreSQL :5432, S3 :9000. Run tests with --services-host $IP"
