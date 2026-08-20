#!/bin/bash
# Queen VM setup for the 1.0.4 tenant-density rerun.
# Mirrors the 2026-07-24 rig: PG18 on-box, broker in docker, memory-safe boot
# profile (shared_buffers 16G, dedup cache 4G).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "=== apt base ==="
apt-get update -qq
apt-get install -y -qq curl ca-certificates gnupg lsb-release jq python3 sysstat >/dev/null

echo "=== PostgreSQL 18 ==="
install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  > /etc/apt/sources.list.d/pgdg.list
apt-get update -qq
apt-get install -y -qq postgresql-18 postgresql-client-18 >/dev/null

echo "=== docker ==="
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update -qq
apt-get install -y -qq docker-ce docker-ce-cli containerd.io >/dev/null

echo "=== PG tuning (July rig profile) ==="
PGCONF=/etc/postgresql/18/main/postgresql.conf
cat >> "$PGCONF" <<'EOF'

# --- tenant-density benchmark profile (2026-08-20, mirrors 2026-07-24 rig) ---
listen_addresses = '*'
max_connections = 500
shared_buffers = 16GB
effective_cache_size = 40GB
work_mem = 32MB
maintenance_work_mem = 2GB
max_wal_size = 16GB
min_wal_size = 2GB
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
random_page_cost = 1.1
effective_io_concurrency = 200
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.max = 10000
pg_stat_statements.track = top
track_io_timing = on
log_min_duration_statement = -1
EOF
echo "host all all 10.114.0.0/20 trust" >> /etc/postgresql/18/main/pg_hba.conf
echo "host all all 127.0.0.1/32 trust" >> /etc/postgresql/18/main/pg_hba.conf

systemctl restart postgresql@18-main
sleep 3
sudo -u postgres psql -qc "ALTER USER postgres PASSWORD 'postgres';" >/dev/null
sudo -u postgres psql -qc "CREATE DATABASE queen;" >/dev/null
sudo -u postgres psql -d queen -qc "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null

echo "=== fsync class (THE comparability spec: July rig was ~95us) ==="
sudo -u postgres /usr/lib/postgresql/18/bin/pg_test_fsync -f /var/lib/postgresql/18/main/fsynctest -s 5 2>&1 \
  | grep -A3 "fdatasync" | head -8
rm -f /var/lib/postgresql/18/main/fsynctest

echo "=== queen 1.0.4 image ==="
docker pull ghcr.io/queen-mq/queen:1.0.4 >/dev/null
docker image inspect ghcr.io/queen-mq/queen:1.0.4 --format '{{.Id}} {{.Architecture}}'

echo "SETUP-QUEEN-OK"
