-- Runs once on first DB init (docker-entrypoint-initdb.d).
-- Queue creation/reset happens per-run in run.sh so partition/group counts and
-- a clean slate are controllable between runs.
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;
