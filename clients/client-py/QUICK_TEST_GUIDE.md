# Quick Test Guide

## TL;DR

```bash
cd clients/client-py
pip install -e ".[dev]"
./run_tests.sh
```

## Requirements

- ✅ A fresh Queen server running on `http://localhost:6632` (the tests expect an empty broker):
  ```bash
  docker run -d --name queen -p 6632:6632 -e QUEEN_RAFT_DIR=/var/lib/queen/raft \
    -v queen-data:/var/lib/queen/raft ghcr.io/queen-mq/queen:latest
  ```
- ✅ Python 3.8+
- ✅ Dependencies installed

## Run Tests

### One Command

```bash
# Install and run
pip install -e ".[dev]" && pytest tests/
```

### Options

```bash
# All tests (recommended)
pytest tests/

# With output
pytest tests/ -vs

# Quick smoke tests
pytest tests/test_queue.py tests/test_push.py

# Specific test
pytest tests/test_push.py::test_push_message

# Using shell script
./run_tests.sh
```

## Expected Result

```
==================== 57 passed in ~45s ====================
```

## If Tests Fail

1. **Check server:** `curl http://localhost:6632/health`
2. **Check logs:** `QUEEN_CLIENT_LOG=true pytest tests/ -vs`
3. **Run one test:** `pytest tests/test_push.py::test_push_message -vvs`
4. **Rerunning?** Most tests use fixed queue names and nothing cleans up after them, so start from a fresh broker: `docker rm -f queen && docker volume rm queen-data`, then start it again.

## Test Categories

- `test_queue.py` - Queue ops (3 tests, ~5s)
- `test_push.py` - Push ops (13 tests, ~15s)
- `test_pop.py` - Pop ops (5 tests, ~10s)
- `test_consume.py` - Consume ops (15 tests, ~60s)
- `test_transaction.py` - Transactions (13 tests, ~20s)
- `test_subscription.py` - Subscriptions (6 tests, ~90s)
- `test_dlq.py` - DLQ (1 test, ~5s)
- `test_complete.py` - Workflows (1 test, ~5s)

## Success

If all tests pass: **Python client is working correctly!** ✅

