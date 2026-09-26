# Getting Started with Queen Python Client Tests

Quick guide to running the Queen Python client test suite.

## Prerequisites

### 1. Start Queen Server

```bash
docker run -d --name queen -p 6632:6632 \
  -e QUEEN_RAFT_DIR=/var/lib/queen/raft \
  -v queen-data:/var/lib/queen/raft \
  ghcr.io/queen-mq/queen:latest
```

The tests expect an empty broker and nothing cleans up after them, so start a
fresh one for every run (see [Starting Over](#starting-over)).

### 2. Install Dependencies

```bash
cd clients/client-py

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"
```

### 3. Verify Setup

```bash
# Check Queen server
curl http://localhost:6632/health

# Check Queen client
python -c "from queen import Queen; print('✅ Ready!')"
```

## Running Tests

### Easiest Way - Shell Script

```bash
# Run all tests
./run_tests.sh

# Run with pytest
./run_tests.sh pytest

# Quick smoke tests
./run_tests.sh quick
```

### Using Pytest Directly

```bash
# All tests
pytest tests/

# Specific test file
pytest tests/test_push.py

# Specific test
pytest tests/test_push.py::test_push_message

# With verbose output
pytest tests/ -v

# Show print statements
pytest tests/ -s

# Both
pytest tests/ -vs
```

### Using Test Runner (Matches Node.js Pattern)

```bash
# All tests
python -m tests.run_tests

# Human tests only
python -m tests.run_tests human

# Specific test by name
python -m tests.run_tests test_push_message
```

## Test Categories

### Quick Tests (< 5 seconds)
```bash
pytest tests/test_queue.py tests/test_push.py -v
```

### Medium Tests (5-30 seconds)
```bash
pytest tests/test_pop.py tests/test_transaction.py -v
```

### Slow Tests (> 30 seconds)
```bash
pytest tests/test_consume.py tests/test_subscription.py -v
```

## Expected Output

```bash
$ pytest tests/test_queue.py -v
===================== test session starts ======================
platform darwin -- Python 3.11.5, pytest-7.4.0
collected 3 items

tests/test_queue.py::test_create_queue PASSED          [ 33%]
tests/test_queue.py::test_delete_queue PASSED          [ 66%]
tests/test_queue.py::test_configure_queue PASSED       [100%]

===================== 3 passed in 2.34s ========================
```

## Troubleshooting

### Can't Connect to Server

```bash
# Check server is running
docker ps | grep queen

# Check logs
docker logs <container-id>

# Try manual connection
curl http://localhost:6632/health
```

### Import Errors

```bash
# Reinstall client
pip install -e .

# Check installation
pip list | grep queen-mq

# Verify imports
python -c "from queen import Queen"
```

### Tests Hang

Some tests intentionally sleep to test delays, buffering, or lease expiry:
- `test_push_buffered_message` - Waits 2s for buffer flush
- `test_push_delayed_message` - Waits 2.5s for delay
- `test_pop_with_ack_reconsume` - Waits 2s for lease expiry
- `test_auto_renew_lease` - Waits 5s for long processing
- `test_subscription_*` - Waits 10s for subscription timing

These are expected and match the Node.js test behavior.

## Debugging Failed Tests

### Run with Maximum Verbosity

```bash
# Show everything
QUEEN_CLIENT_LOG=true pytest tests/test_name.py::test_function -vvs

# This will show:
# - Test execution details
# - Client logs (HTTP requests, etc.)
# - Print statements
# - Error tracebacks
```

### Check Test Data

There is no database to query: look at what a run left behind through the
broker's HTTP API.

```bash
# List queues
curl http://localhost:6632/api/v1/resources/queues
```

### Starting Over

Most tests use fixed queue names and nothing cleans up after them, so a second
run against the same broker sees the first run's data. Start a fresh broker:

```bash
docker rm -f queen && docker volume rm queen-data
# then start it again (step 1 above)
```

## Next Steps

After tests pass:

1. **Run full suite** to verify everything works
2. **Check coverage** (optional): `pytest tests/ --cov=queen`
3. **Run benchmarks** (coming soon)
4. **Deploy to production** with confidence!

## Tips

### Faster Test Runs

```bash
# Run tests in parallel
pytest tests/ -n auto  # Requires pytest-xdist

# Skip slow tests
pytest tests/ -m "not slow"
```

### Continuous Testing

```bash
# Watch mode (requires pytest-watch)
ptw tests/ -- -v
```

### Test Specific Functionality

```bash
# Test only transactions
pytest tests/ -k transaction

# Test only subscriptions
pytest tests/ -k subscription

# Test only push operations
pytest tests/ -k push
```

## Success Criteria

All tests should pass:
- ✅ Queue operations work
- ✅ Push/pop work correctly
- ✅ Consumers process messages
- ✅ Transactions are atomic
- ✅ Subscriptions work correctly
- ✅ DLQ captures failures
- ✅ Complete workflows succeed

If all tests pass, your Python client is working correctly and has 100% parity with the Node.js client! 🎉

## Need Help?

- Check [Test README](README.md) for detailed documentation
- Check [Main README](../README.md) for client documentation
- Check [Node.js tests](../../client-js/test-v2/) for reference
- Open an issue on GitHub

