# Queen Python Client Tests

Comprehensive test suite for the Queen Python client with 100% parity to the Node.js tests.

## Prerequisites

1. **Queen server running** on `http://localhost:6632`
2. **PostgreSQL accessible** (default: localhost:5432)
3. **Python dependencies installed**

```bash
# Install with dev dependencies
pip install -e ".[dev]"
```

## Running Tests

### Run All Tests

```bash
# Using pytest
pytest tests/

# Using the test runner (matches Node.js structure)
python -m tests.run_tests
```

### Run Specific Test Categories

```bash
# Queue tests
pytest tests/test_queue.py

# Push tests
pytest tests/test_push.py

# Pop tests
pytest tests/test_pop.py

# Consume tests
pytest tests/test_consume.py

# Transaction tests
pytest tests/test_transaction.py

# Subscription tests
pytest tests/test_subscription.py

# DLQ tests
pytest tests/test_dlq.py

# Complete workflow tests
pytest tests/test_complete.py
```

### Run Single Test

```bash
# Using pytest
pytest tests/test_push.py::test_push_message

# Using test runner
python -m tests.run_tests test_push_message
```

### Run with Verbose Output

```bash
# Pytest verbose
pytest tests/ -v

# Show print statements
pytest tests/ -s

# Both
pytest tests/ -vs
```

## Test Configuration

### Environment Variables

Configure database connection:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=postgres
export PG_USER=postgres
export PG_PASSWORD=postgres

# Run tests
pytest tests/
```

### Enable Client Logging

```bash
export QUEEN_CLIENT_LOG=true
pytest tests/ -s
```

## Test Structure

### Test Categories

1. **test_queue.py** - Queue operations (create, delete, configure)
2. **test_push.py** - Push operations (basic, buffered, delayed, encrypted, etc.)
3. **test_pop.py** - Pop operations (empty, non-empty, with wait, with ack)
4. **test_consume.py** - Consume operations (basic, batch, ordering, concurrency, retries)
5. **test_transaction.py** - Transaction operations (push+ack, multiple ops, atomicity)
6. **test_subscription.py** - Subscription modes (new, all, from-timestamp)
7. **test_dlq.py** - Dead Letter Queue functionality
8. **test_complete.py** - End-to-end workflows

### Test Count

**Total: 140+ tests** across three layers:

**Core broker tests (~66):**
- Queue management (`test_queue.py`, 3 tests)
- Push operations (`test_push.py`, 13 tests)
- Pop operations (`test_pop.py`, 5 tests)
- Consume operations (`test_consume.py`, 17 tests)
- Transactions (`test_transaction.py`, 14 tests)
- Subscription modes (`test_subscription.py`, 6 tests)
- JWT authentication (`test_auth.py`, 6 tests)
- Dead Letter Queue (`test_dlq.py`, 1 test)
- Complete workflows (`test_complete.py`, 1 test)

**Streaming SDK unit tests (`tests/streams_unit/`, ~46):**
- Operator behaviour, config-hash determinism, cycle protocol,
  event-time / watermarks, end-to-end fake-server harness.

**Streaming SDK integration tests (`tests/streams_integration/`, ~29):**
- Tumbling / sliding / session / cron windows, operators,
  event-time, recovery, throughput, combined pipelines.

The integration tests require a running Queen server + PostgreSQL;
unit tests run with an in-process fake server.

## Fixtures

Tests use pytest fixtures defined in `conftest.py`:

- `client` - Fresh Queen client instance per test
- `db_pool` - PostgreSQL connection pool for verification
- `cleanup_test_data` - Automatic cleanup before/after tests

## Test Patterns

### Basic Test

```python
@pytest.mark.asyncio
async def test_example(client):
    """Test description"""
    # Create queue
    queue = await client.queue("test-queue-name").create()
    assert queue.get("configured")
    
    # Push message
    await client.queue("test-queue-name").push([
        {"data": {"value": 1}}
    ])
    
    # Pop and verify
    messages = await client.queue("test-queue-name").pop()
    assert len(messages) == 1
    assert messages[0]["data"]["value"] == 1
```

### Test with Consumer

```python
@pytest.mark.asyncio
async def test_consumer_example(client):
    """Test consumer"""
    queue = await client.queue("test-queue").create()
    
    await client.queue("test-queue").push([{"data": {"id": 1}}])
    
    result = None
    
    async def handler(msg):
        nonlocal result
        result = msg["data"]["id"]
    
    await client.queue("test-queue").limit(1).consume(handler)
    
    assert result == 1
```

### Test with Transaction

```python
@pytest.mark.asyncio
async def test_transaction_example(client):
    """Test transaction"""
    # Setup queues
    await client.queue("input").create()
    await client.queue("output").create()
    
    # Push to input
    await client.queue("input").push([{"data": {"value": 10}}])
    
    # Pop from input
    messages = await client.queue("input").pop()
    
    # Transaction: ack input, push to output
    await (client.transaction()
        .ack(messages[0])
        .queue("output")
        .push([{"data": {"value": messages[0]["data"]["value"] * 2}}])
        .commit())
    
    # Verify
    result = await client.queue("output").pop()
    assert result[0]["data"]["value"] == 20
```

## Parity with Node.js Tests

The Python tests are direct ports of the Node.js tests from `client-js/test-v2/`:

| Node.js Test | Python Test | Status |
|--------------|-------------|--------|
| queue.js | test_queue.py | ✅ Ported |
| push.js | test_push.py | ✅ Ported |
| pop.js | test_pop.py | ✅ Ported |
| consume.js | test_consume.py | ✅ Ported |
| transaction.js | test_transaction.py | ✅ Ported |
| subscription.js | test_subscription.py | ✅ Ported |
| dlq.js | test_dlq.py | ✅ Ported |
| complete.js | test_complete.py | ✅ Ported |

## Expected Results

All tests should pass when run against a healthy Queen server:

```bash
$ pytest tests/
==================== test session starts ====================
collected 50+ items

tests/test_queue.py ...                            [ 10%]
tests/test_push.py .............                   [ 40%]
tests/test_pop.py .....                            [ 50%]
tests/test_consume.py ...............              [ 75%]
tests/test_transaction.py .............            [ 90%]
tests/test_subscription.py ......                  [ 95%]
tests/test_dlq.py .                                [ 97%]
tests/test_complete.py .                           [100%]

==================== 50+ passed in XXs ====================
```

## Troubleshooting

### Tests Fail to Connect

```bash
# Check Queen server is running
curl http://localhost:6632/health

# Check PostgreSQL is accessible
psql -h localhost -U postgres -d postgres -c "SELECT 1"
```

### Database Permission Errors

```bash
# Make sure test user has permissions
psql -h localhost -U postgres -d postgres
> GRANT ALL ON SCHEMA queen TO postgres;
> GRANT ALL ON ALL TABLES IN SCHEMA queen TO postgres;
```

### Tests Hang

Some tests use sleep to wait for conditions (lease expiry, buffering). This is expected behavior matching the Node.js tests.

### Cleanup Failed

```bash
# Manual cleanup
psql -h localhost -U postgres -d postgres
> DELETE FROM queen.queues WHERE name LIKE 'test-%';
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
        ports:
          - 5432:5432
      
      queen:
        image: smartnessai/queen-mq:latest
        env:
          PG_HOST: postgres
          PG_PASSWORD: postgres
        ports:
          - 6632:6632
    
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          cd clients/client-py
          pip install -e ".[dev]"
      
      - name: Run tests
        run: |
          cd clients/client-py
          pytest tests/ -v
```

## Adding New Tests

1. Create test function in appropriate file:

```python
@pytest.mark.asyncio
async def test_new_feature(client):
    """Test new feature"""
    # Test implementation
    pass
```

2. Add to test runner in `run_tests.py` if using manual runner

3. Run to verify:

```bash
pytest tests/test_yourfile.py::test_new_feature -v
```

## Links

- [Node.js Tests](../../client-js/test-v2/)
- [Python Client Documentation](../README.md)
- [Queen MQ Documentation](https://queenmq.com/)

