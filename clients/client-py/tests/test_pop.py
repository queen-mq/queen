"""
Pop operation tests
"""

import asyncio
import pytest


@pytest.mark.asyncio
async def test_pop_empty_queue(client):
    """Test popping from empty queue"""
    queue = await client.queue("test-queue-v2-pop-empty").create()
    assert queue.get("configured") is True
    
    res = await client.queue("test-queue-v2-pop-empty").batch(1).wait(False).pop()
    assert len(res) == 0


@pytest.mark.asyncio
async def test_pop_non_empty_queue(client):
    """Test popping from non-empty queue"""
    queue = await client.queue("test-queue-v2-pop-non-empty").create()
    assert queue.get("configured") is True
    
    await client.queue("test-queue-v2-pop-non-empty").push([{"data": {"message": "Hello, world!"}}])

    # wait(True) to ride out the PUSHPOPLOOKUPSOL race: partition_lookup
    # is updated asynchronously after push commits, so a wait(False) pop
    # immediately after push can race past the not-yet-committed lookup
    # row and return empty. Long-poll re-runs the candidate scan and
    # picks up the row once it commits (typically within ms).
    res = await client.queue("test-queue-v2-pop-non-empty").batch(1).wait(True).pop()
    assert len(res) == 1


@pytest.mark.asyncio
async def test_pop_with_wait(client):
    """Test pop with long polling (wait)"""
    queue = await client.queue("test-queue-v2-pop-with-wait").create()
    assert queue.get("configured") is True
    
    # Push message after 2 seconds
    async def delayed_push():
        await asyncio.sleep(2)
        await client.queue("test-queue-v2-pop-with-wait").push([{"data": {"message": "Hello, world!"}}])
    
    # Start delayed push
    push_task = asyncio.create_task(delayed_push())
    
    # Pop with wait (should wait for message)
    res = await client.queue("test-queue-v2-pop-with-wait").batch(1).wait(True).pop()
    
    await push_task
    
    assert len(res) == 1


@pytest.mark.asyncio
async def test_pop_with_ack(client):
    """Test pop with manual acknowledgment"""
    queue = await client.queue("test-queue-v2-pop-with-ack").create()
    assert queue.get("configured") is True
    
    await client.queue("test-queue-v2-pop-with-ack").push([{"data": {"message": "Hello, world!"}}])
    
    res = await client.queue("test-queue-v2-pop-with-ack").batch(1).wait(False).pop()
    
    res_ack = await client.ack(res[0])
    
    assert res_ack.get("success") is True, f"Ack failed: {res_ack.get('error')}"


@pytest.mark.asyncio
async def test_pop_with_ack_reconsume(client):
    """Test pop and ack operations"""
    queue = await client.queue("test-queue-v2-pop-with-ack-reconsume").config({
        "lease_time": 1  # 1 second lease
    }).create()
    assert queue.get("configured") is True
    
    # Push 2 messages separately
    await client.queue("test-queue-v2-pop-with-ack-reconsume").push([
        {"data": {"message": "Message 1"}}
    ])
    await asyncio.sleep(0.1)  # Small delay between pushes
    
    await client.queue("test-queue-v2-pop-with-ack-reconsume").push([
        {"data": {"message": "Message 2"}}
    ])
    
    # Pop first message
    res = await client.queue("test-queue-v2-pop-with-ack-reconsume").batch(1).wait(False).pop()
    assert len(res) == 1, f"Expected 1 message, got {len(res)}"
    
    # Ack first message
    await client.ack(res[0], True)
    
    # Pop second message (should be available)
    res2 = await client.queue("test-queue-v2-pop-with-ack-reconsume").batch(1).wait(False).pop()
    assert len(res2) == 1, f"Expected 1 message, got {len(res2)}"
    
    # Ack second message
    await client.ack(res2[0], True)
    
    # Verify queue is empty
    res3 = await client.queue("test-queue-v2-pop-with-ack-reconsume").batch(10).wait(False).pop()
    assert len(res3) == 0, "Queue should be empty after acking all messages"

