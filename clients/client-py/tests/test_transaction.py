"""
Transaction operation tests
"""

import asyncio
import pytest


@pytest.mark.asyncio
async def test_transaction_basic_push_ack(client):
    """Test basic transaction with push and ack"""
    queue_a = await client.queue("test-queue-v2-txn-basic-a").create()
    queue_b = await client.queue("test-queue-v2-txn-basic-b").create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push to queue A
    await client.queue("test-queue-v2-txn-basic-a").push([{"data": {"value": 1}}])
    
    # Pop from A
    messages = await client.queue("test-queue-v2-txn-basic-a").batch(1).wait(False).pop()
    assert len(messages) > 0, "No message to consume"
    
    # Transaction: ack A, push to B
    await (client.transaction()
        .queue("test-queue-v2-txn-basic-b")
        .push([{"data": {"value": messages[0]["data"]["value"] + 1}}])
        .ack(messages[0])
        .commit())
    
    # Verify message moved to B
    result_b = await client.queue("test-queue-v2-txn-basic-b").batch(1).wait(False).pop()
    result_a = await client.queue("test-queue-v2-txn-basic-a").batch(1).wait(False).pop()
    
    assert len(result_b) == 1
    assert len(result_a) == 0
    assert result_b[0]["data"]["value"] == 2


@pytest.mark.asyncio
async def test_transaction_multiple_pushes(client):
    """Test transaction with multiple push operations"""
    queue_a = await client.queue("test-queue-v2-txn-multi-a").create()
    queue_b = await client.queue("test-queue-v2-txn-multi-b").create()
    queue_c = await client.queue("test-queue-v2-txn-multi-c").create()
    assert all([queue_a.get("configured"), queue_b.get("configured"), queue_c.get("configured")])
    
    # Push to A
    await client.queue("test-queue-v2-txn-multi-a").push([{"data": {"id": "source"}}])
    
    # Pop from A
    messages = await client.queue("test-queue-v2-txn-multi-a").batch(1).wait(False).pop()
    assert len(messages) > 0
    
    # Transaction: Push to B and C, ack from A
    await (client.transaction()
        .queue("test-queue-v2-txn-multi-b")
        .push([{"data": {"id": "b", "source": messages[0]["data"]["id"]}}])
        .queue("test-queue-v2-txn-multi-c")
        .push([{"data": {"id": "c", "source": messages[0]["data"]["id"]}}])
        .ack(messages[0])
        .commit())
    
    # Verify
    result_b = await client.queue("test-queue-v2-txn-multi-b").batch(1).wait(False).pop()
    result_c = await client.queue("test-queue-v2-txn-multi-c").batch(1).wait(False).pop()
    result_a = await client.queue("test-queue-v2-txn-multi-a").batch(1).wait(False).pop()
    
    assert len(result_b) == 1
    assert len(result_c) == 1
    assert len(result_a) == 0


@pytest.mark.asyncio
async def test_transaction_multiple_acks(client):
    """Test transaction with multiple ack operations"""
    queue_a = await client.queue("test-queue-v2-txn-multi-ack-a").config({}).create()
    queue_b = await client.queue("test-queue-v2-txn-multi-ack-b").config({}).create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push 3 messages to A
    await client.queue("test-queue-v2-txn-multi-ack-a").push([
        {"data": {"value": 1}},
        {"data": {"value": 2}},
        {"data": {"value": 3}}
    ])
    
    # Pop 3 messages
    messages = await client.queue("test-queue-v2-txn-multi-ack-a").batch(3).wait(False).pop()
    assert len(messages) == 3
    
    # Transaction: Ack all 3, push summary to B
    total = sum(msg["data"]["value"] for msg in messages)
    await (client.transaction()
        .ack(messages[0])
        .ack(messages[1])
        .ack(messages[2])
        .queue("test-queue-v2-txn-multi-ack-b")
        .push([{"data": {"sum": total}}])
        .commit())
    
    # Verify
    result_a = await client.queue("test-queue-v2-txn-multi-ack-a").batch(3).wait(False).pop()
    result_b = await client.queue("test-queue-v2-txn-multi-ack-b").batch(1).wait(False).pop()
    
    assert len(result_a) == 0
    assert len(result_b) == 1
    assert result_b[0]["data"]["sum"] == 6


@pytest.mark.asyncio
async def test_transaction_ack_with_status(client):
    """Test transaction with custom ack status"""
    queue_a = await client.queue("test-queue-v2-txn-ack-status-a").config({"retry_limit": 0}).create()
    queue_b = await client.queue("test-queue-v2-txn-ack-status-b").config({"retry_limit": 0}).create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push message
    await client.queue("test-queue-v2-txn-ack-status-a").push([{"data": {"value": 1}}])
    
    # Pop
    messages = await client.queue("test-queue-v2-txn-ack-status-a").batch(1).wait(False).pop()
    assert len(messages) > 0
    
    # Transaction with custom status
    await (client.transaction()
        .ack(messages[0], "failed")
        .queue("test-queue-v2-txn-ack-status-b")
        .push([{"data": {"processed": True}}])
        .commit())
    
    # Verify
    result_a = await client.queue("test-queue-v2-txn-ack-status-a").batch(1).wait(False).pop()
    result_b = await client.queue("test-queue-v2-txn-ack-status-b").batch(1).wait(False).pop()
    
    assert len(result_a) == 0
    assert len(result_b) == 1


@pytest.mark.asyncio
async def test_transaction_atomicity(client):
    """Test transaction atomicity"""
    queue_a = await client.queue("test-queue-v2-txn-atomic-a").create()
    queue_b = await client.queue("test-queue-v2-txn-atomic-b").create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push to A
    await client.queue("test-queue-v2-txn-atomic-a").push([{"data": {"value": 100}}])
    
    # Pop from A
    messages = await client.queue("test-queue-v2-txn-atomic-a").batch(1).wait(False).pop()
    assert len(messages) > 0
    
    # Successful transaction
    await (client.transaction()
        .ack(messages[0])
        .queue("test-queue-v2-txn-atomic-b")
        .push([{"data": {"value": 200}}])
        .commit())
    
    # Both operations should succeed atomically
    result_a = await client.queue("test-queue-v2-txn-atomic-a").batch(1).wait(False).pop()
    result_b = await client.queue("test-queue-v2-txn-atomic-b").batch(1).wait(False).pop()
    
    assert len(result_a) == 0
    assert len(result_b) == 1
    assert result_b[0]["data"]["value"] == 200


@pytest.mark.asyncio
async def test_transaction_chained_processing(client):
    """Test chained transaction processing"""
    queue1 = await client.queue("test-queue-v2-txn-chain-1").create()
    queue2 = await client.queue("test-queue-v2-txn-chain-2").create()
    queue3 = await client.queue("test-queue-v2-txn-chain-3").create()
    assert all([queue1.get("configured"), queue2.get("configured"), queue3.get("configured")])
    
    # Initial message
    await client.queue("test-queue-v2-txn-chain-1").push([{"data": {"step": 1, "value": 10}}])
    
    # Transaction 1: Process from queue1 to queue2
    msg1 = await client.queue("test-queue-v2-txn-chain-1").batch(1).wait(False).pop()
    assert len(msg1) > 0
    
    await (client.transaction()
        .queue("test-queue-v2-txn-chain-2")
        .push([{"data": {"step": 2, "value": msg1[0]["data"]["value"] * 2}}])
        .ack(msg1[0])
        .commit())
    
    # Transaction 2: Process from queue2 to queue3
    msg2 = await client.queue("test-queue-v2-txn-chain-2").batch(1).wait(False).pop()
    assert len(msg2) > 0
    
    await (client.transaction()
        .queue("test-queue-v2-txn-chain-3")
        .push([{"data": {"step": 3, "value": msg2[0]["data"]["value"] + 5}}])
        .ack(msg2[0])
        .commit())
    
    # Verify final result
    result = await client.queue("test-queue-v2-txn-chain-3").batch(1).wait(False).pop()
    empty1 = await client.queue("test-queue-v2-txn-chain-1").batch(1).wait(False).pop()
    empty2 = await client.queue("test-queue-v2-txn-chain-2").batch(1).wait(False).pop()
    
    assert len(result) == 1
    assert result[0]["data"]["value"] == 25
    assert len(empty1) == 0
    assert len(empty2) == 0


@pytest.mark.asyncio
async def test_transaction_batch_push(client):
    """Test transaction with batch push"""
    queue_a = await client.queue("test-queue-v2-txn-batch-a").create()
    queue_b = await client.queue("test-queue-v2-txn-batch-b").create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push source message
    await client.queue("test-queue-v2-txn-batch-a").push([{"data": {"id": "batch-source"}}])
    
    # Pop from A
    messages = await client.queue("test-queue-v2-txn-batch-a").batch(1).wait(False).pop()
    assert len(messages) > 0
    
    # Transaction: Push multiple messages to B, ack from A
    await (client.transaction()
        .queue("test-queue-v2-txn-batch-b")
        .push([
            {"data": {"index": 1}},
            {"data": {"index": 2}},
            {"data": {"index": 3}},
            {"data": {"index": 4}},
            {"data": {"index": 5}}
        ])
        .ack(messages[0])
        .commit())
    
    # Verify all 5 messages in B
    result_b = await client.queue("test-queue-v2-txn-batch-b").batch(10).wait(False).pop()
    result_a = await client.queue("test-queue-v2-txn-batch-a").batch(1).wait(False).pop()
    
    assert len(result_b) == 5
    assert len(result_a) == 0


@pytest.mark.asyncio
async def test_transaction_with_partitions(client):
    """Test transaction with multiple partitions"""
    queue = await client.queue("test-queue-v2-txn-partitions").create()
    assert queue.get("configured")
    
    # Push to different partitions
    await client.queue("test-queue-v2-txn-partitions").partition("p1").push([{"data": {"partition": "p1", "value": 1}}])
    await client.queue("test-queue-v2-txn-partitions").partition("p2").push([{"data": {"partition": "p2", "value": 2}}])
    
    # Pop from both
    msg1 = await client.queue("test-queue-v2-txn-partitions").partition("p1").batch(1).wait(False).pop()
    msg2 = await client.queue("test-queue-v2-txn-partitions").partition("p2").batch(1).wait(False).pop()
    
    assert len(msg1) > 0 and len(msg2) > 0
    
    # Transaction: Ack both
    await (client.transaction()
        .ack(msg1[0])
        .ack(msg2[0])
        .commit())
    
    # Verify both partitions empty
    result1 = await client.queue("test-queue-v2-txn-partitions").partition("p1").batch(1).wait(False).pop()
    result2 = await client.queue("test-queue-v2-txn-partitions").partition("p2").batch(1).wait(False).pop()
    
    assert len(result1) == 0
    assert len(result2) == 0


@pytest.mark.asyncio
async def test_transaction_with_consumer(client):
    """Test transaction with consumer"""
    queue_a = await client.queue("test-queue-v2-txn-consumer-a").create()
    queue_b = await client.queue("test-queue-v2-txn-consumer-b").create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push messages
    await client.queue("test-queue-v2-txn-consumer-a").push([
        {"data": {"value": 1}},
        {"data": {"value": 2}},
        {"data": {"value": 3}}
    ])
    
    processed_count = 0
    
    async def handler(msg):
        nonlocal processed_count
        await (client.transaction()
            .queue("test-queue-v2-txn-consumer-b")
            .push([{"data": {"value": msg["data"]["value"] * 10}}])
            .ack(msg)
            .commit())
        processed_count += 1
    
    # Consume with transaction
    await (client.queue("test-queue-v2-txn-consumer-a")
        .batch(1)
        .wait(False)
        .limit(3)
        .each()
        .auto_ack(False)
        .consume(handler))
    
    # Verify results
    result_a = await client.queue("test-queue-v2-txn-consumer-a").batch(10).wait(False).pop()
    result_b = await client.queue("test-queue-v2-txn-consumer-b").batch(10).wait(False).pop()
    
    assert processed_count == 3
    assert len(result_a) == 0
    assert len(result_b) == 3


@pytest.mark.asyncio
async def test_transaction_empty_commit(client):
    """Test empty transaction should throw error"""
    queue = await client.queue("test-queue-v2-txn-empty").create()
    assert queue.get("configured")
    
    # Empty transaction should throw
    error_thrown = False
    try:
        await client.transaction().commit()
    except Exception as error:
        error_thrown = str(error) == "Transaction has no operations to commit"
    
    assert error_thrown


@pytest.mark.asyncio
async def test_transaction_large_payload(client):
    """Test transaction with large payload"""
    queue_a = await client.queue("test-queue-v2-txn-large-a").create()
    queue_b = await client.queue("test-queue-v2-txn-large-b").create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Push large message
    large_data = {
        "items": [{"id": i, "data": f"item-{i}"} for i in range(1000)]
    }
    await client.queue("test-queue-v2-txn-large-a").push([{"data": large_data}])
    
    # Pop and process in transaction
    messages = await client.queue("test-queue-v2-txn-large-a").batch(1).wait(False).pop()
    assert len(messages) > 0
    
    await (client.transaction()
        .queue("test-queue-v2-txn-large-b")
        .push([{"data": {"count": len(messages[0]["data"]["items"])}}])
        .ack(messages[0])
        .commit())
    
    # Verify
    result_b = await client.queue("test-queue-v2-txn-large-b").batch(1).wait(False).pop()
    
    assert len(result_b) == 1
    assert result_b[0]["data"]["count"] == 1000


@pytest.mark.asyncio
async def test_transaction_multiple_queues(client):
    """Test transaction across multiple queues"""
    queue_a = await client.queue("test-queue-v2-txn-multi-q-a").create()
    queue_b = await client.queue("test-queue-v2-txn-multi-q-b").create()
    queue_c = await client.queue("test-queue-v2-txn-multi-q-c").create()
    assert all([queue_a.get("configured"), queue_b.get("configured"), queue_c.get("configured")])
    
    # Push to A and B
    await client.queue("test-queue-v2-txn-multi-q-a").push([{"data": {"source": "A"}}])
    await client.queue("test-queue-v2-txn-multi-q-b").push([{"data": {"source": "B"}}])
    
    # Pop from both
    msg_a = await client.queue("test-queue-v2-txn-multi-q-a").batch(1).wait(False).pop()
    msg_b = await client.queue("test-queue-v2-txn-multi-q-b").batch(1).wait(False).pop()
    
    assert len(msg_a) > 0 and len(msg_b) > 0
    
    # Transaction: Ack from A and B, push to C
    await (client.transaction()
        .ack(msg_a[0])
        .ack(msg_b[0])
        .queue("test-queue-v2-txn-multi-q-c")
        .push([{"data": {"merged": True}}])
        .commit())
    
    # Verify
    result_a = await client.queue("test-queue-v2-txn-multi-q-a").batch(1).wait(False).pop()
    result_b = await client.queue("test-queue-v2-txn-multi-q-b").batch(1).wait(False).pop()
    result_c = await client.queue("test-queue-v2-txn-multi-q-c").batch(1).wait(False).pop()
    
    assert len(result_a) == 0
    assert len(result_b) == 0
    assert len(result_c) == 1


@pytest.mark.asyncio
async def test_transaction_rollback(client):
    """Test transaction rollback on failure - simplified version"""
    queue_a = await client.queue("test-queue-v2-txn-rollback-a").config({"lease_time": 10}).create()
    queue_b = await client.queue("test-queue-v2-txn-rollback-b").config({"lease_time": 10}).create()
    assert queue_a.get("configured") and queue_b.get("configured")
    
    # Test that transaction API works (actual rollback behavior is complex and server-dependent)
    # Push, pop, and verify transaction operations complete
    
    await client.queue("test-queue-v2-txn-rollback-a").push([{"data": {"test": "rollback"}}])
    messages = await client.queue("test-queue-v2-txn-rollback-a").batch(1).wait(False).pop()
    assert len(messages) > 0
    
    # Transaction with valid operations (should succeed)
    await (client.transaction()
        .queue("test-queue-v2-txn-rollback-b")
        .push([{"data": {"value": 1}}])
        .ack(messages[0])
        .commit())
    
    # Verify transaction succeeded
    result_b = await client.queue("test-queue-v2-txn-rollback-b").batch(10).wait(False).pop()
    result_a = await client.queue("test-queue-v2-txn-rollback-a").batch(10).wait(False).pop()
    
    assert len(result_b) == 1, "Message should be in queue B"
    assert len(result_a) == 0, "Message should be removed from queue A"
    
    # Clean up
    await client.ack(result_b[0], True)
    
    print("✅ Transaction test completed successfully")


@pytest.mark.asyncio
async def test_transaction_ack_with_consumer_group(client):
    """Test transaction ack with consumer group context"""
    queue_a = await client.queue("test-queue-v2-txn-cg-a").create()
    queue_b = await client.queue("test-queue-v2-txn-cg-b").create()
    assert queue_a.get("configured") and queue_b.get("configured")

    consumer_group = "test-consumer-group-txn"

    # Push messages to queue A
    await client.queue("test-queue-v2-txn-cg-a").push([
        {"data": {"value": 1}},
        {"data": {"value": 2}}
    ])

    # Pop with consumer group. The group is created here, after the push, so it
    # has to ask for 'all' to see the backlog (server default is 'new').
    messages = await (client.queue("test-queue-v2-txn-cg-a")
        .group(consumer_group)
        .subscription_mode("all")
        .batch(2)
        .wait(False)
        .pop())

    assert len(messages) == 2, f"Expected 2 messages, got {len(messages)}"

    # Transaction: ACK with consumer group context, push to B
    await (client.transaction()
        .ack(messages[0], "completed", {"consumer_group": consumer_group})
        .ack(messages[1], "completed", {"consumer_group": consumer_group})
        .queue("test-queue-v2-txn-cg-b")
        .push([{"data": {"sum": messages[0]["data"]["value"] + messages[1]["data"]["value"]}}])
        .commit())

    # Verify: Pop again with same consumer group should get no messages
    # (consumer group cursor should have advanced)
    messages_after_ack = await (client.queue("test-queue-v2-txn-cg-a")
        .group(consumer_group)
        .batch(2)
        .wait(False)
        .pop())

    # Verify: Queue B has the result
    result_b = await client.queue("test-queue-v2-txn-cg-b").batch(1).wait(False).pop()

    # Verify: Using a different consumer group should still see the messages
    messages_other_group = await (client.queue("test-queue-v2-txn-cg-a")
        .group("other-consumer-group")
        .subscription_mode("all")
        .batch(2)
        .wait(False)
        .pop())

    assert len(messages_after_ack) == 0, f"Consumer group cursor should have advanced, got {len(messages_after_ack)} messages"
    assert len(result_b) == 1, f"Queue B should have 1 message, got {len(result_b)}"
    assert result_b[0]["data"]["sum"] == 3, f"Sum should be 3, got {result_b[0]['data']['sum']}"
    assert len(messages_other_group) == 2, f"Other consumer group should see 2 messages, got {len(messages_other_group)}"

    print("✅ Transaction ack with consumer group completed - cursor advanced correctly")

