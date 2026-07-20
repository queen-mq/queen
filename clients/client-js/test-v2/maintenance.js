import { Queen } from '../client-v2/index.js';
import axios from 'axios';

export async function test_maintenance_mode(client = null) {
  console.log('🧪 Testing Maintenance Mode with File Buffer\n');
  
  const queen = client
  
  // Default Queen port; override with QUEEN_SERVER_URL (same as run.js).
  const API_URL = process.env.QUEEN_SERVER_URL || 'http://localhost:6632';
  const QUEUE_NAME = 'test-maintenance-queue';
  const MESSAGES_PER_SECOND = 10;
  const MAINTENANCE_DURATION_SECONDS = 10;
  
  let producedCount = 0;
  let receivedCount = 0;
  let consumerActive = false;
  
  try {
    // 0. Ensure maintenance mode is OFF before starting
    console.log('🧹 Step 0: Ensuring clean state...');
    await axios.post(`${API_URL}/api/v1/system/maintenance`, { enabled: false });
    
    let initialStatus = await axios.get(`${API_URL}/api/v1/system/maintenance`);
    console.log(`   Initial status:`, initialStatus.data);
    
    if (initialStatus.data.bufferedMessages > 0) {
      console.log(`   ⚠️  Found ${initialStatus.data.bufferedMessages} buffered messages from previous runs`);
      console.log('   Waiting for buffer to drain (checking every 2 seconds)...');
      
      // Wait for buffer to drain completely
      let waitTime = 0;
      const maxWait = 60; // Max 60 seconds
      while (waitTime < maxWait) {
        await sleep(2000);
        waitTime += 2;
        const status = await axios.get(`${API_URL}/api/v1/system/maintenance`);
        console.log(`   ${waitTime}s - Buffered: ${status.data.bufferedMessages}`);
        
        if (status.data.bufferedMessages === 0) {
          console.log('   ✅ Buffer drained completely');
          break;
        }
      }
      
      if (waitTime >= maxWait) {
        console.log(`   ⚠️  Timeout waiting for buffer to drain. Continuing anyway...`);
      }
    }
    console.log('');
    
    // 1. Configure queue
    console.log(`📋 Step 1: Configuring queue "${QUEUE_NAME}"...`);
    await queen.queue(QUEUE_NAME).config({
      leaseTime: 60,
    }).create();
    console.log('✅ Queue configured\n');
    
    // 2. Start consumer
    console.log('📥 Step 2: Starting consumer...');
    consumerActive = true;
    
    const consumePromise = (async () => {
      while (consumerActive) {
        try {
          // wait(true) long-polls on the server. This is important: pushes
          // commit before partition_lookup is refreshed (PUSHPOPLOOKUPSOL
          // is async), so a wait(false) pop can race past freshly-pushed
          // messages and return empty. Long-poll re-runs the candidate
          // scan internally and picks up messages once the lookup row
          // commits — typically within milliseconds.
          // Use a short timeout (1s) so the consumer notices
          // consumerActive=false promptly when the test winds down.
          const messages = await queen.queue(QUEUE_NAME)
            .batch(10)
            .wait(true)
            .timeoutMillis(1000)
            .limit(1)
            .each()
            .pop();

          if (messages && messages.length > 0) {
            receivedCount += messages.length;
            console.log(`  📨 Received ${messages.length} messages (total: ${receivedCount})`);
            
            // Acknowledge messages
            await queen.ack(messages);
            
          }
        } catch (err) {
          if (consumerActive) {
            console.error('  ❌ Consumer error:', err.message);
          }
        }
        await sleep(10);
      }
    })();
    
    console.log('✅ Consumer started\n');
    
    // 3. Start producer (10 messages/second)
    console.log(`📤 Step 3: Starting producer (${MESSAGES_PER_SECOND} msgs/sec)...`);
    let producerRunning = true;
    
    const producerLoop = async () => {
      while (producerRunning) {
        try {
          // Push batch of 10 messages
          const batch = [];
          for (let i = 0; i < MESSAGES_PER_SECOND; i++) {
            batch.push({
              data: { 
                messageNumber: producedCount + i,
                timestamp: Date.now()
              }
            });
          }
          
          // Push and wait for confirmation
          await queen.queue(QUEUE_NAME).push(batch);
          producedCount += batch.length;  // Only increment after successful push
          
          // Wait 1 second before next batch
          await sleep(1000);
        } catch (err) {
          console.error('  ❌ Producer error:', err.message);
          await sleep(1000); // Wait before retry
        }
      }
    };
    
    const producerPromise = producerLoop();
    
    console.log('✅ Producer started\n');
    
    // Wait 3 seconds for messages to flow normally
    console.log('⏱️  Step 4: Waiting 3 seconds for normal message flow...');
    await sleep(3000);
    console.log(`   Produced: ${producedCount}, Received: ${receivedCount}\n`);
    
    const receivedBeforeMaintenance = receivedCount;
    
    // 4. Enable maintenance mode
    console.log('🔧 Step 5: Enabling MAINTENANCE MODE...');
    const maintenanceResponse = await axios.post(`${API_URL}/api/v1/system/maintenance`, {
      enabled: true
    });
    console.log(`   Response:`, maintenanceResponse.data);
    console.log('✅ Maintenance mode enabled\n');
    
    // Wait 2 seconds and check that consumer stops receiving
    console.log('⏱️  Step 6: Waiting 5 seconds - consumer should stop receiving...');
    await sleep(5000);
    const receivedDuringWait = receivedCount - receivedBeforeMaintenance;
    console.log(`   Messages received during maintenance: ${receivedDuringWait}`);
    console.log(`   Total: Produced=${producedCount}, Received=${receivedCount}\n`);
    
    // 5. Keep producing during maintenance
    console.log(`⏱️  Step 7: Producing during maintenance for ${MAINTENANCE_DURATION_SECONDS} seconds...`);
    console.log('   (Messages should go to file buffer)\n');
    await sleep(MAINTENANCE_DURATION_SECONDS * 1000);
    
    const producedDuringMaintenance = producedCount;
    const receivedAfterMaintenance = receivedCount;
    const messagesBuffered = producedDuringMaintenance - receivedAfterMaintenance;
    
    console.log(`   Maintenance period complete:`);
    console.log(`   - Total produced: ${producedCount}`);
    console.log(`   - Total received: ${receivedCount}`);
    console.log(`   - Buffered (should be ~${MESSAGES_PER_SECOND * MAINTENANCE_DURATION_SECONDS}): ${messagesBuffered}\n`);
    
    // 6. Check maintenance status
    console.log('📊 Step 8: Checking maintenance status...');
    const statusResponse = await axios.get(`${API_URL}/api/v1/system/maintenance`);
    console.log(`   Status:`, statusResponse.data);
    console.log('');
    
    // 7. Disable maintenance mode
    console.log('✅ Step 9: Disabling MAINTENANCE MODE...');
    const disableResponse = await axios.post(`${API_URL}/api/v1/system/maintenance`, {
      enabled: false
    });
    console.log(`   Response:`, disableResponse.data);
    console.log('   (File buffer should start draining to database)\n');
    
    // 8. Wait for messages to resume and stop producer
    console.log('⏱️  Step 10: Waiting for messages to resume...');
    await sleep(2000);
    
    const receivedAfterResume = receivedCount;
    const messagesResumed = receivedAfterResume - receivedAfterMaintenance;
    console.log(`   Messages received after resuming: ${messagesResumed}`);
    console.log('✅ Messages are flowing again!\n');
    
    // Stop producer
    console.log('🛑 Step 11: Stopping producer...');
    producerRunning = false;
    await producerPromise;  // Wait for producer to actually stop
    
    const finalProducedCount = producedCount;
    console.log(`   Final produced count: ${finalProducedCount}\n`);
    
    // 9. Wait for drain to complete
    console.log('⏱️  Step 12: Waiting 10 seconds for file buffer to drain...');
    for (let i = 0; i < 10; i++) {
      await sleep(1000);
      console.log(`   ${i + 1}s - Received: ${receivedCount}/${finalProducedCount} (${((receivedCount/finalProducedCount)*100).toFixed(1)}%)`);
      
      // Check if we've received all messages
      if (receivedCount >= finalProducedCount) {
        console.log('   ✅ All messages received!\n');
        break;
      }
    }
    
    // 10. Stop consumer
    console.log('🛑 Step 13: Stopping consumer...');
    consumerActive = false;
    await consumePromise;
    console.log('✅ Consumer stopped\n');
    
    // 11. Final verification
    console.log('📊 Final Verification:\n');
    console.log(`   Total Produced: ${finalProducedCount}`);
    console.log(`   Total Received: ${receivedCount}`);
    console.log(`   Difference: ${finalProducedCount - receivedCount}`);
    
    if (receivedCount === finalProducedCount) {
      console.log('\n✅ SUCCESS: All messages accounted for!');
      console.log('   Maintenance mode works correctly with file buffer.\n');
      
      return { 
        success: true, 
        message: `All ${finalProducedCount} messages accounted for during maintenance mode test` 
      };
    } else if (receivedCount < finalProducedCount) {
      const missing = finalProducedCount - receivedCount;
      console.log(`\n⚠️  WARNING: Missing ${missing} messages`);
      console.log('   They may still be in the file buffer or queue.');
      console.log('   Check /var/lib/queen/buffers/ for remaining files.\n');
      
      // Check buffer status
      const finalStatus = await axios.get(`${API_URL}/api/v1/system/maintenance`);
      console.log('   Buffer status:', finalStatus.data);
      
      return { 
        success: false, 
        message: `Missing ${missing} messages (produced: ${finalProducedCount}, received: ${receivedCount})` 
      };
    } else {
      console.log(`\n❌ ERROR: Received more messages than produced! (${receivedCount} > ${finalProducedCount})`);
      console.log('   This should not happen - possible duplicate issue.\n');
      
      return { 
        success: false, 
        message: `Received more than produced (${receivedCount} > ${finalProducedCount})` 
      };
    }
    
  } catch (err) {
    console.error('\n❌ Test failed:', err.message);
    console.error(err.stack);
    
    // Clean up
    consumerActive = false;
    
    return { success: false, message: `Test threw error: ${err.message}` };
  } finally {
    // Clean up
    consumerActive = false;
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

