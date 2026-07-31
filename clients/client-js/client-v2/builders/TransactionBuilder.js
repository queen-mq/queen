/**
 * Transaction builder for atomic operations
 */

import * as logger from '../utils/logger.js'
import { generateUUID } from './QueueBuilder.js'
import { isValidUUID } from '../utils/validation.js'

export class TransactionBuilder {
  #httpClient
  #operations = []
  #requiredLeases = []

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  ack(messages, status = 'completed', context = {}) {
    const msgs = Array.isArray(messages) ? messages : [messages]
    
    logger.log('TransactionBuilder.ack', { count: msgs.length, status, consumerGroup: context.consumerGroup })

    msgs.forEach(msg => {
      const transactionId = typeof msg === 'string' ? msg : (msg.transactionId || msg.id)
      const partitionId = typeof msg === 'object' ? msg.partitionId : null
      const leaseId = typeof msg === 'object' ? msg.leaseId : null

      if (!transactionId) {
        throw new Error('Message must have transactionId or id property')
      }

      // CRITICAL: partitionId is now MANDATORY to prevent acking wrong message
      if (!partitionId) {
        throw new Error('Message must have partitionId property to ensure message uniqueness')
      }

      const operation = {
        type: 'ack',
        transactionId,
        partitionId,
        status
      }

      // Add consumerGroup if provided in context
      if (context.consumerGroup) {
        operation.consumerGroup = context.consumerGroup
      }

      this.#operations.push(operation)

      if (leaseId) {
        this.#requiredLeases.push(leaseId)
      }
    })

    return this
  }

  queue(queueName) {
    // Return a sub-builder for push operations with partition support
    let partition = null
    
    const subBuilder = {
      partition: (partitionKey) => {
        partition = partitionKey
        return subBuilder
      },
      
      push: (items) => {
        const itemArray = Array.isArray(items) ? items : [items]
        
        logger.log('TransactionBuilder.queue.push', { queue: queueName, partition, count: itemArray.length })

        this.#operations.push({
          type: 'push',
          items: itemArray.map(item => {
            // Check if property exists, not just truthy (to support null values)
            let payloadValue
            if ('data' in item) {
              payloadValue = item.data
            } else if ('payload' in item) {
              payloadValue = item.payload
            } else {
              payloadValue = item
            }

            // Same contract as QueueBuilder.push: the caller's transactionId is
            // what makes a retried transaction idempotent inside the dedup
            // window, so it has to reach the wire. Absent, mint one here rather
            // than leaving the broker to do it, so the id is knowable client
            // side either way.
            const result = {
              queue: queueName,
              payload: payloadValue,
              transactionId: item.transactionId || generateUUID()
            }

            // Add partition if set
            if (partition !== null) {
              result.partition = partition
            }

            if (item.traceId && isValidUUID(item.traceId)) {
              result.traceId = item.traceId
            }

            return result
          })
        })

        return this
      }
    }
    
    return subBuilder
  }

  async commit() {
    if (this.#operations.length === 0) {
      logger.error('TransactionBuilder.commit', 'No operations to commit')
      throw new Error('Transaction has no operations to commit')
    }

    logger.log('TransactionBuilder.commit', { operationCount: this.#operations.length, requiredLeases: this.#requiredLeases.length })

    try {
      const result = await this.#httpClient.post('/api/v1/transaction', {
        operations: this.#operations,
        requiredLeases: [...new Set(this.#requiredLeases)] // Unique leases
      })

      if (!result.success) {
        logger.error('TransactionBuilder.commit', { error: result.error })
        throw new Error(result.error || 'Transaction failed')
      }

      logger.log('TransactionBuilder.commit', { status: 'success' })
      return result
    } catch (error) {
      logger.error('TransactionBuilder.commit', { error: error.message })
      throw error
    }
  }
}

