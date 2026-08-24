package queen

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// ConsumerManager manages concurrent consumer workers.
type ConsumerManager struct {
	httpClient *HttpClient
	queen      *Queen
}

// NewConsumerManager creates a new ConsumerManager.
func NewConsumerManager(httpClient *HttpClient, queen *Queen) *ConsumerManager {
	return &ConsumerManager{
		httpClient: httpClient,
		queen:      queen,
	}
}

// Start starts consumer workers for single message handling.
func (cm *ConsumerManager) Start(ctx context.Context, handler MessageHandler, opts ConsumeOptions) error {
	return cm.startWorkers(ctx, opts, func(ctx context.Context, msgs []*Message) error {
		for _, msg := range msgs {
			if err := handler(ctx, msg); err != nil {
				return err
			}
		}
		return nil
	}, false)
}

// StartBatch starts consumer workers for batch message handling.
func (cm *ConsumerManager) StartBatch(ctx context.Context, handler BatchMessageHandler, opts ConsumeOptions) error {
	return cm.startWorkers(ctx, opts, handler, true)
}

// startWorkers starts the consumer workers.
func (cm *ConsumerManager) startWorkers(ctx context.Context, opts ConsumeOptions, handler BatchMessageHandler, isBatch bool) error {
	// Build path and params
	path := cm.buildPath(opts)
	baseParams := cm.buildParams(opts)

	// Generate affinity key for consistent routing
	affinityKey := cm.getAffinityKey(opts)

	logInfo("ConsumerManager.Start", map[string]interface{}{
		"queue":       opts.Queue,
		"partition":   opts.Partition,
		"namespace":   opts.Namespace,
		"task":        opts.Task,
		"group":       opts.Group,
		"concurrency": opts.Concurrency,
		// batch/maxPartitions are the USER's values here, so 0 reads as
		// "the broker sizes this one" rather than as a bogus zero.
		"batch":         opts.Batch,
		"maxPartitions": opts.MaxPartitions,
		"autopilot":     cm.autopilotEnabled(opts),
		"limit":         opts.Limit,
		"autoAck":       opts.AutoAck,
		"wait":          opts.Wait,
		"each":          opts.Each,
	})

	// Start workers
	var wg sync.WaitGroup
	errChan := make(chan error, opts.Concurrency)

	for i := 0; i < opts.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			err := cm.worker(ctx, workerID, handler, isBatch, path, baseParams, affinityKey, opts)
			if err != nil && err != context.Canceled {
				errChan <- err
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errChan)

	// Return first error if any
	for err := range errChan {
		return err
	}

	logInfo("ConsumerManager.Start", map[string]interface{}{
		"status": "completed",
	})

	return nil
}

// worker is the main worker loop.
func (cm *ConsumerManager) worker(
	ctx context.Context,
	workerID int,
	handler BatchMessageHandler,
	isBatch bool,
	path string,
	baseParams string,
	affinityKey string,
	opts ConsumeOptions,
) error {
	logDebug("ConsumerManager.worker", map[string]interface{}{
		"workerId": workerID,
		"status":   "started",
		"limit":    opts.Limit,
		"idleMs":   opts.IdleMillis,
	})

	processedCount := 0
	var lastMessageTime time.Time
	if opts.IdleMillis > 0 {
		lastMessageTime = time.Now()
	}

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			logDebug("ConsumerManager.worker", map[string]interface{}{
				"workerId":       workerID,
				"status":         "cancelled",
				"processedCount": processedCount,
			})
			return ctx.Err()
		default:
		}

		// Check limit
		if opts.Limit > 0 && processedCount >= opts.Limit {
			logDebug("ConsumerManager.worker", map[string]interface{}{
				"workerId":       workerID,
				"status":         "limit-reached",
				"processedCount": processedCount,
				"limit":          opts.Limit,
			})
			return nil
		}

		// Check idle timeout
		if opts.IdleMillis > 0 && !lastMessageTime.IsZero() {
			idleTime := time.Since(lastMessageTime).Milliseconds()
			if idleTime >= int64(opts.IdleMillis) {
				logDebug("ConsumerManager.worker", map[string]interface{}{
					"workerId":       workerID,
					"status":         "idle-timeout",
					"processedCount": processedCount,
					"idleTime":       idleTime,
				})
				return nil
			}
		}

		// Pop messages
		clientTimeout := opts.TimeoutMillis
		if opts.Wait {
			clientTimeout += 5000 // Add 5s buffer for long polling
		}

		fullPath := path
		if baseParams != "" {
			fullPath += "?" + baseParams
		}

		// wait=true is a long-poll: mark it so a 429 backs off and keeps
		// waiting instead of giving up after the bounded push-like budget.
		var getOpts []RequestOption
		if opts.Wait {
			getOpts = append(getOpts, WithLongPollRetry())
		}

		result, err := cm.httpClient.Get(ctx, fullPath, clientTimeout, affinityKey, getOpts...)
		if err != nil {
			// Check if context was cancelled
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Check if this is a timeout error (expected for long polling)
			if isTimeoutError(err) && opts.Wait {
				continue // Retry on timeout
			}

			// 429 (rate limited): HttpClient already retries this internally
			// with backoff (unbounded for wait=true pop, per the retry429
			// policy) -- this branch is a defensive fallback for the case
			// where an explicit Retry429Config.MaxAttempts override got
			// exhausted. Back off and keep polling instead of hot-looping.
			if httpErr, ok := err.(*HTTPError); ok && httpErr.StatusCode == 429 {
				delay := time.Second
				if httpErr.RetryAfterSeconds != nil {
					delay = time.Duration(*httpErr.RetryAfterSeconds * float64(time.Second))
				}
				logWarn("ConsumerManager.worker", map[string]interface{}{
					"workerId":   workerID,
					"status":     "rate-limited",
					"code":       httpErr.Code,
					"retryDelay": delay.Milliseconds(),
				})
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
					continue
				}
			}

			// Network error - wait and retry
			if isNetworkError(err) {
				logWarn("ConsumerManager.worker", map[string]interface{}{
					"workerId": workerID,
					"error":    "network",
					"message":  err.Error(),
				})
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					continue
				}
			}

			// 403 (forbidden): terminal. cluster_suspended in particular can
			// never resolve itself, and none of the other proxy codes
			// (storage_quota_exceeded / feature_gated / forbidden) are worth
			// hot-looping either -- stop this worker and surface the error
			// (with .Code) to the caller instead of retrying.
			if httpErr, ok := err.(*HTTPError); ok && httpErr.StatusCode == 403 {
				logError("ConsumerManager.worker", map[string]interface{}{
					"workerId": workerID,
					"status":   "forbidden",
					"code":     httpErr.Code,
				})
				return err
			}

			// Other errors - log and continue
			logError("ConsumerManager.worker", map[string]interface{}{
				"workerId": workerID,
				"error":    err.Error(),
			})
			continue
		}

		// Conflation echo check, BEFORE anything is parsed or handled
		// (PLAN_CONFLATION.md §4). The broker emits the echo on empty pops too,
		// so a broker that cannot conflate is caught on the first round trip and
		// this worker stops - rather than quietly processing a whole backlog
		// message by message on a consumer that asked for the newest state only.
		if cerr := checkConflationEcho(result, opts.Conflation,
			conflationTarget(opts.Queue, opts.Namespace, opts.Task), opts.Group); cerr != nil {
			logError("ConsumerManager.worker", map[string]interface{}{
				"workerId": workerID,
				"status":   "conflation-not-applied",
				"error":    cerr.Error(),
			})
			return cerr
		}

		// Parse messages
		messages := parseMessages(result)
		if len(messages) == 0 {
			if opts.Wait {
				continue // Long polling timeout, retry
			}
			// Short delay before retry -- the broker's advised pacing when this
			// pop engaged autopilot and the broker had an opinion (it knows the
			// arrival rate on this queue and this client does not), otherwise
			// the historical 100ms. Still a select on ctx, so the advice can
			// never outlive a cancellation.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(emptyPollDelay(parseAutopilotDecision(result))):
				continue
			}
		}

		logDebug("ConsumerManager.worker", map[string]interface{}{
			"workerId": workerID,
			"status":   "messages-received",
			"count":    len(messages),
		})

		// Update last message time
		if opts.IdleMillis > 0 {
			lastMessageTime = time.Now()
		}

		// Enhance messages with trace method
		cm.enhanceMessagesWithTrace(messages, opts.Group)

		// Set up lease renewal if enabled
		var renewalCancel context.CancelFunc
		if opts.RenewLease && opts.RenewLeaseIntervalMillis > 0 {
			var renewalCtx context.Context
			renewalCtx, renewalCancel = context.WithCancel(ctx)
			go cm.leaseRenewalLoop(renewalCtx, messages, opts.RenewLeaseIntervalMillis)
		}

		// Process messages
		var processErr error
		if opts.Each {
			// Process one at a time
			for i, msg := range messages {
				select {
				case <-ctx.Done():
					if renewalCancel != nil {
						renewalCancel()
					}
					return ctx.Err()
				default:
				}

				var handledOK bool
				handledOK, processErr = cm.processMessage(ctx, msg, handler, opts.AutoAck, opts.Group)
				processedCount++

				// A nack releases the lease and clamps the server cursor at the
				// failed message: everything after it in this popped batch WILL
				// be redelivered. Processing it now would only produce
				// duplicates and rejected acks — abandon the rest of the batch.
				if opts.AutoAck && !handledOK {
					logDebug("ConsumerManager.worker", map[string]interface{}{
						"workerId":  workerID,
						"status":    "batch-abandoned-after-nack",
						"remaining": len(messages) - i - 1,
					})
					break
				}

				if opts.Limit > 0 && processedCount >= opts.Limit {
					break
				}
			}
		} else {
			// Process as batch (or single message if batch=1). Under autopilot
			// opts.Batch is 0 (the broker sized this pop), so this takes the
			// batch arm even for a single message — which is the same call for
			// a one-element slice: Queen.Ack collapses a one-message batch onto
			// /api/v1/ack, and processedCount grows by the same 1.
			if opts.Batch == 1 && len(messages) == 1 {
				// For batch=1, pass single message (not array) - wrap in single-element batch
				_, processErr = cm.processMessage(ctx, messages[0], handler, opts.AutoAck, opts.Group)
				processedCount++
			} else {
				// For batch>1, pass array of messages
				processErr = cm.processBatch(ctx, messages, handler, opts.AutoAck, opts.Group)
				processedCount += len(messages)
			}
		}

		// Cancel renewal
		if renewalCancel != nil {
			renewalCancel()
		}

		if processErr != nil && !opts.AutoAck {
			// If auto-ack is disabled and handler failed, propagate error
			return processErr
		}

		logDebug("ConsumerManager.worker", map[string]interface{}{
			"workerId":       workerID,
			"status":         "messages-processed",
			"count":          len(messages),
			"totalProcessed": processedCount,
		})
	}
}

// processMessage processes a single message. The bool result reports whether
// the message was handled (and acked) successfully; false means it was nacked
// and the caller must abandon the rest of the popped batch (the nack released
// the lease server-side).
func (cm *ConsumerManager) processMessage(ctx context.Context, msg *Message, handler BatchMessageHandler, autoAck bool, group string) (bool, error) {
	err := handler(ctx, []*Message{msg})

	if autoAck {
		ackOpts := AckOptions{}
		if group != "" {
			ackOpts.ConsumerGroup = group
		}

		if err != nil {
			// Auto-nack on error
			ackOpts.Error = err.Error()
			res, ackErr := cm.queen.Ack(ctx, msg, false, ackOpts)
			if ackErr != nil {
				logError("ConsumerManager.processMessage", map[string]interface{}{
					"transactionId": msg.TransactionID,
					"error":         ackErr.Error(),
					"status":        "nack-failed",
				})
			} else if len(res) > 0 && !res[0].Success {
				logError("ConsumerManager.processMessage", map[string]interface{}{
					"transactionId": msg.TransactionID,
					"error":         res[0].Error,
					"status":        "nack-rejected",
				})
			} else {
				logDebug("ConsumerManager.processMessage", map[string]interface{}{
					"transactionId": msg.TransactionID,
					"status":        "nacked",
				})
			}
			// Don't propagate error when autoAck is enabled
			return false, nil
		}

		// Auto-ack on success
		res, ackErr := cm.queen.Ack(ctx, msg, true, ackOpts)
		if ackErr != nil {
			logError("ConsumerManager.processMessage", map[string]interface{}{
				"transactionId": msg.TransactionID,
				"error":         ackErr.Error(),
				"status":        "ack-failed",
			})
		} else if len(res) > 0 && !res[0].Success {
			logError("ConsumerManager.processMessage", map[string]interface{}{
				"transactionId": msg.TransactionID,
				"error":         res[0].Error,
				"status":        "ack-rejected",
			})
		} else {
			logDebug("ConsumerManager.processMessage", map[string]interface{}{
				"transactionId": msg.TransactionID,
				"status":        "acked",
			})
		}
		return true, nil
	}

	return err == nil, err
}

// processBatch processes a batch of messages.
func (cm *ConsumerManager) processBatch(ctx context.Context, msgs []*Message, handler BatchMessageHandler, autoAck bool, group string) error {
	err := handler(ctx, msgs)

	if autoAck {
		ackOpts := AckOptions{}
		if group != "" {
			ackOpts.ConsumerGroup = group
		}

		if err != nil {
			// Auto-nack on error
			ackOpts.Error = err.Error()
			_, ackErr := cm.queen.Ack(ctx, msgs, false, ackOpts)
			if ackErr != nil {
				logError("ConsumerManager.processBatch", map[string]interface{}{
					"count":  len(msgs),
					"error":  ackErr.Error(),
					"status": "nack-failed",
				})
			} else {
				logDebug("ConsumerManager.processBatch", map[string]interface{}{
					"count":  len(msgs),
					"status": "nacked",
				})
			}
			return nil
		}

		// Auto-ack on success
		res, ackErr := cm.queen.Ack(ctx, msgs, true, ackOpts)
		if ackErr != nil {
			logError("ConsumerManager.processBatch", map[string]interface{}{
				"count":  len(msgs),
				"error":  ackErr.Error(),
				"status": "ack-failed",
			})
		} else {
			rejected := 0
			for _, r := range res {
				if !r.Success {
					rejected++
				}
			}
			if rejected > 0 {
				logError("ConsumerManager.processBatch", map[string]interface{}{
					"count":    len(msgs),
					"rejected": rejected,
					"status":   "ack-rejected",
				})
			} else {
				logDebug("ConsumerManager.processBatch", map[string]interface{}{
					"count":  len(msgs),
					"status": "acked",
				})
			}
		}
		return nil
	}

	return err
}

// leaseRenewalLoop renews leases periodically.
func (cm *ConsumerManager) leaseRenewalLoop(ctx context.Context, messages []*Message, intervalMillis int) {
	ticker := time.NewTicker(time.Duration(intervalMillis) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := cm.queen.Renew(ctx, messages)
			if err != nil {
				logWarn("ConsumerManager.leaseRenewal", map[string]interface{}{
					"error": err.Error(),
				})
			}
		}
	}
}

// enhanceMessagesWithTrace adds the trace method to messages.
func (cm *ConsumerManager) enhanceMessagesWithTrace(messages []*Message, group string) {
	consumerGroup := group
	if consumerGroup == "" {
		consumerGroup = QueueModeConsumerGroup
	}

	for _, msg := range messages {
		// Create closure for this message
		msgCopy := msg
		groupCopy := consumerGroup

		msg.SetTrace(func(ctx context.Context, config TraceConfig) (*TraceResponse, error) {
			// CRITICAL: NEVER CRASH - just log and return gracefully
			defer func() {
				if r := recover(); r != nil {
					logError("ConsumerManager.trace", map[string]interface{}{
						"transactionId": msgCopy.TransactionID,
						"error":         fmt.Sprintf("panic: %v", r),
					})
				}
			}()

			// Validate required structure
			if config.Data == nil {
				logWarn("ConsumerManager.trace", map[string]interface{}{
					"error":         "Invalid trace config: requires data field",
					"transactionId": msgCopy.TransactionID,
				})
				return &TraceResponse{
					Success: false,
					Error:   "Invalid trace config: requires data field",
				}, nil
			}

			// Normalize trace names
			var traceNames []string
			if config.TraceName != "" {
				traceNames = []string{config.TraceName}
			} else if len(config.TraceNames) > 0 {
				traceNames = config.TraceNames
			}

			// Build request
			req := traceRequest{
				TransactionID: msgCopy.TransactionID,
				PartitionID:   msgCopy.PartitionID,
				ConsumerGroup: groupCopy,
				TraceNames:    traceNames,
				EventType:     config.EventType,
				Data:          config.Data,
			}

			if req.EventType == "" {
				req.EventType = "info"
			}

			// Make request
			_, err := cm.httpClient.Post(ctx, "/api/v1/traces", req)
			if err != nil {
				logError("ConsumerManager.trace", map[string]interface{}{
					"transactionId": msgCopy.TransactionID,
					"error":         err.Error(),
				})
				return &TraceResponse{
					Success: false,
					Error:   err.Error(),
				}, nil
			}

			logDebug("ConsumerManager.trace", map[string]interface{}{
				"transactionId": msgCopy.TransactionID,
				"success":       true,
				"traceNames":    traceNames,
			})

			return &TraceResponse{Success: true}, nil
		})
	}
}

// buildPath builds the pop path.
func (cm *ConsumerManager) buildPath(opts ConsumeOptions) string {
	if opts.Queue != "" {
		if opts.Partition != "" && opts.Partition != DefaultPartition {
			return fmt.Sprintf("/api/v1/pop/queue/%s/partition/%s",
				url.PathEscape(opts.Queue), url.PathEscape(opts.Partition))
		}
		return fmt.Sprintf("/api/v1/pop/queue/%s", url.PathEscape(opts.Queue))
	}

	if opts.Namespace != "" || opts.Task != "" {
		return "/api/v1/pop"
	}

	return "/api/v1/pop"
}

// buildParams builds the query parameters.
//
// THIS BUILDER IS SEPARATE FROM QueueBuilder.buildPopParams (PLAN_CONFLATION.md
// §4) — any option added here must be added there too. The batch/partitions
// half is the exception, and deliberately so: both builders hand it to
// popSizing.apply, which owns the autopilot emission rule outright so the two
// cannot drift on it.
func (cm *ConsumerManager) buildParams(opts ConsumeOptions) string {
	params := url.Values{}

	// Batch and partitions, and with them the autopilot flag. Zero means the
	// user set nothing (getConsumeOptions leaves it that way on purpose when
	// autopilot is on), which is the dimension the broker gets to choose.
	// FallbackBatch is 0 and not ConsumeDefaults.Batch because this builder has
	// always emitted opts.Batch verbatim -- defaults, when they apply at all,
	// are put there upstream -- and an autopilot-off request has to be
	// byte-identical to the pre-autopilot one.
	popSizing{
		Batch:         opts.Batch,
		MaxPartitions: opts.MaxPartitions,
		FallbackBatch: 0,
		Autopilot:     cm.autopilotEnabled(opts),
	}.apply(params)

	params.Set("wait", strconv.FormatBool(opts.Wait))
	params.Set("timeout", strconv.Itoa(opts.TimeoutMillis))

	if opts.Group != "" {
		params.Set("consumerGroup", opts.Group)
	}
	if opts.SubscriptionMode != "" {
		params.Set("subscriptionMode", opts.SubscriptionMode)
	}
	if opts.SubscriptionFrom != "" {
		params.Set("subscriptionFrom", opts.SubscriptionFrom)
	}
	if opts.Namespace != "" {
		params.Set("namespace", opts.Namespace)
	}
	if opts.Task != "" {
		params.Set("task", opts.Task)
	}
	// Conflation: last-value delivery for this group. Emitted ONLY when true so
	// a consumer that does not opt in sends the request it sent before this
	// option existed.
	if opts.Conflation {
		params.Set("conflation", "true")
	}
	// NEVER send autoAck for consume - client always manages acking

	return params.Encode()
}

// autopilotEnabled resolves the autopilot decision for one consume: the
// caller's explicit ConsumeOptions.Autopilot if there is one, otherwise the
// client-wide default settled at New. The builder path has already resolved it
// (getConsumeOptions fills the field); the nil case is for callers that drive
// ConsumerManager with a ConsumeOptions of their own.
func (cm *ConsumerManager) autopilotEnabled(opts ConsumeOptions) bool {
	if opts.Autopilot != nil {
		return *opts.Autopilot
	}
	return cm.queen == nil || !cm.queen.autopilotOff
}

// getAffinityKey generates the affinity key for consistent routing.
func (cm *ConsumerManager) getAffinityKey(opts ConsumeOptions) string {
	if opts.Queue != "" {
		part := opts.Partition
		if part == "" {
			part = "*"
		}
		grp := opts.Group
		if grp == "" {
			grp = QueueModeConsumerGroup
		}
		return fmt.Sprintf("%s:%s:%s", opts.Queue, part, grp)
	}

	if opts.Namespace != "" || opts.Task != "" {
		ns := opts.Namespace
		if ns == "" {
			ns = "*"
		}
		task := opts.Task
		if task == "" {
			task = "*"
		}
		grp := opts.Group
		if grp == "" {
			grp = QueueModeConsumerGroup
		}
		return fmt.Sprintf("%s:%s:%s", ns, task, grp)
	}

	return ""
}
