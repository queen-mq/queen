package queen

import (
	"context"
	"sync"
	"time"
)

// MessageBuffer is a buffer for messages to a specific queue/partition.
type MessageBuffer struct {
	key             string
	config          BufferConfig
	flushFn         FlushFunc
	onFlushComplete func()

	mu              sync.Mutex
	// notFull is signalled by the flusher every time it drains a batch, so
	// Adds blocked on the MaxSize backpressure bound can re-check. It shares
	// mu, which Wait releases while parked.
	notFull         *sync.Cond
	items           []PushItem
	firstAddTime    time.Time
	timer           *time.Timer
	flushing        bool
	stopped         bool
}

// bufferStats contains internal buffer statistics.
type bufferStats struct {
	count int
	age   float64
}

// NewMessageBuffer creates a new message buffer.
func NewMessageBuffer(key string, config BufferConfig, flushFn FlushFunc, onFlushComplete func()) *MessageBuffer {
	// Apply defaults
	if config.MessageCount == 0 {
		config.MessageCount = BufferDefaults.MessageCount
	}
	if config.TimeMillis == 0 {
		config.TimeMillis = BufferDefaults.TimeMillis
	}
	// MaxSize 0 = bounded DEFAULT, not unbounded: the unbounded buffer was
	// the defect (see BufferConfig.MaxSize), so opting out of backpressure
	// is not expressible. The floor keeps MaxSize sane when a caller sets a
	// MessageCount above their MaxSize.
	if config.MaxSize <= 0 {
		config.MaxSize = 4 * config.MessageCount
	}
	if config.MaxSize < config.MessageCount {
		config.MaxSize = config.MessageCount
	}
	if config.RetryDelayMillis <= 0 {
		config.RetryDelayMillis = BufferDefaults.RetryDelayMillis
	}

	mb := &MessageBuffer{
		key:             key,
		config:          config,
		flushFn:         flushFn,
		onFlushComplete: onFlushComplete,
		items:           make([]PushItem, 0),
	}
	mb.notFull = sync.NewCond(&mb.mu)
	return mb
}

// Add adds items to the buffer.
// Triggers flush if count threshold is reached or timer expires.
//
// BACKPRESSURE: once the buffer holds MaxSize messages, Add blocks until the
// flusher drains below the bound or ctx is cancelled. Without this, a producer
// that fills faster than the flush pipeline drains grows process memory
// without limit and every unflushed message dies with the process — measured
// as 20.9M messages lost in 45s at 1.46M adds/s vs 1.0M flush/s, with zero
// errors reported anywhere. Blocking the producer at the bound is the sound
// behavior: the send rate degrades to the drain rate instead of lying.
func (mb *MessageBuffer) Add(ctx context.Context, items []PushItem) error {
	// Wake blocked Adds when the caller's context dies, so cancellation is
	// honored even while parked on the condition variable.
	stopWake := context.AfterFunc(ctx, func() {
		mb.mu.Lock()
		mb.mu.Unlock()
		mb.notFull.Broadcast()
	})
	defer stopWake()

	mb.mu.Lock()

	for len(mb.items) >= mb.config.MaxSize && !mb.stopped {
		if ctx.Err() != nil {
			mb.mu.Unlock()
			return ctx.Err()
		}
		// A blocked Add means producers outran the flusher; make sure one is
		// actually running (the timer may be far away) before parking.
		if !mb.flushing {
			go func() {
				if err := mb.Flush(context.Background()); err != nil {
					logError("MessageBuffer.backpressureFlush", map[string]interface{}{
						"key": mb.key, "error": err.Error(),
					})
				}
			}()
		}
		mb.notFull.Wait()
	}

	if mb.stopped {
		mb.mu.Unlock()
		return nil
	}

	// Record first add time if this is the first item
	if len(mb.items) == 0 {
		mb.firstAddTime = time.Now()
		// Start timer for time-based flush
		mb.startTimer(ctx)
	}

	// Add items
	mb.items = append(mb.items, items...)

	// Check if we need to flush (count threshold)
	shouldFlush := len(mb.items) >= mb.config.MessageCount
	mb.mu.Unlock()

	if shouldFlush {
		return mb.Flush(ctx)
	}

	return nil
}

// startTimer starts the time-based flush timer.
func (mb *MessageBuffer) startTimer(ctx context.Context) {
	if mb.timer != nil {
		mb.timer.Stop()
	}

	mb.timer = time.AfterFunc(time.Duration(mb.config.TimeMillis)*time.Millisecond, func() {
		mb.mu.Lock()
		if mb.stopped || len(mb.items) == 0 {
			mb.mu.Unlock()
			return
		}
		mb.mu.Unlock()

		// Use background context for timer-triggered flush
		if err := mb.Flush(context.Background()); err != nil {
			logError("MessageBuffer.timer", map[string]interface{}{
				"key":   mb.key,
				"error": err.Error(),
			})
		}
	})
}

// Flush flushes all items in the buffer.
func (mb *MessageBuffer) Flush(ctx context.Context) error {
	mb.mu.Lock()

	// Check if already flushing or empty
	if mb.flushing || len(mb.items) == 0 {
		mb.mu.Unlock()
		return nil
	}

	mb.flushing = true
	
	// Stop timer
	if mb.timer != nil {
		mb.timer.Stop()
		mb.timer = nil
	}

	// Extract items in batches. A batch that fails to send is RE-QUEUED at
	// the front and retried after RetryDelayMillis — never dropped. Before
	// this, the batch was sliced off before the send and an error only
	// logged: up to MessageCount messages silently vanished per failed POST.
	// Combined with the MaxSize bound in Add, a broker outage now means
	// blocked producers and bounded memory instead of silent loss.
	var lastErr error
	for len(mb.items) > 0 && !mb.stopped {
		// Get batch — copied out, because re-queueing an aliased slice after
		// mb.items has been re-sliced would corrupt the backing array.
		batchSize := mb.config.MessageCount
		if batchSize > len(mb.items) {
			batchSize = len(mb.items)
		}
		batch := make([]PushItem, batchSize)
		copy(batch, mb.items[:batchSize])
		mb.items = mb.items[batchSize:]

		// Release lock during flush
		mb.mu.Unlock()

		if err := mb.flushFn(ctx, batch); err != nil {
			logError("MessageBuffer.Flush", map[string]interface{}{
				"key":   mb.key,
				"count": len(batch),
				"error": err.Error(),
			})
			lastErr = err

			// Put the batch back at the head (ordering preserved), then wait
			// out the retry delay OUTSIDE the lock. Occupancy may overshoot
			// MaxSize by this one batch — documented on the config.
			mb.mu.Lock()
			mb.items = append(batch, mb.items...)
			mb.mu.Unlock()

			select {
			case <-ctx.Done():
				mb.mu.Lock()
				mb.flushing = false
				mb.mu.Unlock()
				mb.notFull.Broadcast()
				return ctx.Err()
			case <-time.After(time.Duration(mb.config.RetryDelayMillis) * time.Millisecond):
			}
			mb.mu.Lock()
			continue
		}

		logDebug("MessageBuffer.Flush", map[string]interface{}{
			"key":   mb.key,
			"count": len(batch),
		})
		if mb.onFlushComplete != nil {
			mb.onFlushComplete()
		}
		// Capacity freed: wake producers parked on the MaxSize bound.
		mb.notFull.Broadcast()

		// Reacquire lock
		mb.mu.Lock()
	}

	mb.flushing = false
	mb.firstAddTime = time.Time{}
	mb.mu.Unlock()
	mb.notFull.Broadcast()

	return lastErr
}

// GetStats returns buffer statistics.
func (mb *MessageBuffer) GetStats() bufferStats {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	stats := bufferStats{
		count: len(mb.items),
	}

	if !mb.firstAddTime.IsZero() {
		stats.age = time.Since(mb.firstAddTime).Seconds()
	}

	return stats
}

// Stop stops the buffer and prevents further operations. Wakes any Adds
// parked on the backpressure bound so they return instead of hanging.
func (mb *MessageBuffer) Stop() {
	mb.mu.Lock()
	mb.stopped = true
	if mb.timer != nil {
		mb.timer.Stop()
		mb.timer = nil
	}
	mb.mu.Unlock()
	mb.notFull.Broadcast()
}

// Count returns the number of items in the buffer.
func (mb *MessageBuffer) Count() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.items)
}
