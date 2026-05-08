// Package helpers contains composable gate factories for rate limiting.
//
// Mirror of the JS streams/helpers/rateLimiter.js and Python
// queen/streams/helpers/rate_limiter.py. The semantics (per-key state,
// partial-ack on deny, FIFO order on lease expiry) come from the underlying
// .Gate() runtime — these factories only decide HOW each request consumes
// from the bucket.
package helpers

import (
	"github.com/smartpricing/queen/client-go/streams/operators"
)

// CostFn maps a message to its cost in tokens (default: 1 token per message).
type CostFn func(msg interface{}) float64

// TokenBucketGateOptions configures a token-bucket gate.
type TokenBucketGateOptions struct {
	Capacity      float64
	RefillPerSec  float64
	CostFn        CostFn // optional, defaults to () => 1
	AllowZeroCost bool   // defaults to true
}

// TokenBucketGate returns a GateFn suitable for Stream.Gate(...).
//
// The bucket state lives in ctx.State (which the runtime persists per-key in
// queen_streams.state on every ALLOWED message).
//
//	gateFn := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
//	    Capacity:     100,
//	    RefillPerSec: 100,
//	})
//	stream := streams.From(q.Queue("requests")).Gate(gateFn).To(q.Queue("approved"))
func TokenBucketGate(opts TokenBucketGateOptions) operators.GateFn {
	if opts.Capacity <= 0 {
		panic("token_bucket_gate: capacity must be a positive number")
	}
	if opts.RefillPerSec <= 0 {
		panic("token_bucket_gate: refill_per_sec must be a positive number")
	}
	costFn := opts.CostFn
	if costFn == nil {
		costFn = func(_ interface{}) float64 { return 1 }
	}
	allowZero := opts.AllowZeroCost
	if !allowZero { // default to true unless caller explicitly opted out
		allowZero = true
	}

	return func(value interface{}, ctx operators.GateContext) (bool, error) {
		now := ctx.StreamTimeMs

		var tokens float64
		if t, ok := ctx.State["tokens"].(float64); ok {
			tokens = t
		} else {
			tokens = opts.Capacity
		}

		var lastRefill float64
		if t, ok := ctx.State["lastRefillAt"].(float64); ok {
			lastRefill = t
		} else {
			lastRefill = float64(now)
		}

		elapsedSec := (float64(now) - lastRefill) / 1000.0
		if elapsedSec < 0 {
			elapsedSec = 0
		}
		tokens = min64(opts.Capacity, tokens+elapsedSec*opts.RefillPerSec)
		ctx.State["tokens"] = tokens
		ctx.State["lastRefillAt"] = float64(now)

		cost := costFn(value)
		if cost < 0 {
			cost = 1
		}
		if cost == 0 {
			return allowZero, nil
		}
		if tokens >= cost {
			ctx.State["tokens"] = tokens - cost
			ctx.State["allowedTotal"] = numAdd(ctx.State["allowedTotal"], 1)
			ctx.State["consumedTotal"] = numAdd(ctx.State["consumedTotal"], cost)
			return true, nil
		}
		return false, nil
	}
}

// SlidingWindowGateOptions configures a 2-bucket sliding-window gate.
type SlidingWindowGateOptions struct {
	Limit     float64
	WindowSec float64
	CostFn    CostFn
}

// SlidingWindowGate returns a GateFn that enforces a "max N events in last W
// seconds" rate using a 2-bucket approximation.
func SlidingWindowGate(opts SlidingWindowGateOptions) operators.GateFn {
	if opts.Limit <= 0 {
		panic("sliding_window_gate: limit must be a positive number")
	}
	if opts.WindowSec <= 0 {
		panic("sliding_window_gate: window_sec must be a positive number")
	}
	costFn := opts.CostFn
	if costFn == nil {
		costFn = func(_ interface{}) float64 { return 1 }
	}
	windowMs := opts.WindowSec * 1000
	return func(value interface{}, ctx operators.GateContext) (bool, error) {
		now := float64(ctx.StreamTimeMs)
		currentWindow := float64(int64(now / windowMs))
		elapsedInWindow := (now - currentWindow*windowMs) / windowMs

		stateWindow, _ := ctx.State["window"].(float64)
		if stateWindow != currentWindow {
			prevCount := float64(0)
			if stateWindow == currentWindow-1 {
				if c, ok := ctx.State["currentCount"].(float64); ok {
					prevCount = c
				}
			}
			ctx.State["previousCount"] = prevCount
			ctx.State["currentCount"] = float64(0)
			ctx.State["window"] = currentWindow
		}
		prev, _ := ctx.State["previousCount"].(float64)
		cur, _ := ctx.State["currentCount"].(float64)
		estimated := prev*(1-elapsedInWindow) + cur

		cost := costFn(value)
		if cost < 0 {
			cost = 1
		}
		if estimated+cost <= opts.Limit {
			ctx.State["currentCount"] = cur + cost
			return true, nil
		}
		return false, nil
	}
}

func min64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func numAdd(a interface{}, b float64) float64 {
	if x, ok := a.(float64); ok {
		return x + b
	}
	return b
}
