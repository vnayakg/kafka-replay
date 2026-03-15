package ratelimit

import (
	"context"

	"golang.org/x/time/rate"
)

// Limiter wraps a token bucket rate limiter.
type Limiter struct {
	limiter *rate.Limiter
}

// New creates a rate limiter. If msgsPerSec is 0, returns nil (unlimited).
func New(msgsPerSec int, burstSize int) *Limiter {
	if msgsPerSec <= 0 {
		return nil
	}
	if burstSize <= 0 {
		burstSize = msgsPerSec
	}
	return &Limiter{
		limiter: rate.NewLimiter(rate.Limit(msgsPerSec), burstSize),
	}
}

// Wait blocks until the rate limiter allows one event.
func (l *Limiter) Wait(ctx context.Context) error {
	return l.limiter.Wait(ctx)
}
