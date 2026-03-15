package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestNew_ZeroDisabled(t *testing.T) {
	l := New(0, 0)
	if l != nil {
		t.Error("expected nil limiter for rate=0")
	}
}

func TestNew_NegativeDisabled(t *testing.T) {
	l := New(-1, 0)
	if l != nil {
		t.Error("expected nil limiter for negative rate")
	}
}

func TestLimiter_Wait(t *testing.T) {
	l := New(1000, 100) // 1000 msgs/sec
	if l == nil {
		t.Fatal("expected non-nil limiter")
	}

	ctx := context.Background()
	start := time.Now()

	// Should be near-instant for first burst
	for i := 0; i < 100; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatalf("Wait error: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Errorf("burst of 100 should be fast, took %v", elapsed)
	}
}

func TestLimiter_WaitCancelled(t *testing.T) {
	l := New(1, 1) // very slow: 1 msg/sec
	if l == nil {
		t.Fatal("expected non-nil limiter")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Use up the burst
	_ = l.Wait(ctx)

	// Cancel context then try to wait
	cancel()
	err := l.Wait(ctx)
	if err == nil {
		t.Error("expected error on cancelled context")
	}
}
