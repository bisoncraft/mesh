package tatanka

import (
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTokenBucketRefill(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// First broadcast is always allowed
	allowed, _ := limiter.allowBroadcast(clientID)
	if !allowed {
		t.Fatalf("First broadcast should be allowed")
	}

	// Consume remaining bucket capacity (7 tokens left)
	for i := 0; i < 7; i++ {
		allowed, _ := limiter.allowBroadcast(clientID)
		if !allowed {
			t.Fatalf("Broadcast %d should be allowed (bucket not empty)", i+2)
		}
	}

	// Now bucket is empty
	allowed, _ = limiter.allowBroadcast(clientID)
	if allowed {
		t.Fatalf("Broadcast should be denied (bucket empty)")
	}

	// Simulate 1 second passing - should get 4 new tokens
	now = now.Add(1 * time.Second)
	allowed, _ = limiter.allowBroadcast(clientID)
	if !allowed {
		t.Fatalf("Broadcast should be allowed (tokens refilled)")
	}
}

func TestTokenBucketBurstAllowance(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// Bucket capacity is 8, so 8 broadcasts should be allowed immediately
	for i := 0; i < 8; i++ {
		allowed, _ := limiter.allowBroadcast(clientID)
		if !allowed {
			t.Fatalf("Broadcast %d should be allowed (burst capacity = 8)", i+1)
		}
	}

	// 9th should be denied
	allowed, _ := limiter.allowBroadcast(clientID)
	if allowed {
		t.Fatalf("9th broadcast should be denied (bucket capacity exceeded)")
	}
}

func TestTokenBucketRateEnforcement(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// Consume full bucket
	for i := 0; i < 8; i++ {
		limiter.allowBroadcast(clientID)
	}

	// Try immediately - should fail
	allowed, _ := limiter.allowBroadcast(clientID)
	if allowed {
		t.Fatalf("Should be rate limited immediately")
	}

	// After 100ms (0.4 tokens), should still fail
	now = now.Add(100 * time.Millisecond)
	allowed, _ = limiter.allowBroadcast(clientID)
	if allowed {
		t.Fatalf("Should be rate limited at 100ms")
	}

	// After 300ms (1.2 tokens), should succeed
	now = now.Add(200 * time.Millisecond)
	allowed, _ = limiter.allowBroadcast(clientID)
	if !allowed {
		t.Fatalf("Should be allowed at 300ms (1+ token available)")
	}
}

func TestViolationCounting(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// Consume full bucket
	for i := 0; i < 8; i++ {
		limiter.allowBroadcast(clientID)
	}

	// First 3 violations should not record infractions (warning threshold = 3)
	for i := 0; i < 3; i++ {
		_, shouldRecord := limiter.allowBroadcast(clientID)
		if shouldRecord {
			t.Fatalf("Violation %d should not record infraction yet", i+1)
		}
	}

	// 4th violation should record infraction
	_, shouldRecord := limiter.allowBroadcast(clientID)
	if !shouldRecord {
		t.Fatalf("Violation 4 should record infraction")
	}
}

func TestViolationWindowExpiry(t *testing.T) {
	currentTime := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return currentTime },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// Consume full bucket and record violations
	for i := 0; i < 8; i++ {
		limiter.allowBroadcast(clientID)
	}

	// Record 5 violations (keep consuming, tokens won't refill yet)
	for i := 0; i < 5; i++ {
		limiter.allowBroadcast(clientID)
	}

	// Verify violations are recorded before window expiry
	limiter.mtx.RLock()
	bucket := limiter.clientBuckets[clientID]
	limiter.mtx.RUnlock()

	if bucket.violations != 5 {
		t.Fatalf("Expected 5 violations before window expiry, got %d", bucket.violations)
	}

	// Move past violation window - time moves enough that window expires but not enough to refill many tokens
	currentTime = currentTime.Add(violationWindowDuration + 100*time.Millisecond)

	// Consume bucket again (more violations after window expires)
	// These should trigger a new window with reset violations
	for i := 0; i < 8; i++ {
		limiter.allowBroadcast(clientID)
	}

	// Record one more violation after window expiry
	limiter.allowBroadcast(clientID)

	limiter.mtx.RLock()
	bucket = limiter.clientBuckets[clientID]
	limiter.mtx.RUnlock()

	// After window expires, violations should have been reset and then incremented from abuse
	// Should be between 1 and 2 depending on exact timing
	if bucket.violations < 1 || bucket.violations > 2 {
		t.Fatalf("Expected violations between 1-2 after window expiry, got %d", bucket.violations)
	}
}

func TestViolationAbuseThreshold(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// Consume full bucket
	for i := 0; i < 8; i++ {
		limiter.allowBroadcast(clientID)
	}

	// Record violations to reach severe abuse threshold (10 violations)
	for i := 0; i < 10; i++ {
		limiter.allowBroadcast(clientID)
	}

	// Check the infraction type
	infractionType := limiter.getInfractionType(clientID)
	if infractionType != RateLimitAbuse {
		t.Fatalf("Expected SevereRateLimitAbuse at 10 violations, got %v", infractionType)
	}

	// Test that at 9 violations it's still normal violation
	now = now.Add(violationWindowDuration + 1*time.Second)
	limiter.allowBroadcast(clientID) // Reset violations

	// Consume full bucket again
	now = time.Now() // Reset time to prevent accumulation
	cfg.now = func() time.Time { return now }
	limiter2 := newBroadcastRateLimiter(cfg)

	for i := 0; i < 8; i++ {
		limiter2.allowBroadcast(clientID)
	}

	// Record 9 violations
	for i := 0; i < 9; i++ {
		limiter2.allowBroadcast(clientID)
	}

	infractionType = limiter2.getInfractionType(clientID)
	if infractionType != RateLimitViolation {
		t.Fatalf("Expected RateLimitViolation at 9 violations, got %v", infractionType)
	}
}

func TestNewClientFirstBroadcast(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// First broadcast for any client should be allowed
	allowed, shouldRecord := limiter.allowBroadcast(clientID)
	if !allowed {
		t.Fatalf("First broadcast for new client should be allowed")
	}
	if shouldRecord {
		t.Fatalf("First broadcast should not record infraction")
	}
}

func TestConcurrentBroadcasts(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	clientID := generateTestPeerID(t)

	// This test should be run with go test -race to detect race conditions
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				limiter.allowBroadcast(clientID)
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestMultipleIndependentClients(t *testing.T) {
	now := time.Now()
	cfg := &rateLimitConfig{
		recordInfraction: func(ip string, peerID peer.ID, it infractionType) error {
			return nil
		},
		now: func() time.Time { return now },
		log: slog.Disabled,
	}

	limiter := newBroadcastRateLimiter(cfg)
	client1 := generateTestPeerID(t)
	client2 := generateTestPeerID(t)

	// Consume client1's bucket
	for i := 0; i < 8; i++ {
		limiter.allowBroadcast(client1)
	}

	// client1 should be rate limited
	allowed, _ := limiter.allowBroadcast(client1)
	if allowed {
		t.Fatalf("Client1 should be rate limited")
	}

	// client2 should still be allowed
	allowed, _ = limiter.allowBroadcast(client2)
	if !allowed {
		t.Fatalf("Client2 should not be rate limited")
	}
}

