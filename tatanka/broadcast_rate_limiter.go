package tatanka

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	tokensPerSecond  = 4.0 // 4 messages per second sustained rate
	bucketCapacity   = 8.0 // Allow bursts of up to 8 messages
	warningThreshold = 3

	abuseThreshold          = 10
	violationWindowDuration = 5 * time.Minute
)

type clientBucket struct {
	tokens         float64
	lastRefillTime time.Time
	violations     uint32
	lastViolation  time.Time
}

type rateLimitConfig struct {
	recordInfraction func(ip string, peerID peer.ID, infractionType infractionType) error
	now              func() time.Time
	log              slog.Logger
}

type broadcastRateLimiter struct {
	cfg *rateLimitConfig

	mtx           sync.RWMutex
	clientBuckets map[peer.ID]*clientBucket
}

func newBroadcastRateLimiter(cfg *rateLimitConfig) *broadcastRateLimiter {
	return &broadcastRateLimiter{
		clientBuckets: make(map[peer.ID]*clientBucket),
		cfg:           cfg,
	}
}

func (rl *broadcastRateLimiter) allowBroadcast(client peer.ID) (bool, bool) {
	rl.mtx.Lock()
	defer rl.mtx.Unlock()

	now := rl.cfg.now()

	var allowed, recordViolation bool
	bucket, exists := rl.clientBuckets[client]
	if !exists {
		bucket = &clientBucket{
			tokens:         bucketCapacity,
			lastRefillTime: now,
		}

		rl.clientBuckets[client] = bucket
		bucket.tokens--

		allowed = true
		recordViolation = false

		return allowed, recordViolation
	}

	// Refill tokens based on elapsed time
	elapsed := now.Sub(bucket.lastRefillTime).Seconds()
	tokensToAdd := elapsed * tokensPerSecond
	bucket.tokens = math.Min(bucket.tokens+tokensToAdd, bucketCapacity)
	bucket.lastRefillTime = now

	if bucket.tokens >= 1.0 {
		bucket.tokens--

		allowed = true
		recordViolation = false

		return allowed, recordViolation
	}

	// Rate limit violation
	allowed = false
	recordViolation = rl.recordViolation(bucket, now)

	return allowed, recordViolation
}

func (rl *broadcastRateLimiter) recordViolation(bucket *clientBucket, now time.Time) bool {
	if now.Sub(bucket.lastViolation) >= violationWindowDuration {
		bucket.violations = 0
	}

	bucket.violations++
	bucket.lastViolation = now

	// Only report violations after warning threshold
	return bucket.violations > uint32(warningThreshold)
}

func (rl *broadcastRateLimiter) getInfractionType(client peer.ID) infractionType {
	rl.mtx.RLock()
	defer rl.mtx.RUnlock()

	bucket, exists := rl.clientBuckets[client]
	if !exists {
		return RateLimitViolation
	}

	if bucket.violations >= abuseThreshold {
		return RateLimitAbuse
	}

	return RateLimitViolation
}

func (rl *broadcastRateLimiter) cleanup() {
	rl.mtx.Lock()
	defer rl.mtx.Unlock()

	now := rl.cfg.now()
	cutoff := now.Add(-violationWindowDuration)

	for client, bucket := range rl.clientBuckets {
		// Delete if inactive (no broadcast attempts) OR violation window fully expired
		if bucket.lastRefillTime.Before(cutoff) || !bucket.lastViolation.After(cutoff) {
			delete(rl.clientBuckets, client)
		}
	}
}

func (rl *broadcastRateLimiter) run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rl.cleanup()
		}
	}
}
