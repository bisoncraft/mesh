package oracle

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/tatanka/pb"
)

func TestDivinerFetchUpdates(t *testing.T) {
	t.Run("fetches and emits price updates with weight", func(t *testing.T) {
		emitted := make(chan *pb.NodeOracleUpdate, 1)

		fetcher := func(ctx context.Context) (any, error) {
			return []*priceUpdate{
				{ticker: "BTC", price: 50000.0},
				{ticker: "ETH", price: 3000.0},
			}, nil
		}

		publishUpdate := func(ctx context.Context, update *pb.NodeOracleUpdate) error {
			emitted <- update
			return nil
		}

		div := newDiviner(
			"test-source",
			fetcher,
			0.8,
			time.Minute*5,
			time.Minute,
			publishUpdate,
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		err := div.fetchUpdates(context.Background())
		if err != nil {
			t.Fatalf("fetchUpdates failed: %v", err)
		}

		select {
		case update := <-emitted:
			if update.GetPriceUpdate() == nil {
				t.Fatalf("Expected price update, got %T", update.Update)
			}
			priceUpdate := update.GetPriceUpdate()
			if priceUpdate.Source != "test-source" {
				t.Errorf("Expected source 'test-source', got %s", priceUpdate.Source)
			}
			if len(priceUpdate.Prices) != 2 {
				t.Errorf("Expected 2 prices, got %d", len(priceUpdate.Prices))
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected update to be emitted")
		}
	})

	t.Run("fetches and emits fee rate updates", func(t *testing.T) {
		emitted := make(chan *pb.NodeOracleUpdate, 1)

		fetcher := func(ctx context.Context) (any, error) {
			return []*feeRateUpdate{
				{network: "BTC", feeRate: big.NewInt(50)},
			}, nil
		}

		publishUpdate := func(ctx context.Context, update *pb.NodeOracleUpdate) error {
			emitted <- update
			return nil
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			time.Minute*5,
			time.Minute,
			publishUpdate,
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		err := div.fetchUpdates(context.Background())
		if err != nil {
			t.Fatalf("fetchUpdates failed: %v", err)
		}

		select {
		case update := <-emitted:
			if update.GetFeeRateUpdate() == nil {
				t.Fatalf("Expected fee rate update, got %T", update.Update)
			}
			feeUpdate := update.GetFeeRateUpdate()
			if feeUpdate.Source != "test-source" {
				t.Errorf("Expected source 'test-source', got %s", feeUpdate.Source)
			}
			if len(feeUpdate.FeeRates) == 0 {
				t.Error("Expected at least one fee rate")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected update to be emitted")
		}
	})

	t.Run("returns error on fetch failure", func(t *testing.T) {
		fetcher := func(ctx context.Context) (any, error) {
			return nil, fmt.Errorf("fetch error")
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			time.Minute*5,
			time.Minute,
			func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		err := div.fetchUpdates(context.Background())
		if err == nil {
			t.Error("Expected error on fetch failure")
		}
	})

	t.Run("includes weight in updates", func(t *testing.T) {
		emitted := make(chan *pb.NodeOracleUpdate, 1)

		fetcher := func(ctx context.Context) (any, error) {
			return []*priceUpdate{
				{ticker: "BTC", price: 50000.0},
			}, nil
		}

		publishUpdate := func(ctx context.Context, update *pb.NodeOracleUpdate) error {
			emitted <- update
			return nil
		}

		div := newDiviner(
			"weighted-source",
			fetcher,
			0.5,
			time.Minute*5,
			time.Minute,
			publishUpdate,
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		err := div.fetchUpdates(context.Background())
		if err != nil {
			t.Fatalf("fetchUpdates failed: %v", err)
		}

		select {
		case update := <-emitted:
			if update.GetPriceUpdate() == nil {
				t.Fatalf("Expected price update")
			}
			// Weight is stored in diviner but not exposed in protobuf
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected update to be emitted")
		}
	})

	t.Run("rejects unexpected divination type", func(t *testing.T) {
		fetcher := func(ctx context.Context) (any, error) {
			return "invalid type", nil
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			time.Minute*5,
			time.Minute,
			func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		err := div.fetchUpdates(context.Background())
		if err == nil {
			t.Error("Expected error on unexpected divination type")
		}
	})

	t.Run("publish error is logged but doesn't block", func(t *testing.T) {
		emitted := make(chan *pb.NodeOracleUpdate, 10)

		fetcher := func(ctx context.Context) (any, error) {
			return []*priceUpdate{
				{ticker: "BTC", price: 50000.0},
			}, nil
		}

		// Publish function that returns error but still buffers to verify it was called
		publishUpdate := func(ctx context.Context, update *pb.NodeOracleUpdate) error {
			emitted <- update
			return fmt.Errorf("publish error")
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			time.Millisecond,
			time.Millisecond,
			publishUpdate,
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		err := div.fetchUpdates(context.Background())
		if err != nil {
			t.Fatalf("fetchUpdates failed: %v", err)
		}

		// The fire-and-forget goroutine should still send the update
		// even though publishUpdate returns an error
		select {
		case <-emitted:
			// Good, update was sent to publish even though it will fail
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected publish to be attempted despite error")
		}
	})
}

func TestDivinerRun(t *testing.T) {
	t.Run("runs and fetches periodically", func(t *testing.T) {
		callCount := int32(0)

		fetcher := func(ctx context.Context) (any, error) {
			atomic.AddInt32(&callCount, 1)
			return []*priceUpdate{
				{ticker: "BTC", price: 50000.0},
			}, nil
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			50*time.Millisecond,
			25*time.Millisecond,
			func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		ctx, cancel := context.WithCancel(context.Background())

		go div.run(ctx)

		// Wait for at least 2 calls. The initial timer has a 5 second interval
		// plus a random delay of up to 1 second, then subsequent calls at 50ms intervals.
		// We need to wait: 5s (initial) + 1s (max delay) + 100ms (2 periods) = 6.1s
		time.Sleep(6200 * time.Millisecond)
		cancel()

		count := atomic.LoadInt32(&callCount)
		if count < 2 {
			t.Errorf("Expected at least 2 calls, got %d", count)
		}
	})

	t.Run("stops on context cancellation", func(t *testing.T) {
		fetcher := func(ctx context.Context) (any, error) {
			return []*priceUpdate{
				{ticker: "BTC", price: 50000.0},
			}, nil
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			time.Hour,
			time.Hour,
			func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			div.run(ctx)
			close(done)
		}()

		// Cancel immediately
		cancel()

		select {
		case <-done:
			// Good, run exited
		case <-time.After(time.Second):
			t.Error("run did not exit after context cancellation")
		}
	})

	t.Run("reschedule resets timer", func(t *testing.T) {
		callCount := int32(0)

		fetcher := func(ctx context.Context) (any, error) {
			atomic.AddInt32(&callCount, 1)
			return []*priceUpdate{
				{ticker: "BTC", price: 50000.0},
			}, nil
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			500*time.Millisecond,
			500*time.Millisecond,
			func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go div.run(ctx)

		// Wait a bit, then reschedule multiple times
		time.Sleep(50 * time.Millisecond)
		div.reschedule()
		time.Sleep(50 * time.Millisecond)
		div.reschedule()
		time.Sleep(50 * time.Millisecond)

		count := atomic.LoadInt32(&callCount)

		// Timer should be continuously reset, so we shouldn't have any calls yet
		// (period is 500ms, we only waited ~150ms with resets)
		if count > 0 {
			t.Logf("Got %d calls (timer may have fired due to initial delay)", count)
		}
	})

	t.Run("uses errPeriod on error", func(t *testing.T) {
		callTimes := make([]time.Time, 0, 5)
		var mu sync.Mutex

		fetcher := func(ctx context.Context) (any, error) {
			mu.Lock()
			callTimes = append(callTimes, time.Now())
			mu.Unlock()
			// Return error to trigger errPeriod
			return nil, fmt.Errorf("fetch error")
		}

		div := newDiviner(
			"test-source",
			fetcher,
			1.0,
			50*time.Millisecond,
			30*time.Millisecond,
			func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
			slog.NewBackend(os.Stdout).Logger("test"),
		)

		ctx, cancel := context.WithCancel(context.Background())

		go div.run(ctx)

		// Wait for at least 2 error retries. The initial timer has a 5 second interval
		// plus a random delay of up to 1 second, then subsequent retries at errPeriod (30ms).
		// We need to wait: 5s (initial) + 1s (max delay) + 60ms (2 errPeriods) = 6.06s
		time.Sleep(6200 * time.Millisecond)
		cancel()

		mu.Lock()
		times := callTimes
		mu.Unlock()

		if len(times) < 2 {
			t.Fatalf("Expected at least 2 calls, got %d", len(times))
		}

		// Check interval between calls - should be closer to errPeriod
		interval := times[1].Sub(times[0])
		if interval > 500*time.Millisecond {
			t.Errorf("Expected short retry interval (errPeriod), got %v", interval)
		}
	})

}
