package oracle

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/tatanka/pb"
)

// fetcher returns either a list of price updates or a list of fee rate updates.
type fetcher func(ctx context.Context) (any, error)

// diviner wraps an httpSource and handles periodic fetching and emitting of
// price and fee rate updates.
type diviner struct {
	name          string
	fetcher       func(ctx context.Context) (any, error)
	weight        float64
	period        time.Duration
	errPeriod     time.Duration
	log           slog.Logger
	publishUpdate func(ctx context.Context, update *pb.NodeOracleUpdate) error
	resetTimer    chan struct{}
}

func newDiviner(name string, fetcher fetcher, weight float64, period time.Duration, errPeriod time.Duration, publishUpdate func(ctx context.Context, update *pb.NodeOracleUpdate) error, log slog.Logger) *diviner {
	return &diviner{
		name:          name,
		fetcher:       fetcher,
		weight:        weight,
		period:        period,
		errPeriod:     errPeriod,
		log:           log,
		publishUpdate: publishUpdate,
		resetTimer:    make(chan struct{}),
	}
}

func (d *diviner) fetchUpdates(ctx context.Context) error {
	divination, err := d.fetcher(ctx)
	if err != nil {
		return err
	}

	now := time.Now()

	switch updates := divination.(type) {
	case []*priceUpdate:
		prices := make([]*SourcedPrice, 0, len(updates))
		for _, entry := range updates {
			prices = append(prices, &SourcedPrice{
				Ticker: entry.ticker,
				Price:  entry.price,
			})
		}

		sourcedUpdate := &SourcedPriceUpdate{
			Source: d.name,
			Stamp:  now,
			Weight: d.weight,
			Prices: prices,
		}

		payload := pbNodePriceUpdate(sourcedUpdate)
		go func() {
			err := d.publishUpdate(ctx, payload)
			if err != nil {
				d.log.Errorf("Failed to publish sourced price update: %v", err)
			}
		}()

	case []*feeRateUpdate:
		feeRates := make([]*SourcedFeeRate, 0, len(updates))
		for _, entry := range updates {
			feeRates = append(feeRates, &SourcedFeeRate{
				Network: entry.network,
				FeeRate: bigIntToBytes(entry.feeRate),
			})
		}

		sourcedUpdate := &SourcedFeeRateUpdate{
			Source:   d.name,
			Stamp:    now,
			Weight:   d.weight,
			FeeRates: feeRates,
		}

		payload := pbNodeFeeRateUpdate(sourcedUpdate)
		go func() {
			err := d.publishUpdate(ctx, payload)
			if err != nil {
				d.log.Errorf("Failed to publish sourced fee rate update: %v", err)
			}
		}()
	default:
		return fmt.Errorf("source %q returned unexpected type %T", d.name, divination)
	}

	return nil
}

func (d *diviner) reschedule() {
	select {
	case d.resetTimer <- struct{}{}:
	default:
	}
}

func (d *diviner) run(ctx context.Context) {
	// Initialize with a shorter period to fetch initial oracle updates.
	initialPeriod := time.Second * 5
	delay := randomDelay(time.Second)
	timer := time.NewTimer(initialPeriod + delay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.resetTimer:
			timer.Reset(d.period)
		case <-timer.C:
			if err := d.fetchUpdates(ctx); err != nil {
				d.log.Errorf("Failed to fetch divination: %v", err)
				timer.Reset(d.errPeriod)
			} else {
				timer.Reset(d.period)
			}
		}
	}
}

func randomDelay(maxDelay time.Duration) time.Duration {
	return time.Duration(math.Round((rand.Float64() * float64(maxDelay))))
}

// --- Protobuf Helper Functions ---

func pbNodePriceUpdate(update *SourcedPriceUpdate) *pb.NodeOracleUpdate {
	pbPrices := make([]*pb.SourcedPrice, len(update.Prices))
	for i, p := range update.Prices {
		pbPrices[i] = &pb.SourcedPrice{
			Ticker: string(p.Ticker),
			Price:  p.Price,
		}
	}
	return &pb.NodeOracleUpdate{
		Update: &pb.NodeOracleUpdate_PriceUpdate{
			PriceUpdate: &pb.SourcedPriceUpdate{
				Source:    update.Source,
				Timestamp: update.Stamp.Unix(),
				Prices:    pbPrices,
			},
		},
	}
}

func pbNodeFeeRateUpdate(update *SourcedFeeRateUpdate) *pb.NodeOracleUpdate {
	pbFeeRates := make([]*pb.SourcedFeeRate, len(update.FeeRates))
	for i, fr := range update.FeeRates {
		pbFeeRates[i] = &pb.SourcedFeeRate{
			Network: string(fr.Network),
			FeeRate: fr.FeeRate,
		}
	}
	return &pb.NodeOracleUpdate{
		Update: &pb.NodeOracleUpdate_FeeRateUpdate{
			FeeRateUpdate: &pb.SourcedFeeRateUpdate{
				Source:    update.Source,
				Timestamp: update.Stamp.Unix(),
				FeeRates:  pbFeeRates,
			},
		},
	}
}
