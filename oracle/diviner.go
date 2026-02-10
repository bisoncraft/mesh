package oracle

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/tatanka/pb"
)

// diviner wraps a Source and handles periodic fetching and emitting of
// price and fee rate updates.
type diviner struct {
	source        sources.Source
	log           slog.Logger
	publishUpdate func(ctx context.Context, update *pb.NodeOracleUpdate) error
	resetTimer    chan struct{}
}

func newDiviner(src sources.Source, publishUpdate func(ctx context.Context, update *pb.NodeOracleUpdate) error, log slog.Logger) *diviner {
	return &diviner{
		source:        src,
		log:           log,
		publishUpdate: publishUpdate,
		resetTimer:    make(chan struct{}),
	}
}

func (d *diviner) fetchUpdates(ctx context.Context) error {
	rateInfo, err := d.source.FetchRates(ctx)
	if err != nil {
		return err
	}

	now := time.Now()

	if len(rateInfo.Prices) > 0 {
		prices := make([]*SourcedPrice, 0, len(rateInfo.Prices))
		for _, entry := range rateInfo.Prices {
			prices = append(prices, &SourcedPrice{
				Ticker: Ticker(entry.Ticker),
				Price:  entry.Price,
			})
		}

		sourcedUpdate := &SourcedPriceUpdate{
			Source: d.source.Name(),
			Stamp:  now,
			Weight: d.source.Weight(),
			Prices: prices,
		}

		payload := pbNodePriceUpdate(sourcedUpdate)
		go func() {
			err := d.publishUpdate(ctx, payload)
			if err != nil {
				d.log.Errorf("Failed to publish sourced price update: %v", err)
			}
		}()
	}

	if len(rateInfo.FeeRates) > 0 {
		feeRates := make([]*SourcedFeeRate, 0, len(rateInfo.FeeRates))
		for _, entry := range rateInfo.FeeRates {
			feeRates = append(feeRates, &SourcedFeeRate{
				Network: Network(entry.Network),
				FeeRate: bigIntToBytes(entry.FeeRate),
			})
		}

		sourcedUpdate := &SourcedFeeRateUpdate{
			Source:   d.source.Name(),
			Stamp:    now,
			Weight:   d.source.Weight(),
			FeeRates: feeRates,
		}

		payload := pbNodeFeeRateUpdate(sourcedUpdate)
		go func() {
			err := d.publishUpdate(ctx, payload)
			if err != nil {
				d.log.Errorf("Failed to publish sourced fee rate update: %v", err)
			}
		}()
	}

	if len(rateInfo.Prices) == 0 && len(rateInfo.FeeRates) == 0 {
		return fmt.Errorf("source %q returned empty rate info", d.source.Name())
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
	period := d.source.MinPeriod()
	errPeriod := time.Minute
	timer := time.NewTimer(initialPeriod + delay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.resetTimer:
			timer.Reset(period)
		case <-timer.C:
			if err := d.fetchUpdates(ctx); err != nil {
				d.log.Errorf("Failed to fetch divination: %v", err)
				timer.Reset(errPeriod)
			} else {
				timer.Reset(period)
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
