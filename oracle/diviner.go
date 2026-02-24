package oracle

import (
	"context"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
)

const (
	errBaseDelay = 10 * time.Second
	errMaxDelay  = 5 * time.Minute
)

// diviner wraps a Source and handles periodic fetching and emitting of
// price and fee rate updates.
type diviner struct {
	source             sources.Source
	log                slog.Logger
	publishUpdate      func(ctx context.Context, update *OracleUpdate) error
	onScheduleChanged  func(*OracleSnapshot)
	resetTimer         chan struct{}
	nextFetchInfo      atomic.Value // networkSchedule
	errorInfo          atomic.Value // fetchErrorInfo
	getNetworkSchedule func() networkSchedule
	errBaseDelay       time.Duration
	errMaxDelay        time.Duration
}

type fetchErrorInfo struct {
	message string
	stamp   time.Time
}

func newDiviner(
	src sources.Source,
	publishUpdate func(ctx context.Context, update *OracleUpdate) error,
	log slog.Logger,
	getNetworkSchedule func() networkSchedule,
	onScheduleChanged func(*OracleSnapshot),
) *diviner {
	return &diviner{
		source:             src,
		log:                log,
		publishUpdate:      publishUpdate,
		resetTimer:         make(chan struct{}),
		getNetworkSchedule: getNetworkSchedule,
		onScheduleChanged:  onScheduleChanged,
		errBaseDelay:       errBaseDelay,
		errMaxDelay:        errMaxDelay,
	}
}

// calcBackoff computes exponential backoff: min(baseDelay * 2^attempt, maxDelay).
func (d *diviner) calcBackoff(attempt int) time.Duration {
	delay := d.errBaseDelay * (1 << uint(attempt))
	if delay > d.errMaxDelay {
		return d.errMaxDelay
	}
	return delay
}

// fetchScheduleInfo returns the current fetch schedule info.
func (d *diviner) fetchScheduleInfo() networkSchedule {
	if v := d.nextFetchInfo.Load(); v != nil {
		return v.(networkSchedule)
	}
	return networkSchedule{}
}

func (d *diviner) fetchErrorInfo() (string, *time.Time) {
	if v := d.errorInfo.Load(); v != nil {
		info := v.(fetchErrorInfo)
		if info.message == "" {
			return "", nil
		}
		stamp := info.stamp
		return info.message, &stamp
	}
	return "", nil
}

func (d *diviner) fetchUpdates(ctx context.Context) error {
	rateInfo, err := d.source.FetchRates(ctx)
	if err != nil {
		return err
	}

	if len(rateInfo.Prices) == 0 && len(rateInfo.FeeRates) == 0 {
		return nil
	}

	update := &OracleUpdate{
		Source: d.source.Name(),
		Stamp:  time.Now(),
		Quota:  d.source.QuotaStatus(),
	}

	if len(rateInfo.Prices) > 0 {
		update.Prices = make(map[Ticker]float64, len(rateInfo.Prices))
		for _, entry := range rateInfo.Prices {
			update.Prices[Ticker(entry.Ticker)] = entry.Price
		}
	}

	if len(rateInfo.FeeRates) > 0 {
		update.FeeRates = make(map[Network]*big.Int, len(rateInfo.FeeRates))
		for _, entry := range rateInfo.FeeRates {
			update.FeeRates[Network(entry.Network)] = entry.FeeRate
		}
	}

	go func() {
		if err := d.publishUpdate(ctx, update); err != nil {
			d.log.Errorf("Failed to publish oracle update: %v", err)
		}
	}()

	return nil
}

func (d *diviner) reschedule() {
	select {
	case d.resetTimer <- struct{}{}:
	default:
	}
}

func (d *diviner) run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	var consecutiveErrors int

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.resetTimer:
			info := d.getNetworkSchedule()
			timer.Reset(time.Until(info.NextFetchTime))
			d.nextFetchInfo.Store(info)
			d.fireScheduleChanged(info)
		case <-timer.C:
			if err := d.fetchUpdates(ctx); err != nil {
				d.log.Errorf("Failed to fetch divination: %v", err)
				consecutiveErrors++
				errTime := time.Now()
				d.errorInfo.Store(fetchErrorInfo{message: err.Error(), stamp: errTime})
				info := d.fetchScheduleInfo()
				if info.NextFetchTime.IsZero() {
					info = d.getNetworkSchedule()
				}
				backoff := d.calcBackoff(consecutiveErrors - 1)
				info.NextFetchTime = errTime.Add(backoff)
				d.nextFetchInfo.Store(info)
				d.fireScheduleChanged(info)
				timer.Reset(backoff)
			} else {
				consecutiveErrors = 0
				d.errorInfo.Store(fetchErrorInfo{message: "", stamp: time.Time{}})
				info := d.getNetworkSchedule()
				timer.Reset(time.Until(info.NextFetchTime))
				d.nextFetchInfo.Store(info)
				d.fireScheduleChanged(info)
			}
		}
	}
}

func (d *diviner) fireScheduleChanged(info networkSchedule) {
	errMsg, errStamp := d.fetchErrorInfo()
	nft := info.NextFetchTime
	minPeriod := info.MinPeriod
	nsp := info.NetworkSustainablePeriod
	nnft := info.NetworkNextFetchTime
	status := &SourceStatus{
		NextFetchTime:            &nft,
		MinFetchInterval:         &minPeriod,
		NetworkSustainableRate:   &info.NetworkSustainableRate,
		NetworkSustainablePeriod: &nsp,
		NetworkNextFetchTime:     &nnft,
		OrderedNodes:             info.OrderedNodes,
	}
	if errMsg != "" && errStamp != nil {
		status.LastError = errMsg
		status.LastErrorTime = errStamp
	}
	d.onScheduleChanged(&OracleSnapshot{
		Sources: map[string]*SourceStatus{
			d.source.Name(): status,
		},
	})
}
