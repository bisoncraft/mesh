package oracle

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
)

// mockSource implements sources.Source for testing.
type mockSource struct {
	name      string
	weight    float64
	period    time.Duration
	minPeriod time.Duration
	quota     *sources.QuotaStatus
	fetchFunc func(ctx context.Context) (*sources.RateInfo, error)
}

func (m *mockSource) Name() string             { return m.name }
func (m *mockSource) Weight() float64          { return m.weight }
func (m *mockSource) Period() time.Duration    { return m.period }
func (m *mockSource) MinPeriod() time.Duration { return m.minPeriod }
func (m *mockSource) QuotaStatus() *sources.QuotaStatus {
	if m.quota != nil {
		return m.quota
	}
	return &sources.QuotaStatus{
		FetchesRemaining: 100,
		FetchesLimit:     100,
		ResetTime:        time.Now().Add(24 * time.Hour),
	}
}
func (m *mockSource) FetchRates(ctx context.Context) (*sources.RateInfo, error) {
	return m.fetchFunc(ctx)
}

func TestDiviner(t *testing.T) {
	tests := []struct {
		name           string
		rateInfo       *sources.RateInfo
		fetchErr       error
		quota          *sources.QuotaStatus
		expectedUpdate *OracleUpdate
		expectErrorMsg bool
	}{
		{
			name: "successful price fetch",
			quota: &sources.QuotaStatus{
				FetchesRemaining: 42,
				FetchesLimit:     100,
			},
			rateInfo: &sources.RateInfo{
				Prices: []*sources.PriceUpdate{
					{Ticker: "BTC", Price: 50000.0},
					{Ticker: "ETH", Price: 3000.0},
				},
			},
			expectedUpdate: &OracleUpdate{
				Source: "test-source",
				Prices: map[Ticker]float64{
					"BTC": 50000.0,
					"ETH": 3000.0,
				},
				Quota: &sources.QuotaStatus{
					FetchesRemaining: 42,
					FetchesLimit:     100,
				},
			},
		},
		{
			name: "successful fee rate fetch",
			quota: &sources.QuotaStatus{
				FetchesRemaining: 42,
				FetchesLimit:     100,
			},
			rateInfo: &sources.RateInfo{
				FeeRates: []*sources.FeeRateUpdate{
					{Network: "BTC", FeeRate: big.NewInt(50)},
				},
			},
			expectedUpdate: &OracleUpdate{
				Source: "test-source",
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(50),
				},
				Quota: &sources.QuotaStatus{
					FetchesRemaining: 42,
					FetchesLimit:     100,
				},
			},
		},
		{
			name: "fetch failure",
			quota: &sources.QuotaStatus{
				FetchesRemaining: 42,
				FetchesLimit:     100,
			},
			fetchErr:       fmt.Errorf("fetch error"),
			expectErrorMsg: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			log := slog.NewBackend(os.Stdout).Logger("test")

			resetTime := time.Now().Add(10 * time.Minute)
			src := &mockSource{
				name:      "test-source",
				weight:    0.8,
				period:    5 * time.Minute,
				minPeriod: 30 * time.Second,
				quota: &sources.QuotaStatus{
					FetchesRemaining: test.quota.FetchesRemaining,
					FetchesLimit:     test.quota.FetchesLimit,
					ResetTime:        resetTime,
				},
				fetchFunc: func(ctx context.Context) (*sources.RateInfo, error) {
					if test.fetchErr != nil {
						return nil, test.fetchErr
					}
					return test.rateInfo, nil
				},
			}

			baseTime := time.Unix(0, 0)
			expectedSchedule := networkSchedule{
				NextFetchTime:            baseTime.Add(30 * time.Second),
				NetworkSustainableRate:   0.5,
				MinPeriod:                src.minPeriod,
				NetworkSustainablePeriod: 2 * time.Second,
				NetworkNextFetchTime:     baseTime.Add(2 * time.Second),
				OrderedNodes:             []string{"node-a", "node-b"},
			}
			getNetworkSchedule := func() networkSchedule {
				return expectedSchedule
			}

			updateCh := make(chan *OracleUpdate, 1)
			publishUpdate := func(ctx context.Context, update *OracleUpdate) error {
				updateCh <- update
				return nil
			}

			scheduleCh := make(chan *OracleSnapshot, 1)
			onScheduleChanged := func(update *OracleSnapshot) {
				scheduleCh <- update
			}

			div := newDiviner(src, publishUpdate, log, getNetworkSchedule, onScheduleChanged)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go div.run(ctx)

			var (
				update         *OracleUpdate
				scheduleUpdate *OracleSnapshot
			)

			deadline := time.After(10 * time.Second)
			for update == nil || scheduleUpdate == nil {
				select {
				case update = <-updateCh:
				case scheduleUpdate = <-scheduleCh:
				case <-deadline:
					t.Fatal("Timed out waiting for updates")
				}
				if test.fetchErr != nil && scheduleUpdate != nil {
					break
				}
			}

			if test.fetchErr == nil {
				if update == nil {
					t.Fatal("Expected a publish update")
				}
				expectedUpdate := cloneOracleUpdate(test.expectedUpdate)
				if expectedUpdate != nil && expectedUpdate.Quota != nil {
					expectedUpdate.Quota.ResetTime = resetTime
				}
				// The diviner sets Stamp to time.Now() at fetch time, so
				// copy the actual stamp into the expected value before
				// comparing.
				expectedUpdate.Stamp = update.Stamp
				if !reflect.DeepEqual(update, expectedUpdate) {
					t.Errorf("Expected update %+v, got %+v", expectedUpdate, update)
				}
			} else if update != nil {
				t.Fatal("Did not expect a publish update on error")
			}

			if scheduleUpdate == nil {
				t.Fatal("Expected schedule update")
			}

			srcStatus, ok := scheduleUpdate.Sources["test-source"]
			if !ok {
				t.Fatal("Expected schedule update to contain 'test-source' in Sources")
			}

			minPeriod := expectedSchedule.MinPeriod
			nsp := expectedSchedule.NetworkSustainablePeriod
			nnft := expectedSchedule.NetworkNextFetchTime
			expectedStatus := &SourceStatus{
				MinFetchInterval:         &minPeriod,
				NetworkSustainableRate:   &expectedSchedule.NetworkSustainableRate,
				NetworkSustainablePeriod: &nsp,
				NetworkNextFetchTime:     &nnft,
				OrderedNodes:             expectedSchedule.OrderedNodes,
			}
			if test.fetchErr == nil {
				nft := expectedSchedule.NextFetchTime
				expectedStatus.NextFetchTime = &nft
			} else {
				expectedStatus.NextFetchTime = srcStatus.NextFetchTime
			}
			if test.expectErrorMsg {
				expectedStatus.LastError = "fetch error"
				expectedStatus.LastErrorTime = srcStatus.LastErrorTime
			}

			if !reflect.DeepEqual(expectedStatus, srcStatus) {
				t.Fatalf("Unexpected schedule update source status: %#v", srcStatus)
			}

			if test.fetchErr != nil && srcStatus.NextFetchTime.Sub(baseTime) < 50*time.Second {
				t.Errorf("Expected retry next fetch to be ~1 minute later, got %v", srcStatus.NextFetchTime.Sub(baseTime))
			}
		})
	}
}

func cloneOracleUpdate(update *OracleUpdate) *OracleUpdate {
	if update == nil {
		return nil
	}

	clone := &OracleUpdate{
		Source: update.Source,
		Stamp:  update.Stamp,
	}

	if update.Prices != nil {
		clone.Prices = make(map[Ticker]float64, len(update.Prices))
		for k, v := range update.Prices {
			clone.Prices[k] = v
		}
	}

	if update.FeeRates != nil {
		clone.FeeRates = make(map[Network]*big.Int, len(update.FeeRates))
		for k, v := range update.FeeRates {
			clone.FeeRates[k] = new(big.Int).Set(v)
		}
	}

	if update.Quota != nil {
		q := *update.Quota
		clone.Quota = &q
	}

	return clone
}
