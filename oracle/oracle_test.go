package oracle

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/bisoncraft/mesh/tatanka/pb"
)


func TestGetPrices(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	tests := []struct {
		name     string
		prices   map[Ticker]map[string]*priceUpdate
		filter   map[Ticker]bool
		expected map[Ticker]float64
	}{
		{
			name: "single source per ticker",
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
				},
				"ETH": {
					"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
				},
			},
			filter: nil,
			expected: map[Ticker]float64{
				"BTC": 50000.0,
				"ETH": 3000.0,
			},
		},
		{
			name: "multiple sources weighted average",
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
					"source2": {ticker: "BTC", price: 52000.0, stamp: now, weight: 1.0},
				},
			},
			filter: nil,
			expected: map[Ticker]float64{
				"BTC": 51000.0,
			},
		},
		{
			name: "different weights",
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 0.25},
					"source2": {ticker: "BTC", price: 52000.0, stamp: now, weight: 0.75},
				},
			},
			filter: nil,
			expected: map[Ticker]float64{
				"BTC": 51500.0,
			},
		},
		{
			name: "aged weights",
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
					"source2": {ticker: "BTC", price: 30000.0, stamp: now.Add(-validityExpiration - time.Second), weight: 1.0},
				},
			},
			filter: nil,
			expected: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "filtered tickers",
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
				},
				"ETH": {
					"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
				},
				"DCR": {
					"source1": {ticker: "DCR", price: 25.0, stamp: now, weight: 1.0},
				},
			},
			filter: map[Ticker]bool{
				"BTC": true,
				"ETH": true,
			},
			expected: map[Ticker]float64{
				"BTC": 50000.0,
				"ETH": 3000.0,
			},
		},
		{
			name: "all expired sources",
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now.Add(-validityExpiration - time.Second), weight: 1.0},
					"source2": {ticker: "BTC", price: 52000.0, stamp: now.Add(-validityExpiration - time.Second), weight: 1.0},
				},
			},
			filter:   nil,
			expected: map[Ticker]float64{},
		},
		{
			name:     "empty oracle",
			prices:   map[Ticker]map[string]*priceUpdate{},
			filter:   nil,
			expected: map[Ticker]float64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := &Oracle{
				log:    log,
				prices: tt.prices,
			}

			result := oracle.getPrices(tt.filter)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d tickers, got %d", len(tt.expected), len(result))
			}

			for ticker, expectedPrice := range tt.expected {
				actualPrice, found := result[ticker]
				if !found {
					t.Errorf("Expected ticker %s to be in result", ticker)
					continue
				}
				if actualPrice != expectedPrice {
					t.Errorf("For ticker %s, expected price %.2f, got %.2f",
						ticker, expectedPrice, actualPrice)
				}
			}

			for ticker := range result {
				if _, expected := tt.expected[ticker]; !expected {
					t.Errorf("Unexpected ticker %s in result", ticker)
				}
			}
		})
	}
}

func TestGetFeeRates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	tests := []struct {
		name     string
		feeRates map[Network]map[string]*feeRateUpdate
		filter   map[Network]bool
		expected map[Network]*big.Int
	}{
		{
			name: "single source per network",
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
				},
				"ETH": {
					"source1": {network: "ETH", feeRate: big.NewInt(200), stamp: now, weight: 1.0},
				},
			},
			filter: nil,
			expected: map[Network]*big.Int{
				"BTC": big.NewInt(100),
				"ETH": big.NewInt(200),
			},
		},
		{
			name: "multiple sources weighted average",
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
					"source2": {network: "BTC", feeRate: big.NewInt(200), stamp: now, weight: 1.0},
				},
			},
			filter: nil,
			expected: map[Network]*big.Int{
				"BTC": big.NewInt(150),
			},
		},
		{
			name: "different weights",
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 0.25},
					"source2": {network: "BTC", feeRate: big.NewInt(200), stamp: now, weight: 0.75},
				},
			},
			filter: nil,
			expected: map[Network]*big.Int{
				"BTC": big.NewInt(175),
			},
		},
		{
			name: "aged weights",
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
					"source2": {network: "BTC", feeRate: big.NewInt(200), stamp: now.Add(-validityExpiration - time.Second), weight: 1.0},
				},
			},
			filter: nil,
			expected: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "filtered networks",
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
				},
				"ETH": {
					"source1": {network: "ETH", feeRate: big.NewInt(200), stamp: now, weight: 1.0},
				},
				"DCR": {
					"source1": {network: "DCR", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
				},
			},
			filter: map[Network]bool{
				"BTC": true,
				"ETH": true,
			},
			expected: map[Network]*big.Int{
				"BTC": big.NewInt(100),
				"ETH": big.NewInt(200),
			},
		},
		{
			name: "all expired sources",
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now.Add(-validityExpiration - time.Second), weight: 1.0},
					"source2": {network: "BTC", feeRate: big.NewInt(200), stamp: now.Add(-validityExpiration - time.Second), weight: 1.0},
				},
			},
			filter:   nil,
			expected: map[Network]*big.Int{},
		},
		{
			name:     "empty oracle",
			feeRates: map[Network]map[string]*feeRateUpdate{},
			filter:   nil,
			expected: map[Network]*big.Int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := &Oracle{
				log:      log,
				feeRates: tt.feeRates,
			}

			result := oracle.getFeeRates(tt.filter)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d networks, got %d", len(tt.expected), len(result))
			}

			for network, expectedRate := range tt.expected {
				actualRate, found := result[network]
				if !found {
					t.Errorf("Expected network %s to be in result", network)
					continue
				}
				if actualRate.Cmp(expectedRate) != 0 {
					t.Errorf("For network %s, expected fee rate %s, got %s",
						network, expectedRate.String(), actualRate.String())
				}
			}

			for network := range result {
				if _, expected := tt.expected[network]; !expected {
					t.Errorf("Unexpected network %s in result", network)
				}
			}
		})
	}
}

func TestMergePrices(t *testing.T) {
	now := time.Now()
	oldStamp := now.Add(-time.Hour)
	newerStamp := now.Add(time.Hour)

	tests := []struct {
		name           string
		existingPrices map[Ticker]map[string]*priceUpdate
		sourcedUpdate  *SourcedPriceUpdate
		expectedPrices map[Ticker]map[string]*priceUpdate
		expectedResult map[Ticker]float64
	}{
		{
			name:           "new ticker from external source",
			existingPrices: map[Ticker]map[string]*priceUpdate{},
			sourcedUpdate: &SourcedPriceUpdate{
				Source: "external-oracle",
				Stamp:  now,
				Weight: 1.0,
				Prices: []*SourcedPrice{
					{Ticker: "BTC", Price: 50000.0},
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "existing ticker with newer timestamp should update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  48000.0,
						stamp:  oldStamp,
						weight: 1.0,
					},
				},
			},
			sourcedUpdate: &SourcedPriceUpdate{
				Source: "external-oracle",
				Stamp:  newerStamp,
				Weight: 1.0,
				Prices: []*SourcedPrice{
					{Ticker: "BTC", Price: 50000.0},
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "existing ticker with older timestamp should ignore",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			sourcedUpdate: &SourcedPriceUpdate{
				Source: "external-oracle",
				Stamp:  oldStamp,
				Weight: 1.0,
				Prices: []*SourcedPrice{
					{Ticker: "BTC", Price: 48000.0},
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			expectedResult: nil, // No update occurred
		},
		{
			name: "multiple prices in single update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			sourcedUpdate: &SourcedPriceUpdate{
				Source: "source2",
				Stamp:  now,
				Weight: 0.8,
				Prices: []*SourcedPrice{
					{Ticker: "BTC", Price: 51000.0},
					{Ticker: "ETH", Price: 3000.0},
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
					"source2": {
						ticker: "BTC",
						price:  51000.0,
						stamp:  now,
						weight: 0.8,
					},
				},
				"ETH": {
					"source2": {
						ticker: "ETH",
						price:  3000.0,
						stamp:  now,
						weight: 0.8,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50444.444444444445253, // (50000*1.0 + 51000*0.8) / 1.8
				"ETH": 3000.0,
			},
		},
		{
			name: "new source for existing ticker",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			sourcedUpdate: &SourcedPriceUpdate{
				Source: "source2",
				Stamp:  now,
				Weight: 1.0,
				Prices: []*SourcedPrice{
					{Ticker: "BTC", Price: 51000.0},
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
					"source2": {
						ticker: "BTC",
						price:  51000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50500.0, // (50000*1.0 + 51000*1.0) / 2.0
			},
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := &Oracle{
				log:      log,
				prices:   tt.existingPrices,
				diviners: make(map[string]*diviner),
			}

			result := oracle.MergePrices(tt.sourcedUpdate)

			// Verify the merged prices match expected
			if len(oracle.prices) != len(tt.expectedPrices) {
				t.Errorf("Expected %d tickers, got %d", len(tt.expectedPrices), len(oracle.prices))
			}

			for ticker, expectedSources := range tt.expectedPrices {
				actualSources, found := oracle.prices[ticker]
				if !found {
					t.Errorf("Expected ticker %s to be in oracle.prices", ticker)
					continue
				}

				if len(actualSources) != len(expectedSources) {
					t.Errorf("For ticker %s, expected %d sources, got %d",
						ticker, len(expectedSources), len(actualSources))
				}

				for source, expectedUpdate := range expectedSources {
					actualUpdate, found := actualSources[source]
					if !found {
						t.Errorf("Expected source %s for ticker %s", source, ticker)
						continue
					}

					if actualUpdate.price != expectedUpdate.price {
						t.Errorf("For ticker %s source %s, expected price %.2f, got %.2f",
							ticker, source, expectedUpdate.price, actualUpdate.price)
					}

					if actualUpdate.weight != expectedUpdate.weight {
						t.Errorf("For ticker %s source %s, expected weight %.2f, got %.2f",
							ticker, source, expectedUpdate.weight, actualUpdate.weight)
					}

					if !actualUpdate.stamp.Equal(expectedUpdate.stamp) {
						t.Errorf("For ticker %s source %s, expected stamp %v, got %v",
							ticker, source, expectedUpdate.stamp, actualUpdate.stamp)
					}
				}
			}

			// Verify the return value matches expected result
			if tt.expectedResult == nil {
				if len(result) > 0 {
					t.Errorf("Expected nil/empty result, got %v", result)
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
				} else {
					if len(result) != len(tt.expectedResult) {
						t.Errorf("Expected %d tickers in result, got %d", len(tt.expectedResult), len(result))
					}
					for ticker, expectedPrice := range tt.expectedResult {
						actualPrice, found := result[ticker]
						if !found {
							t.Errorf("Expected ticker %s in result", ticker)
							continue
						}
						if actualPrice != expectedPrice {
							t.Errorf("For ticker %s, expected aggregated price %.15f, got %.15f",
								ticker, expectedPrice, actualPrice)
						}
					}
					for ticker := range result {
						if _, expected := tt.expectedResult[ticker]; !expected {
							t.Errorf("Unexpected ticker %s in result", ticker)
						}
					}
				}
			}
		})
	}
}

func TestMergeFeeRates(t *testing.T) {
	now := time.Now()
	oldStamp := now.Add(-time.Hour)
	newerStamp := now.Add(time.Hour)

	tests := []struct {
		name             string
		existingFeeRates map[Network]map[string]*feeRateUpdate
		sourcedUpdate    *SourcedFeeRateUpdate
		expectedFeeRates map[Network]map[string]*feeRateUpdate
		expectedResult   map[Network]*big.Int
	}{
		{
			name:             "new network from external source",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{},
			sourcedUpdate: &SourcedFeeRateUpdate{
				Source: "external-oracle",
				Stamp:  now,
				Weight: 1.0,
				FeeRates: []*SourcedFeeRate{
					{Network: "BTC", FeeRate: []byte{0, 0, 0, 100}},
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "existing network with newer timestamp should update",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(80),
						stamp:   oldStamp,
						weight:  1.0,
					},
				},
			},
			sourcedUpdate: &SourcedFeeRateUpdate{
				Source: "external-oracle",
				Stamp:  newerStamp,
				Weight: 1.0,
				FeeRates: []*SourcedFeeRate{
					{Network: "BTC", FeeRate: []byte{0, 0, 0, 100}},
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "existing network with older timestamp should ignore",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			sourcedUpdate: &SourcedFeeRateUpdate{
				Source: "external-oracle",
				Stamp:  oldStamp,
				Weight: 1.0,
				FeeRates: []*SourcedFeeRate{
					{Network: "BTC", FeeRate: []byte{0, 0, 0, 80}},
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			expectedResult: nil, // No update occurred
		},
		{
			name: "multiple fee rates in single update",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			sourcedUpdate: &SourcedFeeRateUpdate{
				Source: "source2",
				Stamp:  now,
				Weight: 0.8,
				FeeRates: []*SourcedFeeRate{
					{Network: "BTC", FeeRate: []byte{0, 0, 0, 120}},
					{Network: "ETH", FeeRate: []byte{0, 0, 0, 50}},
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
					"source2": {
						network: "BTC",
						feeRate: big.NewInt(120),
						stamp:   now,
						weight:  0.8,
					},
				},
				"ETH": {
					"source2": {
						network: "ETH",
						feeRate: big.NewInt(50),
						stamp:   now,
						weight:  0.8,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(109), // round((100*1.0 + 120*0.8) / 1.8) = round(108.888...) = 109
				"ETH": big.NewInt(50),
			},
		},
		{
			name: "new source for existing network",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			sourcedUpdate: &SourcedFeeRateUpdate{
				Source: "source2",
				Stamp:  now,
				Weight: 1.0,
				FeeRates: []*SourcedFeeRate{
					{Network: "BTC", FeeRate: []byte{0, 0, 0, 120}},
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
					"source2": {
						network: "BTC",
						feeRate: big.NewInt(120),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(110), // round((100*1.0 + 120*1.0) / 2.0) = round(110.0) = 110
			},
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := &Oracle{
				log:      log,
				feeRates: tt.existingFeeRates,
				diviners: make(map[string]*diviner),
			}

			result := oracle.MergeFeeRates(tt.sourcedUpdate)

			// Verify the merged fee rates match expected
			if len(oracle.feeRates) != len(tt.expectedFeeRates) {
				t.Errorf("Expected %d networks, got %d", len(tt.expectedFeeRates), len(oracle.feeRates))
			}

			for network, expectedSources := range tt.expectedFeeRates {
				actualSources, found := oracle.feeRates[network]
				if !found {
					t.Errorf("Expected network %s to be in oracle.feeRates", network)
					continue
				}

				if len(actualSources) != len(expectedSources) {
					t.Errorf("For network %s, expected %d sources, got %d",
						network, len(expectedSources), len(actualSources))
				}

				for source, expectedUpdate := range expectedSources {
					actualUpdate, found := actualSources[source]
					if !found {
						t.Errorf("Expected source %s for network %s", source, network)
						continue
					}

					if actualUpdate.feeRate.Cmp(expectedUpdate.feeRate) != 0 {
						t.Errorf("For network %s source %s, expected fee rate %s, got %s",
							network, source, expectedUpdate.feeRate.String(), actualUpdate.feeRate.String())
					}

					if actualUpdate.weight != expectedUpdate.weight {
						t.Errorf("For network %s source %s, expected weight %.2f, got %.2f",
							network, source, expectedUpdate.weight, actualUpdate.weight)
					}

					if !actualUpdate.stamp.Equal(expectedUpdate.stamp) {
						t.Errorf("For network %s source %s, expected stamp %v, got %v",
							network, source, expectedUpdate.stamp, actualUpdate.stamp)
					}
				}
			}

			// Verify the return value matches expected result
			if tt.expectedResult == nil {
				if len(result) > 0 {
					t.Errorf("Expected nil/empty result, got %v", result)
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
				} else {
					if len(result) != len(tt.expectedResult) {
						t.Errorf("Expected %d networks in result, got %d", len(tt.expectedResult), len(result))
					}
					for network, expectedRate := range tt.expectedResult {
						actualRate, found := result[network]
						if !found {
							t.Errorf("Expected network %s in result", network)
							continue
						}
						if actualRate.Cmp(expectedRate) != 0 {
							t.Errorf("For network %s, expected aggregated fee rate %s, got %s",
								network, expectedRate.String(), actualRate.String())
						}
					}
					for network := range result {
						if _, expected := tt.expectedResult[network]; !expected {
							t.Errorf("Unexpected network %s in result", network)
						}
					}
				}
			}
		})
	}
}


func TestConcurrency(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("multiple goroutines reading prices simultaneously", func(t *testing.T) {
		oracle := &Oracle{
			log:    log,
			prices: make(map[Ticker]map[string]*priceUpdate),
		}

		now := time.Now()
		// Pre-populate with some price data
		oracle.prices["BTC"] = map[string]*priceUpdate{
			"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
			"source2": {ticker: "BTC", price: 51000.0, stamp: now, weight: 1.0},
		}
		oracle.prices["ETH"] = map[string]*priceUpdate{
			"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
		}

		// Launch multiple readers concurrently
		const numReaders = 50
		done := make(chan bool, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					prices := oracle.Prices()
					if len(prices) > 0 {
						// Verify data integrity
						if btcPrice, found := prices["BTC"]; found {
							if btcPrice < 0 {
								t.Errorf("Invalid BTC price: %.2f", btcPrice)
							}
						}
					}
				}
				done <- true
			}()
		}

		// Wait for all readers to complete
		for i := 0; i < numReaders; i++ {
			<-done
		}
	})

	t.Run("multiple goroutines reading fee rates simultaneously", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			feeRates: make(map[Network]map[string]*feeRateUpdate),
		}

		now := time.Now()
		// Pre-populate with some fee rate data
		oracle.feeRates["BTC"] = map[string]*feeRateUpdate{
			"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
			"source2": {network: "BTC", feeRate: big.NewInt(120), stamp: now, weight: 1.0},
		}
		oracle.feeRates["ETH"] = map[string]*feeRateUpdate{
			"source1": {network: "ETH", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
		}

		const numReaders = 50
		done := make(chan bool, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					feeRates := oracle.FeeRates()
					if len(feeRates) > 0 {
						// Verify data integrity
						if btcRate, found := feeRates["BTC"]; found {
							if btcRate.Sign() == 0 {
								t.Error("Invalid BTC fee rate: 0")
							}
						}
					}
				}
				done <- true
			}()
		}

		// Wait for all readers to complete
		for i := 0; i < numReaders; i++ {
			<-done
		}
	})

	t.Run("concurrent reads and writes of prices", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			prices:   make(map[Ticker]map[string]*priceUpdate),
			diviners: make(map[string]*diviner),
		}

		const numReaders = 20
		const numWriters = 5
		done := make(chan bool, numReaders+numWriters)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.Prices()
					_ = oracle.getPrices(map[Ticker]bool{"BTC": true})
				}
				done <- true
			}()
		}

		// Launch writers
		for i := 0; i < numWriters; i++ {
			writerID := i
			go func() {
				for j := 0; j < 10; j++ {
					sourcedUpdate := &SourcedPriceUpdate{
						Source: fmt.Sprintf("writer-%d", writerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Weight: 1.0,
						Prices: []*SourcedPrice{
							{Ticker: "BTC", Price: float64(50000 + j)},
							{Ticker: "ETH", Price: float64(3000 + j)},
						},
					}
					oracle.MergePrices(sourcedUpdate)
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}
	})

	t.Run("concurrent reads and writes of fee rates", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			feeRates: make(map[Network]map[string]*feeRateUpdate),
			diviners: make(map[string]*diviner),
		}

		const numReaders = 20
		const numWriters = 5
		done := make(chan bool, numReaders+numWriters)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.FeeRates()
					_ = oracle.getFeeRates(map[Network]bool{"BTC": true})
				}
				done <- true
			}()
		}

		// Launch writers
		for i := 0; i < numWriters; i++ {
			writerID := i
			go func() {
				for j := 0; j < 10; j++ {
					sourcedUpdate := &SourcedFeeRateUpdate{
						Source: fmt.Sprintf("writer-%d", writerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Weight: 1.0,
						FeeRates: []*SourcedFeeRate{
							{Network: "BTC", FeeRate: bigIntToBytes(big.NewInt(int64(100 + j)))},
							{Network: "ETH", FeeRate: bigIntToBytes(big.NewInt(int64(50 + j)))},
						},
					}
					oracle.MergeFeeRates(sourcedUpdate)
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}
	})

	t.Run("concurrent merge and read operations", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			prices:   make(map[Ticker]map[string]*priceUpdate),
			feeRates: make(map[Network]map[string]*feeRateUpdate),
			diviners: make(map[string]*diviner),
		}

		const numReaders = 20
		const numMergers = 10
		done := make(chan bool, numReaders+numMergers)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.Prices()
					_ = oracle.FeeRates()
				}
				done <- true
			}()
		}

		// Launch mergers
		for i := 0; i < numMergers; i++ {
			mergerID := i
			go func() {
				for j := 0; j < 10; j++ {
					sourcedPrices := &SourcedPriceUpdate{
						Source: fmt.Sprintf("merger-%d", mergerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Weight: 1.0,
						Prices: []*SourcedPrice{
							{Ticker: "BTC", Price: float64(50000 + j)},
						},
					}
					oracle.MergePrices(sourcedPrices)

					sourcedFeeRates := &SourcedFeeRateUpdate{
						Source: fmt.Sprintf("merger-%d", mergerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Weight: 1.0,
						FeeRates: []*SourcedFeeRate{
							{Network: "BTC", FeeRate: bigIntToBytes(big.NewInt(int64(100 + j)))},
						},
					}
					oracle.MergeFeeRates(sourcedFeeRates)
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numMergers; i++ {
			<-done
		}
	})
}

func TestPublicPrices(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	t.Run("returns all prices", func(t *testing.T) {
		oracle := &Oracle{
			log: log,
			prices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
				},
				"ETH": {
					"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
				},
			},
		}

		result := oracle.Prices()

		if len(result) != 2 {
			t.Errorf("Expected 2 prices, got %d", len(result))
		}

		if result["BTC"] != 50000.0 {
			t.Errorf("Expected BTC price 50000.0, got %.2f", result["BTC"])
		}

		if result["ETH"] != 3000.0 {
			t.Errorf("Expected ETH price 3000.0, got %.2f", result["ETH"])
		}
	})

	t.Run("returns empty map for empty oracle", func(t *testing.T) {
		oracle := &Oracle{
			log:    log,
			prices: make(map[Ticker]map[string]*priceUpdate),
		}

		result := oracle.Prices()

		if len(result) != 0 {
			t.Errorf("Expected 0 prices, got %d", len(result))
		}
	})
}

func TestPublicFeeRates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	t.Run("returns all fee rates", func(t *testing.T) {
		oracle := &Oracle{
			log: log,
			feeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
				},
				"ETH": {
					"source1": {network: "ETH", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
				},
			},
		}

		result := oracle.FeeRates()

		if len(result) != 2 {
			t.Errorf("Expected 2 fee rates, got %d", len(result))
		}

		if result["BTC"].Cmp(big.NewInt(100)) != 0 {
			t.Errorf("Expected BTC fee rate 100, got %s", result["BTC"].String())
		}

		if result["ETH"].Cmp(big.NewInt(50)) != 0 {
			t.Errorf("Expected ETH fee rate 50, got %s", result["ETH"].String())
		}
	})

	t.Run("returns empty map for empty oracle", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			feeRates: make(map[Network]map[string]*feeRateUpdate),
		}

		result := oracle.FeeRates()

		if len(result) != 0 {
			t.Errorf("Expected 0 fee rates, got %d", len(result))
		}
	})
}

func TestMergeWithEmptyUpdates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("MergePrices with nil", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			prices:   make(map[Ticker]map[string]*priceUpdate),
			diviners: make(map[string]*diviner),
		}

		// Should not panic
		result := oracle.MergePrices(nil)

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}

		if len(oracle.prices) != 0 {
			t.Errorf("Expected no prices, got %d", len(oracle.prices))
		}
	})

	t.Run("MergeFeeRates with nil", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			feeRates: make(map[Network]map[string]*feeRateUpdate),
			diviners: make(map[string]*diviner),
		}

		// Should not panic
		result := oracle.MergeFeeRates(nil)

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}

		if len(oracle.feeRates) != 0 {
			t.Errorf("Expected no fee rates, got %d", len(oracle.feeRates))
		}
	})

	t.Run("MergePrices with empty prices slice", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			prices:   make(map[Ticker]map[string]*priceUpdate),
			diviners: make(map[string]*diviner),
		}

		result := oracle.MergePrices(&SourcedPriceUpdate{
			Source: "test",
			Prices: []*SourcedPrice{},
		})

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})

	t.Run("MergeFeeRates with empty fee rates slice", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			feeRates: make(map[Network]map[string]*feeRateUpdate),
			diviners: make(map[string]*diviner),
		}

		result := oracle.MergeFeeRates(&SourcedFeeRateUpdate{
			Source:   "test",
			FeeRates: []*SourcedFeeRate{},
		})

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})
}

// TestAgedWeightBoundaries tests edge cases in weight calculation.
func TestAgedWeightBoundaries(t *testing.T) {
	const defaultWeight = 1.0

	t.Run("exactly at full validity period boundary", func(t *testing.T) {
		stamp := time.Now().Add(-fullValidityPeriod)
		weight := agedWeight(defaultWeight, stamp)

		// At exactly fullValidityPeriod, should still be full weight
		if weight < 0.99 || weight > 1.0 {
			t.Errorf("Expected weight ~1.0 at fullValidityPeriod boundary, got %.4f", weight)
		}
	})

	t.Run("exactly at expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration)
		weight := agedWeight(defaultWeight, stamp)

		// At exactly validityExpiration, should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 at expiration boundary, got %.4f", weight)
		}
	})

	t.Run("just before expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration + time.Millisecond)
		weight := agedWeight(defaultWeight, stamp)

		// Just before expiration should be very small but not zero
		if weight <= 0 || weight > 0.01 {
			t.Errorf("Expected small positive weight just before expiration, got %.4f", weight)
		}
	})

	t.Run("just after expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration - time.Millisecond)
		weight := agedWeight(defaultWeight, stamp)

		// After expiration should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 after expiration, got %.4f", weight)
		}
	})

	t.Run("future timestamp", func(t *testing.T) {
		futureStamp := time.Now().Add(time.Hour)
		weight := agedWeight(defaultWeight, futureStamp)

		// Future timestamps should get full weight (age is negative)
		if weight != defaultWeight {
			t.Errorf("Expected full weight for future timestamp, got %.4f", weight)
		}
	})

	t.Run("very old timestamp", func(t *testing.T) {
		veryOld := time.Now().Add(-24 * time.Hour)
		weight := agedWeight(defaultWeight, veryOld)

		// Very old should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 for very old timestamp, got %.4f", weight)
		}
	})

	t.Run("zero default weight", func(t *testing.T) {
		stamp := time.Now()
		weight := agedWeight(0, stamp)

		// Zero weight should always be zero
		if weight != 0 {
			t.Errorf("Expected weight 0 for zero default weight, got %.4f", weight)
		}
	})

	t.Run("fractional default weight", func(t *testing.T) {
		stamp := time.Now()
		weight := agedWeight(0.5, stamp)

		// Fresh timestamp with 0.5 weight should return 0.5
		if weight != 0.5 {
			t.Errorf("Expected weight 0.5 for fresh timestamp with 0.5 default, got %.4f", weight)
		}
	})

	t.Run("decay progression is linear", func(t *testing.T) {
		// Test that decay is linear through the decay period
		quarterDecay := time.Now().Add(-fullValidityPeriod - decayPeriod/4)
		halfDecay := time.Now().Add(-fullValidityPeriod - decayPeriod/2)
		threeQuarterDecay := time.Now().Add(-fullValidityPeriod - 3*decayPeriod/4)

		weightQuarter := agedWeight(defaultWeight, quarterDecay)
		weightHalf := agedWeight(defaultWeight, halfDecay)
		weightThreeQuarter := agedWeight(defaultWeight, threeQuarterDecay)

		// Should be approximately: 0.75, 0.5, 0.25
		if weightQuarter < 0.70 || weightQuarter > 0.80 {
			t.Errorf("Expected weight ~0.75 at quarter decay, got %.4f", weightQuarter)
		}
		if weightHalf < 0.45 || weightHalf > 0.55 {
			t.Errorf("Expected weight ~0.5 at half decay, got %.4f", weightHalf)
		}
		if weightThreeQuarter < 0.20 || weightThreeQuarter > 0.30 {
			t.Errorf("Expected weight ~0.25 at three-quarter decay, got %.4f", weightThreeQuarter)
		}

		// Verify progression is decreasing
		if weightQuarter <= weightHalf || weightHalf <= weightThreeQuarter {
			t.Errorf("Weight should decrease linearly: %.4f > %.4f > %.4f",
				weightQuarter, weightHalf, weightThreeQuarter)
		}
	})

	t.Run("within full validity period", func(t *testing.T) {
		// Test various points within full validity period
		fresh := time.Now()
		oneMinute := time.Now().Add(-time.Minute)
		twoMinutes := time.Now().Add(-2 * time.Minute)
		fourMinutes := time.Now().Add(-4 * time.Minute)

		weights := []float64{
			agedWeight(defaultWeight, fresh),
			agedWeight(defaultWeight, oneMinute),
			agedWeight(defaultWeight, twoMinutes),
			agedWeight(defaultWeight, fourMinutes),
		}

		// All should be full weight since fullValidityPeriod is 5 minutes
		for i, w := range weights {
			if w != defaultWeight {
				t.Errorf("Expected full weight at %d minutes old, got %.4f", i, w)
			}
		}
	})
}

func TestGetSourceWeight(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("returns weight for existing source", func(t *testing.T) {
		div1 := &diviner{name: "source1", weight: 0.8}
		div2 := &diviner{name: "source2", weight: 0.5}

		oracle := &Oracle{
			log:      log,
			diviners: map[string]*diviner{
				"source1": div1,
				"source2": div2,
			},
		}

		weight := oracle.GetSourceWeight("source1")
		if weight != 0.8 {
			t.Errorf("Expected weight 0.8, got %.1f", weight)
		}

		weight = oracle.GetSourceWeight("source2")
		if weight != 0.5 {
			t.Errorf("Expected weight 0.5, got %.1f", weight)
		}
	})

	t.Run("returns default weight for non-existent source", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			diviners: make(map[string]*diviner),
		}

		weight := oracle.GetSourceWeight("non-existent")
		if weight != 1.0 {
			t.Errorf("Expected default weight 1.0, got %.1f", weight)
		}
	})

	t.Run("returns default weight when diviners is empty", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			diviners: make(map[string]*diviner),
		}

		weight := oracle.GetSourceWeight("any-source")
		if weight != 1.0 {
			t.Errorf("Expected default weight 1.0, got %.1f", weight)
		}
	})
}

func TestRescheduleDiviner(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("reschedules existing diviner", func(t *testing.T) {
		mockDiv := &diviner{
			name:       "test-source",
			resetTimer: make(chan struct{}, 1),
		}

		oracle := &Oracle{
			log: log,
			diviners: map[string]*diviner{
				"test-source": mockDiv,
			},
		}

		oracle.rescheduleDiviner("test-source")

		// Verify the reschedule signal was sent
		select {
		case <-mockDiv.resetTimer:
			// Success - signal was sent
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected reschedule signal to be sent")
		}
	})

	t.Run("does nothing for non-existent diviner", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			diviners: make(map[string]*diviner),
		}

		// Should not panic
		oracle.rescheduleDiviner("non-existent")
	})

	t.Run("does nothing when diviners is empty", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			diviners: make(map[string]*diviner),
		}

		// Should not panic
		oracle.rescheduleDiviner("any-source")
	})
}

func TestRun(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("Run completes with no diviners", func(t *testing.T) {
		oracle := &Oracle{
			log:      log,
			diviners: make(map[string]*diviner),
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			oracle.Run(ctx)
			close(done)
		}()

		select {
		case <-done:
			// Success - Run completed immediately
		case <-time.After(time.Second):
			t.Error("Run did not complete with empty diviners")
		}
	})

	t.Run("Run waits for diviners and exits on context cancellation", func(t *testing.T) {
		// Create mock diviners that wait for context
		mockDiviners := make(map[string]*diviner)
		for i := 0; i < 2; i++ {
			name := fmt.Sprintf("source%d", i)
			mockDiviners[name] = &diviner{name: name}
		}

		oracle := &Oracle{
			log:      log,
			diviners: mockDiviners,
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			oracle.Run(ctx)
			close(done)
		}()

		// Give Run time to start goroutines
		time.Sleep(50 * time.Millisecond)

		// Cancel and wait for completion
		cancel()

		select {
		case <-done:
			// Success - Run exited
		case <-time.After(time.Second):
			t.Error("Run did not exit after context cancellation")
		}
	})
}

func TestNewOracle(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("creates oracle with default sources", func(t *testing.T) {
		cfg := &Config{
			Log:           log,
			PublishUpdate: func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle == nil {
			t.Fatal("Expected non-nil oracle")
		}

		if oracle.log != log {
			t.Error("Expected logger to be set")
		}

		if len(oracle.diviners) == 0 {
			t.Error("Expected diviners to be initialized with default sources")
		}

		if oracle.prices == nil || oracle.feeRates == nil {
			t.Error("Expected prices and fee rates maps to be initialized")
		}
	})

	t.Run("initializes with unauthed sources", func(t *testing.T) {
		cfg := &Config{
			Log:           log,
			PublishUpdate: func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify some default unauthed sources exist
		if len(oracle.diviners) == 0 {
			t.Error("Expected at least some diviners from unauthed sources")
		}
	})

	t.Run("nil http client uses default client", func(t *testing.T) {
		cfg := &Config{
			Log:           log,
			HTTPClient:    nil,
			PublishUpdate: func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle.httpClient == nil {
			t.Error("Expected httpClient to be set to default")
		}
	})

	t.Run("custom http client is used", func(t *testing.T) {
		customClient := &mockHTTPClient{}
		cfg := &Config{
			Log:           log,
			HTTPClient:    customClient,
			PublishUpdate: func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle.httpClient != customClient {
			t.Error("Expected custom HTTP client to be used")
		}
	})

	t.Run("initializes empty price and fee rate maps", func(t *testing.T) {
		cfg := &Config{
			Log:           log,
			PublishUpdate: func(ctx context.Context, update *pb.NodeOracleUpdate) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(oracle.prices) != 0 {
			t.Error("Expected empty prices map")
		}

		if len(oracle.feeRates) != 0 {
			t.Error("Expected empty fee rates map")
		}
	})
}

// mockHTTPClient is a mock implementation of HTTPClient for testing
type mockHTTPClient struct{}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return nil, nil
}
