package sources

import (
	"context"
	"math/big"
	"time"
)

// Ticker is the upper-case symbol used to indicate an asset.
type Ticker string

// Network is the network symbol of a Blockchain.
type Network string

// PriceUpdate represents a price update from a source.
type PriceUpdate struct {
	Ticker Ticker
	Price  float64
}

// FeeRateUpdate represents a fee rate update from a source.
type FeeRateUpdate struct {
	Network Network
	FeeRate *big.Int
}

// RateInfo is a union type that can hold either price updates or fee rate updates.
type RateInfo struct {
	Prices   []*PriceUpdate
	FeeRates []*FeeRateUpdate
}

// QuotaStatus represents the current quota state for an API source.
// Values represent fetches, not raw API credits.
type QuotaStatus struct {
	FetchesRemaining int64
	FetchesLimit     int64
	ResetTime        time.Time
}

// Source is the interface that all oracle data sources must implement.
type Source interface {
	// Name returns the source identifier.
	Name() string

	// FetchRates fetches current rates/data.
	FetchRates(ctx context.Context) (*RateInfo, error)

	// QuotaStatus returns the current quota status. Always returns a valid status.
	QuotaStatus() *QuotaStatus

	// Weight returns the configured weight for this source (0-1 range).
	Weight() float64

	// MinPeriod returns the minimum allowed fetch period for this source.
	// This is based on the API's data refresh rate and rate limits.
	MinPeriod() time.Duration
}
