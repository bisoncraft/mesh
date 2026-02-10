package utils

import (
	"context"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
)

// FetchRatesFunc fetches rates. Used by quota-aware wrappers that don't want to
// know about URLs, headers, or parsing details.
type FetchRatesFunc func(ctx context.Context) (*sources.RateInfo, error)

// UnlimitedSourceConfig configures a source without quota constraints.
type UnlimitedSourceConfig struct {
	Name       string
	Weight     float64
	MinPeriod  time.Duration
	FetchRates FetchRatesFunc
}

// UnlimitedSource is a source without quota constraints.
type UnlimitedSource struct {
	name       string
	weight     float64
	minPeriod  time.Duration
	fetchRates FetchRatesFunc
}

// NewUnlimitedSource creates a new unlimited source.
func NewUnlimitedSource(cfg UnlimitedSourceConfig) *UnlimitedSource {
	if cfg.Name == "" {
		panic("unlimited source: name is required")
	}
	if cfg.FetchRates == nil {
		panic("unlimited source: FetchRates is required")
	}

	weight := cfg.Weight
	if weight == 0 {
		weight = defaultWeight
	}
	minPeriod := cfg.MinPeriod
	if minPeriod == 0 {
		minPeriod = defaultMinPeriod
	}

	return &UnlimitedSource{
		name:       cfg.Name,
		weight:     weight,
		minPeriod:  minPeriod,
		fetchRates: cfg.FetchRates,
	}
}

func (s *UnlimitedSource) Name() string             { return s.name }
func (s *UnlimitedSource) Weight() float64          { return s.weight }
func (s *UnlimitedSource) MinPeriod() time.Duration { return s.minPeriod }

func (s *UnlimitedSource) FetchRates(ctx context.Context) (*sources.RateInfo, error) {
	return s.fetchRates(ctx)
}

func (s *UnlimitedSource) QuotaStatus() *sources.QuotaStatus {
	return UnlimitedQuotaStatus()
}
