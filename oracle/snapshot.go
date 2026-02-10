package oracle

import (
	"fmt"
	"time"
)

// DataType identifies the kind of data point (price or fee rate).
type DataType = string

const (
	PriceData   DataType = "price"
	FeeRateData DataType = "fee_rate"
)

// SourceStatus is the per-source view.
type SourceStatus struct {
	LastFetch                *time.Time        `json:"last_fetch,omitempty"`
	NextFetchTime            *time.Time        `json:"next_fetch_time,omitempty"`
	MinFetchInterval         *time.Duration    `json:"min_fetch_interval,omitempty"`
	NetworkSustainableRate   *float64          `json:"network_sustainable_rate,omitempty"`
	NetworkSustainablePeriod *time.Duration    `json:"network_sustainable_period,omitempty"`
	NetworkNextFetchTime     *time.Time        `json:"network_next_fetch_time,omitempty"`
	LastError                string            `json:"last_error,omitempty"`
	LastErrorTime            *time.Time        `json:"last_error_time,omitempty"`
	OrderedNodes             []string          `json:"ordered_nodes,omitempty"` // Node IDs in fetch order
	Fetches24h               map[string]int    `json:"fetches_24h,omitempty"`
	Quotas                   map[string]*Quota `json:"quotas,omitempty"`
	// LatestData holds the most recent values from this source, keyed by
	// data type ("price" or "fee_rate") then by identifier (ticker or
	// network name), with the formatted value string as the map value.
	LatestData map[string]map[string]string `json:"latest_data,omitempty"`
}

// Quota is per-node quota info embedded in each source.
type Quota struct {
	FetchesRemaining int64     `json:"fetches_remaining"`
	FetchesLimit     int64     `json:"fetches_limit"`
	ResetTime        time.Time `json:"reset_time"`
}

// SourceContribution represents a single source's contribution to an
// aggregated price or fee rate.
type SourceContribution struct {
	Value  string    `json:"value,omitempty"`
	Stamp  time.Time `json:"stamp,omitempty"`
	Weight float64   `json:"weight,omitempty"`
}

// sourcesStatus assembles the per-source status data.
func (o *Oracle) sourcesStatus() map[string]*SourceStatus {
	fetchCounts := o.fetchTracker.fetchCounts()
	latestPerSource := o.fetchTracker.latestPerSource()
	localQuotas := o.quotaManager.getLocalQuotas()
	networkQuotas := o.quotaManager.getNetworkQuotas()

	// Collect all source names.
	sourceNames := make(map[string]bool)
	o.divinersMtx.RLock()
	for name := range o.diviners {
		sourceNames[name] = true
	}
	o.divinersMtx.RUnlock()

	sources := make(map[string]*SourceStatus, len(sourceNames))

	for name := range sourceNames {
		status := &SourceStatus{
			Fetches24h: make(map[string]int),
			Quotas:     make(map[string]*Quota),
		}

		// Latest fetch.
		if stamp, ok := latestPerSource[name]; ok {
			status.LastFetch = &stamp
		}

		// Next fetch time and intervals (only for our diviners).
		o.divinersMtx.RLock()
		if div, ok := o.diviners[name]; ok {
			info := div.fetchScheduleInfo()
			if !info.NextFetchTime.IsZero() {
				nft := info.NextFetchTime
				status.NextFetchTime = &nft
			}
			if !info.NetworkNextFetchTime.IsZero() {
				nnft := info.NetworkNextFetchTime
				status.NetworkNextFetchTime = &nnft
			}
			minPeriod := info.MinPeriod
			status.MinFetchInterval = &minPeriod
			status.NetworkSustainableRate = &info.NetworkSustainableRate
			nsp := info.NetworkSustainablePeriod
			status.NetworkSustainablePeriod = &nsp
			status.OrderedNodes = info.OrderedNodes
			if errMsg, errTime := div.fetchErrorInfo(); errMsg != "" && errTime != nil {
				status.LastError = errMsg
				status.LastErrorTime = errTime
			}
		}
		o.divinersMtx.RUnlock()

		// Per-node fetch counts.
		if counts, ok := fetchCounts[name]; ok {
			status.Fetches24h = counts
		}

		// Local quotas (our node).
		if lq, ok := localQuotas[name]; ok {
			status.Quotas[o.nodeID] = &Quota{
				FetchesRemaining: lq.FetchesRemaining,
				FetchesLimit:     lq.FetchesLimit,
				ResetTime:        lq.ResetTime,
			}
		}

		// Network quotas (peers).
		for peerID, sourceQuotas := range networkQuotas {
			if pq, ok := sourceQuotas[name]; ok {
				status.Quotas[peerID] = &Quota{
					FetchesRemaining: pq.FetchesRemaining,
					FetchesLimit:     pq.FetchesLimit,
					ResetTime:        pq.ResetTime,
				}
			}
		}

		// Latest data from this source.
		latestData := make(map[string]map[string]string)
		o.pricesMtx.RLock()
		for ticker, bucket := range o.prices {
			bucket.mtx.RLock()
			if entry, ok := bucket.sources[name]; ok {
				if latestData[PriceData] == nil {
					latestData[PriceData] = make(map[string]string)
				}
				latestData[PriceData][string(ticker)] = fmt.Sprintf("%f", entry.price)
			}
			bucket.mtx.RUnlock()
		}
		o.pricesMtx.RUnlock()

		o.feeRatesMtx.RLock()
		for network, bucket := range o.feeRates {
			bucket.mtx.RLock()
			if entry, ok := bucket.sources[name]; ok {
				if latestData[FeeRateData] == nil {
					latestData[FeeRateData] = make(map[string]string)
				}
				latestData[FeeRateData][string(network)] = entry.feeRate.String()
			}
			bucket.mtx.RUnlock()
		}
		o.feeRatesMtx.RUnlock()

		if len(latestData) > 0 {
			status.LatestData = latestData
		}

		sources[name] = status
	}

	return sources
}

// SnapshotRate holds the aggregated value and all source contributions for a rate.
type SnapshotRate struct {
	Value         string                         `json:"value,omitempty"`
	Contributions map[string]*SourceContribution `json:"contributions,omitempty"`
}

// priceContributions returns all prices with their source contributions.
func (o *Oracle) priceContributions() map[string]*SnapshotRate {
	result := make(map[string]*SnapshotRate)
	o.pricesMtx.RLock()
	defer o.pricesMtx.RUnlock()

	for ticker, bucket := range o.prices {
		bucket.mtx.RLock()
		contribs := make(map[string]*SourceContribution, len(bucket.sources))
		for name, upd := range bucket.sources {
			contribs[name] = &SourceContribution{
				Value:  fmt.Sprintf("%f", upd.price),
				Stamp:  upd.stamp,
				Weight: upd.weight,
			}
		}
		agg := bucket.aggregatedPrice()
		bucket.mtx.RUnlock()

		result[string(ticker)] = &SnapshotRate{
			Value:         fmt.Sprintf("%f", agg),
			Contributions: contribs,
		}
	}
	return result
}

// feeRateContributions returns all fee rates with their source contributions.
func (o *Oracle) feeRateContributions() map[string]*SnapshotRate {
	result := make(map[string]*SnapshotRate)
	o.feeRatesMtx.RLock()
	defer o.feeRatesMtx.RUnlock()

	for network, bucket := range o.feeRates {
		bucket.mtx.RLock()
		contribs := make(map[string]*SourceContribution, len(bucket.sources))
		for name, upd := range bucket.sources {
			contribs[name] = &SourceContribution{
				Value:  upd.feeRate.String(),
				Stamp:  upd.stamp,
				Weight: upd.weight,
			}
		}
		agg := bucket.aggregatedRate()
		bucket.mtx.RUnlock()
		if agg == nil {
			continue
		}

		result[string(network)] = &SnapshotRate{
			Value:         agg.String(),
			Contributions: contribs,
		}
	}
	return result
}

// OracleSnapshot contains the current state of the oracle.
type OracleSnapshot struct {
	NodeID   string                   `json:"node_id,omitempty"`
	Sources  map[string]*SourceStatus `json:"sources,omitempty"`
	Prices   map[string]*SnapshotRate `json:"prices,omitempty"`
	FeeRates map[string]*SnapshotRate `json:"fee_rates,omitempty"`
}

// OracleSnapshot returns the current state of the oracle.
func (o *Oracle) OracleSnapshot() *OracleSnapshot {
	return &OracleSnapshot{
		NodeID:   o.nodeID,
		Sources:  o.sourcesStatus(),
		Prices:   o.priceContributions(),
		FeeRates: o.feeRateContributions(),
	}
}
