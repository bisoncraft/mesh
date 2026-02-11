package oracle

import (
	"context"
	"math/big"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/bisoncraft/mesh/tatanka/pb"
)

const (
	fullValidityPeriod = time.Minute * 5
	validityExpiration = time.Minute * 30
	decayPeriod        = validityExpiration - fullValidityPeriod
	requestTimeout     = time.Second * 5

	// PriceTopicPrefix is the topic prefix for price updates sent to clients.
	PriceTopicPrefix = "price."
	// FeeRateTopicPrefix is the topic prefix for fee rate updates sent to clients.
	FeeRateTopicPrefix = "fee_rate."
)

// Ticker is the upper-case symbol used to indicate an asset.
type Ticker string

// Network is the network symbol of a Blockchain.
type Network string

// SourcedPrice represents a single price entry within a sourced update batch.
type SourcedPrice struct {
	Ticker Ticker
	Price  float64
}

// SourcedPriceUpdate is a batch of price updates from a single source, used for
// sharing with other Tatanka Mesh nodes.
type SourcedPriceUpdate struct {
	Source string
	Stamp  time.Time
	Weight float64
	Prices []*SourcedPrice
}

// SourcedFeeRate represents a single fee rate entry within a sourced update batch.
type SourcedFeeRate struct {
	Network Network
	FeeRate []byte // big-endian encoded big integer
}

// SourcedFeeRateUpdate is a batch of fee rate updates from a single source, used
// for sharing with other Tatanka Mesh nodes.
type SourcedFeeRateUpdate struct {
	Source   string
	Stamp    time.Time
	Weight   float64
	FeeRates []*SourcedFeeRate
}

// PriceUpdate is an aggregated price update. These are emitted when an update
// is received from a source.
type PriceUpdate struct {
	Ticker Ticker
	Price  float64
}

// FeeRateUpdate is an aggregated fee rate update. These are emitted when an
// update is received from a source.
type FeeRateUpdate struct {
	Network Network
	FeeRate *big.Int
}

// HTTPClient defines the requirements for implementing an http client.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	Log           slog.Logger
	CMCKey        string
	TatumKey      string
	CryptoApisKey string
	HTTPClient    HTTPClient // Optional. If nil, http.DefaultClient is used.
	PublishUpdate func(ctx context.Context, update *pb.NodeOracleUpdate) error
}

type Oracle struct {
	log         slog.Logger
	httpClient  HTTPClient
	httpSources []*httpSource

	feeRatesMtx sync.RWMutex
	feeRates    map[Network]map[string]*feeRateUpdate

	pricesMtx sync.RWMutex
	prices    map[Ticker]map[string]*priceUpdate

	divinersMtx sync.RWMutex
	diviners    map[string]*diviner

	publishUpdate func(ctx context.Context, update *pb.NodeOracleUpdate) error
}

func New(cfg *Config) (*Oracle, error) {
	httpSources := slices.Clone(unauthedHttpSources)

	if cfg.CMCKey != "" {
		httpSources = append(httpSources, coinmarketcapSource(cfg.CMCKey))
	}

	if cfg.TatumKey != "" {
		httpSources = append(httpSources,
			tatumBitcoinSource(cfg.TatumKey),
			tatumLitecoinSource(cfg.TatumKey),
			tatumDogecoinSource(cfg.TatumKey),
		)
	}

	if cfg.CryptoApisKey != "" {
		httpSources = append(httpSources,
			cryptoApisBitcoinSource(cfg.CryptoApisKey),
			cryptoApisBitcoinCashSource(cfg.CryptoApisKey),
			cryptoApisDogecoinSource(cfg.CryptoApisKey),
			cryptoApisDashSource(cfg.CryptoApisKey),
			cryptoApisLitecoinSource(cfg.CryptoApisKey),
		)
	}

	if err := setHTTPSourceDefaults(httpSources); err != nil {
		return nil, err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	oracle := &Oracle{
		log:           cfg.Log,
		httpClient:    httpClient,
		httpSources:   httpSources,
		feeRates:      make(map[Network]map[string]*feeRateUpdate),
		prices:        make(map[Ticker]map[string]*priceUpdate),
		diviners:      make(map[string]*diviner),
		publishUpdate: cfg.PublishUpdate,
	}

	for _, source := range httpSources {
		src := source
		fetcher := func(ctx context.Context) (any, error) {
			return src.fetch(ctx, httpClient)
		}
		div := newDiviner(src.name, fetcher, src.weight, src.period, src.errPeriod, oracle.publishUpdate, oracle.log)
		oracle.diviners[div.name] = div
	}

	return oracle, nil
}

// priceWeightCounter is used to calculate weighted averages for prices.
type priceWeightCounter struct {
	weightedSum float64
	totalWeight float64
}

// feeRateWeightCounter is used to calculate weighted fee rate averages with arbitrary precision.
type feeRateWeightCounter struct {
	weightedSum *big.Float
	totalWeight float64
}

// agedWeight returns a weight based on the age of an update.
func agedWeight(weight float64, stamp time.Time) float64 {
	// Older updates lose weight.
	age := time.Since(stamp)
	if age < 0 {
		age = 0
	}

	switch {
	case age < fullValidityPeriod:
		return weight
	case age > validityExpiration:
		return 0
	default:
		// Calculate remaining validity as a fraction of the decay period.
		remainingValidity := validityExpiration - age
		return weight * (float64(remainingValidity) / float64(decayPeriod))
	}
}

func (o *Oracle) getFeeRates(nets map[Network]bool) map[Network]*big.Int {
	o.feeRatesMtx.RLock()
	size := len(nets)
	if nets == nil {
		size = len(o.feeRates)
	}

	counters := make(map[Network]*feeRateWeightCounter, size)
	for net, updates := range o.feeRates {
		if nets != nil && !nets[net] {
			continue
		}

		counter, found := counters[net]
		if !found {
			counter = &feeRateWeightCounter{
				weightedSum: new(big.Float),
			}
			counters[net] = counter
		}

		for _, entry := range updates {
			weight := agedWeight(entry.weight, entry.stamp)
			if weight == 0 {
				continue
			}
			counter.totalWeight += weight

			// Multiply weight (float64) by feeRate (big.Int) using big.Float
			weightFloat := new(big.Float).SetFloat64(weight)
			feeRateFloat := new(big.Float).SetInt(entry.feeRate)
			product := new(big.Float).Mul(weightFloat, feeRateFloat)
			counter.weightedSum.Add(counter.weightedSum, product)
		}
	}

	o.feeRatesMtx.RUnlock()

	// Calculate weighted averages.
	feeRates := make(map[Network]*big.Int, len(counters))
	for net, counter := range counters {
		if counter.totalWeight == 0 {
			continue
		}

		// Divide weightedSum (big.Float) by totalWeight (float64)
		totalWeightFloat := new(big.Float).SetFloat64(counter.totalWeight)
		avgFloat := new(big.Float).Quo(counter.weightedSum, totalWeightFloat)

		// Round to nearest integer
		if avgFloat.Sign() >= 0 {
			avgFloat.Add(avgFloat, new(big.Float).SetFloat64(0.5))
		} else {
			avgFloat.Sub(avgFloat, new(big.Float).SetFloat64(0.5))
		}

		// Convert to big.Int (this truncates towards zero after rounding)
		rounded := new(big.Int)
		avgFloat.Int(rounded)
		feeRates[net] = rounded
	}

	return feeRates
}

// FeeRates returns the aggregated tx fee rates for all known networks.
func (o *Oracle) FeeRates() map[Network]*big.Int {
	return o.getFeeRates(nil)
}

// MergeFeeRates merges fee rates from another oracle into this oracle.
// Returns a map of the networks whose aggregated fee rates were updated.
func (o *Oracle) MergeFeeRates(sourcedUpdate *SourcedFeeRateUpdate) map[Network]*big.Int {
	if sourcedUpdate == nil || len(sourcedUpdate.FeeRates) == 0 {
		return nil
	}

	o.feeRatesMtx.Lock()
	updatedNetworks := make(map[Network]bool)

	for _, fr := range sourcedUpdate.FeeRates {
		proposedUpdate := &feeRateUpdate{
			network: fr.Network,
			feeRate: bytesToBigInt(fr.FeeRate),
			stamp:   sourcedUpdate.Stamp,
			weight:  sourcedUpdate.Weight,
		}
		netSources, found := o.feeRates[fr.Network]
		if !found {
			o.feeRates[fr.Network] = map[string]*feeRateUpdate{
				sourcedUpdate.Source: proposedUpdate,
			}
			updatedNetworks[fr.Network] = true
			continue
		}
		existingUpdate, found := netSources[sourcedUpdate.Source]
		if !found {
			netSources[sourcedUpdate.Source] = proposedUpdate
			updatedNetworks[fr.Network] = true
			continue
		}
		if sourcedUpdate.Stamp.After(existingUpdate.stamp) {
			netSources[sourcedUpdate.Source] = proposedUpdate
			updatedNetworks[fr.Network] = true
		}
	}
	o.feeRatesMtx.Unlock()

	o.rescheduleDiviner(sourcedUpdate.Source)

	return o.getFeeRates(updatedNetworks)
}

func (o *Oracle) getPrices(tickers map[Ticker]bool) map[Ticker]float64 {
	o.pricesMtx.RLock()
	size := len(tickers)
	if tickers == nil {
		size = len(o.prices)
	}
	counters := make(map[Ticker]*priceWeightCounter, size)

	for ticker, updates := range o.prices {
		if tickers != nil && !tickers[ticker] {
			continue
		}
		counter, found := counters[ticker]
		if !found {
			counter = &priceWeightCounter{}
			counters[ticker] = counter
		}
		for _, entry := range updates {
			weight := agedWeight(entry.weight, entry.stamp)
			if weight == 0 {
				continue
			}
			counter.totalWeight += weight
			counter.weightedSum += weight * entry.price
		}
	}
	o.pricesMtx.RUnlock()

	priceMap := make(map[Ticker]float64, len(counters))
	for ticker, counter := range counters {
		if counter.totalWeight == 0 {
			continue
		}
		priceMap[ticker] = counter.weightedSum / counter.totalWeight
	}

	return priceMap
}

// Prices returns the aggregated prices for all known tickers.
func (o *Oracle) Prices() map[Ticker]float64 {
	return o.getPrices(nil)
}

func (o *Oracle) rescheduleDiviner(name string) {
	o.divinersMtx.RLock()
	div, found := o.diviners[name]
	o.divinersMtx.RUnlock()
	if !found {
		// Do nothing.
		return
	}

	div.reschedule()
}

// GetSourceWeight returns the configured weight for a source by name.
// If the source is not found, returns 1.0 as a default weight.
func (o *Oracle) GetSourceWeight(sourceName string) float64 {
	o.divinersMtx.RLock()
	div, found := o.diviners[sourceName]
	o.divinersMtx.RUnlock()
	if !found {
		return 1.0
	}
	return div.weight
}

// MergePrices merges prices from another oracle into this oracle.
// Returns a map of the tickers whose aggregated prices were updated.
func (o *Oracle) MergePrices(sourcedUpdate *SourcedPriceUpdate) map[Ticker]float64 {
	if sourcedUpdate == nil || len(sourcedUpdate.Prices) == 0 {
		return nil
	}

	o.pricesMtx.Lock()
	updatedTickers := make(map[Ticker]bool)

	for _, p := range sourcedUpdate.Prices {
		proposedUpdate := &priceUpdate{
			ticker: p.Ticker,
			price:  p.Price,
			stamp:  sourcedUpdate.Stamp,
			weight: sourcedUpdate.Weight,
		}
		tickerSources, found := o.prices[p.Ticker]
		if !found {
			o.prices[p.Ticker] = map[string]*priceUpdate{
				sourcedUpdate.Source: proposedUpdate,
			}
			updatedTickers[p.Ticker] = true
			continue
		}
		existingUpdate, found := tickerSources[sourcedUpdate.Source]
		if !found {
			tickerSources[sourcedUpdate.Source] = proposedUpdate
			updatedTickers[p.Ticker] = true
			continue
		}
		if sourcedUpdate.Stamp.After(existingUpdate.stamp) {
			tickerSources[sourcedUpdate.Source] = proposedUpdate
			updatedTickers[p.Ticker] = true
		}
	}
	o.pricesMtx.Unlock()

	o.rescheduleDiviner(sourcedUpdate.Source)

	return o.getPrices(updatedTickers)
}

func (o *Oracle) Run(ctx context.Context) {
	var wg sync.WaitGroup

	o.divinersMtx.RLock()
	for _, div := range o.diviners {
		wg.Add(1)
		go func(d *diviner) {
			defer wg.Done()
			d.run(ctx)
		}(div)
	}
	o.divinersMtx.RUnlock()

	wg.Wait()
}

// bytesToBigInt converts big-endian encoded bytes to big.Int.
func bytesToBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return big.NewInt(0)
	}
	return new(big.Int).SetBytes(b)
}

// bigIntToBytes converts big.Int to big-endian encoded bytes.
func bigIntToBytes(bi *big.Int) []byte {
	if bi == nil || bi.Sign() == 0 {
		return []byte{0}
	}
	return bi.Bytes()
}

// uint64ToBigInt converts uint64 to big.Int.
func uint64ToBigInt(val uint64) *big.Int {
	return new(big.Int).SetUint64(val)
}
