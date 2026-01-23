package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"time"
)

// priceUpdate is the internal message used for when a price update is fetched
// or received from a source.
type priceUpdate struct {
	ticker Ticker
	price  float64

	// Added by Oracle loops
	stamp  time.Time
	weight float64
}

// feeRateUpdate is the internal message used for when a fee rate update is
// fetched or received from a source.
type feeRateUpdate struct {
	network Network
	feeRate *big.Int

	// Added by Oracle loops
	stamp  time.Time
	weight float64
}

// divination is an update from a source, which could be fee rates or prices.
type divination any // []*priceUpdate or []*feeRateUpdate

// httpSource is a source from which http requests will be performed on some
// interval.
type httpSource struct {
	name      string
	url       string
	parse     func(io.Reader) (divination, error)
	period    time.Duration // default 5 minutes
	errPeriod time.Duration // default 1 minute
	weight    float64       // range: [0, 1], default 1
	headers   []http.Header
}

func (h *httpSource) fetch(ctx context.Context, client HTTPClient) (any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return nil, fmt.Errorf("error generating request %q: %v", h.url, err)
	}

	for _, header := range h.headers {
		for k, vs := range header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching %q: %v", h.url, err)
	}
	defer resp.Body.Close()

	return h.parse(resp.Body)
}

// setHTTPSourceDefaults sets default values for HTTP sources.
func setHTTPSourceDefaults(sources []*httpSource) error {
	for _, s := range sources {
		const defaultWeight = 1.0
		if s.weight == 0 {
			s.weight = defaultWeight
		} else if s.weight < 0 {
			return fmt.Errorf("http source '%s' has a negative weight", s.name)
		} else if s.weight > 1 {
			return fmt.Errorf("http source '%s' has a weight > 1", s.name)
		}
		const defaultHttpRequestInterval = time.Minute * 5
		if s.period == 0 {
			s.period = defaultHttpRequestInterval
		}
		const defaultHttpErrorInterval = time.Minute
		if s.errPeriod == 0 {
			s.errPeriod = defaultHttpErrorInterval
		}
	}

	return nil
}

// unauthedHttpSources are HTTP sources that don't require any kind of
// authorization e.g. registration or API keys.
var unauthedHttpSources = []*httpSource{
	{
		name:  "dcrdata",
		url:   "https://explorer.dcrdata.org/insight/api/utils/estimatefee?nbBlocks=2",
		parse: dcrdataParser,
	},
	{
		name:  "btc.mempooldotspace",
		url:   "https://mempool.space/api/v1/fees/recommended",
		parse: mempoolDotSpaceParser,
	},
	{
		// You can make up to 20,000 requests per month on the free plan, which
		// works out to one request every ~2m10s, but we'll stick with the
		// default of 5m.
		name:  "coinpaprika",
		url:   "https://api.coinpaprika.com/v1/tickers",
		parse: coinpaprikaParser,
	},
	// Bitcore APIs  not well-documented, and I believe that they use
	// estimatesmartfee, which is known to be a little wild. Use with caution.
	{
		name:   "bch.bitcore",
		url:    "https://api.bitcore.io/api/BCH/mainnet/fee/2",
		parse:  bitcoreBitcoinCashParser,
		weight: 0.25,
	},
	{
		name:   "doge.bitcore",
		url:    "https://api.bitcore.io/api/DOGE/mainnet/fee/2",
		parse:  bitcoreDogecoinParser,
		weight: 0.25,
	},
	{
		name:   "ltc.bitcore",
		url:    "https://api.bitcore.io/api/LTC/mainnet/fee/2",
		parse:  bitcoreLitecoinParser,
		weight: 0.25,
	},
	{
		name:   "firo.org",
		url:    "https://explorer.firo.org/insight-api-zcoin/utils/estimatefee",
		parse:  firoOrgParser,
		weight: 0.25, // Also an estimatesmartfee source, I believe.
	},
	{
		name:   "ltc.blockcypher",
		url:    "https://api.blockcypher.com/v1/ltc/main",
		parse:  blockcypherLitecoinParser,
		weight: 0.25,
	},
}

func dcrdataParser(r io.Reader) (u divination, err error) {
	var resp map[string]float64
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if len(resp) != 1 || resp["2"] == 0 {
		return nil, fmt.Errorf("unexpected response format: %+v", resp)
	}
	if resp["2"] <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return []*feeRateUpdate{{network: "DCR", feeRate: uint64ToBigInt(uint64(math.Round(resp["2"] * 1e5)))}}, nil
}

func mempoolDotSpaceParser(r io.Reader) (u divination, err error) {
	var resp struct {
		FastestFee uint64 `json:"fastestFee"`
	}
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.FastestFee == 0 {
		return nil, fmt.Errorf("zero fee rate returned")
	}
	return []*feeRateUpdate{{network: "BTC", feeRate: uint64ToBigInt(resp.FastestFee)}}, nil
}

func coinpaprikaParser(r io.Reader) (u divination, err error) {
	var prices []*struct {
		Symbol string `json:"symbol"`
		Quotes struct {
			USD struct {
				Price float64 `json:"price"`
			} `json:"USD"`
		} `json:"quotes"`
	}
	if err := streamDecodeJSON(r, &prices); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(prices))
	us := make([]*priceUpdate, 0, len(prices))
	for _, p := range prices {
		if seen[p.Symbol] {
			continue
		}
		seen[p.Symbol] = true
		us = append(us, &priceUpdate{
			ticker: Ticker(p.Symbol),
			price:  p.Quotes.USD.Price,
		})
	}
	return us, nil
}

func coinmarketcapSource(key string) *httpSource {
	// Coinmarketcap free plan gives 10,000 credits per month. This endpoint
	// uses 1 credit per call per 200 assets requested. So if we request the
	// top 400 assets, we can call 5,000 times per month, which comes to
	// about 1 call per every 8.9 minutes. We'll call every 10 minutes.
	const requestInterval = time.Minute * 10
	return &httpSource{
		name:    "coinmarketcap",
		url:     "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=400",
		parse:   coinmarketcapParser,
		headers: []http.Header{{"X-CMC_PRO_API_KEY": []string{key}}},
		period:  requestInterval,
	}
}

func coinmarketcapParser(r io.Reader) (u divination, err error) {
	var resp struct {
		Data []*struct {
			Symbol string `json:"symbol"`
			Quote  struct {
				USD struct {
					Price float64 `json:"price"`
				} `json:"USD"`
			} `json:"quote"`
		} `json:"data"`
	}
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	prices := resp.Data
	seen := make(map[string]bool, len(prices))
	us := make([]*priceUpdate, 0, len(prices))
	for _, p := range prices {
		if seen[p.Symbol] {
			continue
		}
		seen[p.Symbol] = true
		us = append(us, &priceUpdate{
			ticker: Ticker(p.Symbol),
			price:  p.Quote.USD.Price,
		})
	}
	return us, nil
}

func parseBitcoreResponse(netName Network, r io.Reader) (u divination, err error) {
	var resp struct {
		RatePerKB float64 `json:"feerate"`
	}
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.RatePerKB <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return []*feeRateUpdate{{network: netName, feeRate: uint64ToBigInt(uint64(resp.RatePerKB * 1e5))}}, nil
}

func bitcoreBitcoinCashParser(r io.Reader) (u divination, err error) {
	return parseBitcoreResponse("BCH", r)
}

func bitcoreDogecoinParser(r io.Reader) (u divination, err error) {
	return parseBitcoreResponse("DOGE", r)
}

func bitcoreLitecoinParser(r io.Reader) (u divination, err error) {
	return parseBitcoreResponse("LTC", r)
}

func firoOrgParser(r io.Reader) (u divination, err error) {
	var resp map[string]float64
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if len(resp) != 1 || resp["2"] == 0 {
		return nil, fmt.Errorf("unexpected response format: %+v", resp)
	}
	if resp["2"] <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return []*feeRateUpdate{{network: "FIRO", feeRate: uint64ToBigInt(uint64(math.Round(resp["2"] * 1e5)))}}, nil
}

func blockcypherLitecoinParser(r io.Reader) (u divination, err error) {
	var resp struct {
		// Low    float64 `json:"low_fee_per_kb"`
		Medium float64 `json:"medium_fee_per_kb"`
		// High   float64 `json:"high_fee_per_kb"`
	}
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.Medium <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return []*feeRateUpdate{{network: "LTC", feeRate: uint64ToBigInt(uint64(resp.Medium * 1e5))}}, nil
}

func tatumSource(key, coin, name string, parser func(io.Reader) (divination, error)) *httpSource {
	// Tatum free tier provides 100,000 lifetime API credits. With 3 sources
	// (BTC, LTC, DOGE) making requests every 5 minutes, this equals ~864
	// requests/day, which will exhaust the free tier in approximately 116 days.
	// A paid plan will be required for use in production.
	return &httpSource{
		name:      name,
		url:       fmt.Sprintf("https://api.tatum.io/v3/blockchain/fee/%s", coin),
		parse:     parser,
		headers:   []http.Header{{"x-api-key": []string{key}}},
		period:    time.Minute * 5,
		errPeriod: time.Minute,
		weight:    1.0,
	}
}

func tatumBitcoinSource(key string) *httpSource {
	return tatumSource(key, "BTC", "tatum.btc", tatumBitcoinParser)
}

func tatumLitecoinSource(key string) *httpSource {
	return tatumSource(key, "LTC", "tatum.ltc", tatumLitecoinParser)
}

func tatumDogecoinSource(key string) *httpSource {
	return tatumSource(key, "DOGE", "tatum.doge", tatumDogecoinParser)
}

func tatumParser(r io.Reader, network Network) (u divination, err error) {
	var resp struct {
		Fast float64 `json:"fast"`
		// Medium float64 `json:"medium"`
		// Slow   float64 `json:"slow"`
	}
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.Fast <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return []*feeRateUpdate{{network: network, feeRate: uint64ToBigInt(uint64(resp.Fast))}}, nil
}

func tatumBitcoinParser(r io.Reader) (u divination, err error) {
	return tatumParser(r, "BTC")
}

func tatumLitecoinParser(r io.Reader) (u divination, err error) {
	return tatumParser(r, "LTC")
}

func tatumDogecoinParser(r io.Reader) (u divination, err error) {
	return tatumParser(r, "DOGE")
}

func cryptoApisSource(key, blockchain, name string, parser func(io.Reader) (divination, error)) *httpSource {
	// Crypto APIs free tier provides 100 requests per day. With 5 sources
	// (BTC, BCH, DOGE, DASH, LTC) making requests every 5 minutes, this equals
	// ~1,440 requests/day, which exceeds the free tier limit. A paid plan is
	// required for production use.
	return &httpSource{
		name:      name,
		url:       fmt.Sprintf("https://rest.cryptoapis.io/blockchain-fees/utxo/%s/mainnet/mempool", blockchain),
		parse:     parser,
		headers:   []http.Header{{"X-API-Key": []string{key}}},
		period:    time.Minute * 5,
		errPeriod: time.Minute,
		weight:    1.0,
	}
}

func cryptoApisBitcoinSource(key string) *httpSource {
	return cryptoApisSource(key, "BTC", "cryptoapis.btc", cryptoApisBitcoinParser)
}

func cryptoApisBitcoinCashSource(key string) *httpSource {
	return cryptoApisSource(key, "BCH", "cryptoapis.bch", cryptoApisBitcoinCashParser)
}

func cryptoApisDogecoinSource(key string) *httpSource {
	return cryptoApisSource(key, "DOGE", "cryptoapis.doge", cryptoApisDogecoinParser)
}

func cryptoApisDashSource(key string) *httpSource {
	return cryptoApisSource(key, "DASH", "cryptoapis.dash", cryptoApisDashParser)
}

func cryptoApisLitecoinSource(key string) *httpSource {
	return cryptoApisSource(key, "LTC", "cryptoapis.ltc", cryptoApisLitecoinParser)
}

func cryptoApisParser(r io.Reader, network Network) (u divination, err error) {
	var resp struct {
		Data struct {
			Item struct {
				Fast string `json:"fast"`
				// Standard float64 `json:"standard"`
				// Slow   float64 `json:"slow"`
			} `json:"item"`
		} `json:"data"`
	}
	if err := streamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	// The API returns fees in the coin's base unit (e.g., BTC, LTC, DOGE).
	// Convert to satoshis per byte by multiplying by 1e8.
	feeRate, err := strconv.ParseFloat(resp.Data.Item.Fast, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse fee rate: %v", err)
	}
	if feeRate <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	feeRateSatoshis := uint64(feeRate * 1e8)
	return []*feeRateUpdate{{network: network, feeRate: uint64ToBigInt(feeRateSatoshis)}}, nil
}

func cryptoApisBitcoinParser(r io.Reader) (u divination, err error) {
	return cryptoApisParser(r, "BTC")
}

func cryptoApisBitcoinCashParser(r io.Reader) (u divination, err error) {
	return cryptoApisParser(r, "BCH")
}

func cryptoApisDogecoinParser(r io.Reader) (u divination, err error) {
	return cryptoApisParser(r, "DOGE")
}

func cryptoApisDashParser(r io.Reader) (u divination, err error) {
	return cryptoApisParser(r, "DASH")
}

func cryptoApisLitecoinParser(r io.Reader) (u divination, err error) {
	return cryptoApisParser(r, "LTC")
}

func streamDecodeJSON(stream io.Reader, thing any) error {
	return json.NewDecoder(stream).Decode(thing)
}
