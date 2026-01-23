//go:build live

package oracle

import (
	"context"
	"math/big"
	"net/http"
	"testing"
	"time"
)

// These tests make real HTTP requests to external APIs.
// Run with: go test -tags=live -v -run TestLive ./oracle

func TestDcrdataFetcher(t *testing.T) {
	src := &httpSource{
		name:  "dcrdata",
		url:   "https://explorer.dcrdata.org/insight/api/utils/estimatefee?nbBlocks=2",
		parse: dcrdataParser,
	}
	testFeeRateFetcher(t, src, "DCR")
}

func TestMempoolDotSpaceFetcher(t *testing.T) {
	src := &httpSource{
		name:  "btc.mempooldotspace",
		url:   "https://mempool.space/api/v1/fees/recommended",
		parse: mempoolDotSpaceParser,
	}
	testFeeRateFetcher(t, src, "BTC")
}

func TestCoinpaprikaFetcher(t *testing.T) {
	src := &httpSource{
		name:  "coinpaprika",
		url:   "https://api.coinpaprika.com/v1/tickers",
		parse: coinpaprikaParser,
	}
	testPriceFetcher(t, src)
}

func TestBitcoreBitcoinCashFetcher(t *testing.T) {
	src := &httpSource{
		name:   "bch.bitcore",
		url:    "https://api.bitcore.io/api/BCH/mainnet/fee/2",
		parse:  bitcoreBitcoinCashParser,
		weight: 0.25,
	}
	testFeeRateFetcher(t, src, "BCH")
}

func TestBitcoreDogecoinFetcher(t *testing.T) {
	src := &httpSource{
		name:   "doge.bitcore",
		url:    "https://api.bitcore.io/api/DOGE/mainnet/fee/2",
		parse:  bitcoreDogecoinParser,
		weight: 0.25,
	}
	testFeeRateFetcher(t, src, "DOGE")
}

func TestBitcoreLitecoinFetcher(t *testing.T) {
	src := &httpSource{
		name:   "ltc.bitcore",
		url:    "https://api.bitcore.io/api/LTC/mainnet/fee/2",
		parse:  bitcoreLitecoinParser,
		weight: 0.25,
	}
	testFeeRateFetcher(t, src, "LTC")
}

func TestFiroOrgFetcher(t *testing.T) {
	src := &httpSource{
		name:   "firo.org",
		url:    "https://explorer.firo.org/insight-api-zcoin/utils/estimatefee",
		parse:  firoOrgParser,
		weight: 0.25,
	}
	testFeeRateFetcher(t, src, "FIRO")
}

func TestBlockcypherLitecoinFetcher(t *testing.T) {
	src := &httpSource{
		name:   "ltc.blockcypher",
		url:    "https://api.blockcypher.com/v1/ltc/main",
		parse:  blockcypherLitecoinParser,
		weight: 0.25,
	}
	testFeeRateFetcher(t, src, "LTC")
}

// Supply API keys before running authenticated source tests.
const (
	coinmarketcapAPIKey = ""
	tatumAPIKey         = ""
	cryptoApisAPIKey    = ""
)

func TestCoinmarketcapFetcher(t *testing.T) {
	if coinmarketcapAPIKey == "" {
		t.Skip("coinmarketcap API key not provided")
	}
	src := coinmarketcapSource(coinmarketcapAPIKey)
	testPriceFetcher(t, src)
}

func TestTatumBitcoinFetcher(t *testing.T) {
	if tatumAPIKey == "" {
		t.Skip("tatum API key not provided")
	}
	src := tatumBitcoinSource(tatumAPIKey)
	testFeeRateFetcher(t, src, "BTC")
}

func TestTatumLitecoinFetcher(t *testing.T) {
	if tatumAPIKey == "" {
		t.Skip("tatum API key not provided")
	}
	src := tatumLitecoinSource(tatumAPIKey)
	testFeeRateFetcher(t, src, "LTC")
}

func TestTatumDogecoinFetcher(t *testing.T) {
	if tatumAPIKey == "" {
		t.Skip("tatum API key not provided")
	}
	src := tatumDogecoinSource(tatumAPIKey)
	testFeeRateFetcher(t, src, "DOGE")
}

func TestCryptoApisBitcoinFetcher(t *testing.T) {
	if cryptoApisAPIKey == "" {
		t.Skip("cryptoapis API key not provided")
	}
	src := cryptoApisBitcoinSource(cryptoApisAPIKey)
	testFeeRateFetcher(t, src, "BTC")
}

func TestCryptoApisBitcoinCashFetcher(t *testing.T) {
	if cryptoApisAPIKey == "" {
		t.Skip("cryptoapis API key not provided")
	}
	src := cryptoApisBitcoinCashSource(cryptoApisAPIKey)
	testFeeRateFetcher(t, src, "BCH")
}

func TestCryptoApisDogecoinFetcher(t *testing.T) {
	if cryptoApisAPIKey == "" {
		t.Skip("cryptoapis API key not provided")
	}
	src := cryptoApisDogecoinSource(cryptoApisAPIKey)
	testFeeRateFetcher(t, src, "DOGE")
}

func TestCryptoApisDashFetcher(t *testing.T) {
	if cryptoApisAPIKey == "" {
		t.Skip("cryptoapis API key not provided")
	}
	src := cryptoApisDashSource(cryptoApisAPIKey)
	testFeeRateFetcher(t, src, "DASH")
}

func TestCryptoApisLitecoinFetcher(t *testing.T) {
	if cryptoApisAPIKey == "" {
		t.Skip("cryptoapis API key not provided")
	}
	src := cryptoApisLitecoinSource(cryptoApisAPIKey)
	testFeeRateFetcher(t, src, "LTC")
}

func testFeeRateFetcher(t *testing.T, src *httpSource, expectedNetwork Network) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 30 * time.Second}
	result, err := src.fetch(ctx, client)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	updates, ok := result.([]*feeRateUpdate)
	if !ok {
		t.Fatalf("expected []*feeRateUpdate, got %T", result)
	}

	if len(updates) == 0 {
		t.Fatal("no fee rate updates returned")
	}

	found := false
	for _, u := range updates {
		if u.network == expectedNetwork {
			found = true
			if u.feeRate.Cmp(big.NewInt(0)) == 0 {
				t.Errorf("fee rate for %s is zero", expectedNetwork)
			}
			t.Logf("%s fee rate: %d", expectedNetwork, u.feeRate)
		}
	}

	if !found {
		t.Errorf("expected network %s not found in updates", expectedNetwork)
	}
}

func testPriceFetcher(t *testing.T, src *httpSource) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 30 * time.Second}
	result, err := src.fetch(ctx, client)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	updates, ok := result.([]*priceUpdate)
	if !ok {
		t.Fatalf("expected []*priceUpdate, got %T", result)
	}

	if len(updates) == 0 {
		t.Fatal("no price updates returned")
	}

	// Check for some common tickers.
	commonTickers := []Ticker{"BTC", "ETH", "LTC"}
	for _, ticker := range commonTickers {
		for _, u := range updates {
			if u.ticker == ticker {
				if u.price <= 0 {
					t.Errorf("price for %s is <= 0: %f", ticker, u.price)
				}
				t.Logf("%s price: $%.2f", ticker, u.price)
				break
			}
		}
	}

	t.Logf("total prices returned: %d", len(updates))
}
