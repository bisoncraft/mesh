package bond

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// TxSource is a block explorer API source for fetching raw transaction data.
type TxSource struct {
	name      string
	asset     uint32
	baseURL   string
	txHexPath string
	rateLimit time.Duration
}

var defaultTxSources = []TxSource{
	{
		name:      "dcrdata.decred.org",
		asset:     AssetDCR,
		baseURL:   "https://dcrdata.decred.org/api",
		txHexPath: "/tx/hex/%s",
		rateLimit: 500 * time.Millisecond,
	},
	{
		name:      "explorer.dcrdata.org",
		asset:     AssetDCR,
		baseURL:   "https://explorer.dcrdata.org/api",
		txHexPath: "/tx/hex/%s",
		rateLimit: 500 * time.Millisecond,
	},
	{
		name:      "blockstream.info",
		asset:     AssetBTC,
		baseURL:   "https://blockstream.info/api",
		txHexPath: "/tx/%s/hex",
		rateLimit: 500 * time.Millisecond,
	},
	{
		name:      "mempool.space",
		asset:     AssetBTC,
		baseURL:   "https://mempool.space/api",
		txHexPath: "/tx/%s/hex",
		rateLimit: 500 * time.Millisecond,
	},
}

// TxFetcher fetches raw transaction data from block explorers.
type TxFetcher struct {
	httpClient *http.Client
	txSources  map[uint32][]TxSource
	rateLimits map[uint32]time.Time
	rateMutex  sync.Mutex
}

// NewTxFetcher creates a new transaction fetcher.
func NewTxFetcher(txSources []TxSource) *TxFetcher {
	tf := &TxFetcher{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		txSources:  make(map[uint32][]TxSource),
		rateLimits: make(map[uint32]time.Time),
	}

	sources := txSources
	if len(sources) == 0 {
		sources = defaultTxSources
	}

	for _, source := range sources {
		tf.txSources[source.asset] = append(tf.txSources[source.asset], source)
	}

	return tf
}

// FetchTx fetches raw transaction bytes from block explorers with fallback logic.
func (tf *TxFetcher) FetchTx(ctx context.Context, asset uint32, txid string) ([]byte, error) {
	sources, ok := tf.txSources[asset]
	if !ok {
		return nil, fmt.Errorf("no explorers configured for asset %d", asset)
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no explorers available for asset %d", asset)
	}

	var lastErr error
	for i, source := range sources {
		if err := tf.applyRateLimit(asset, source.rateLimit); err != nil {
			lastErr = err
			continue
		}

		url := fmt.Sprintf("%s%s", source.baseURL, fmt.Sprintf(source.txHexPath, txid))
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}

		resp, err := tf.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("explorer %d request failed: %w", i, err)
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, fmt.Errorf("transaction not found")
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("explorer %d returned %d: %s", i, resp.StatusCode, string(body))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response: %w", err)
			continue
		}

		txHex := string(body)
		txBytes, err := hex.DecodeString(txHex)
		if err != nil {
			lastErr = fmt.Errorf("invalid hex in response: %w", err)
			continue
		}

		return txBytes, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("all explorers failed")
	}

	return nil, lastErr
}

func (tf *TxFetcher) applyRateLimit(asset uint32, limit time.Duration) error {
	tf.rateMutex.Lock()
	defer tf.rateMutex.Unlock()

	lastRequest := tf.rateLimits[asset]
	now := time.Now()

	if !lastRequest.IsZero() {
		elapsed := now.Sub(lastRequest)
		if elapsed < limit {
			// Sleep until rate limit expires
			sleepDuration := limit - elapsed
			time.Sleep(sleepDuration)
			tf.rateLimits[asset] = now.Add(sleepDuration)
			return nil
		}
	}

	tf.rateLimits[asset] = now
	return nil
}
