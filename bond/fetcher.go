package bond

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultFetchTimeout = time.Second * 5
)

// TxSource is a block explorer API source for fetching raw transaction data.
type TxSource struct {
	name      string
	asset     string
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
	txSources  map[string][]TxSource
	rateLimits map[string]time.Time
	rateMutex  sync.Mutex
}

// NewTxFetcher creates a new transaction fetcher.
func NewTxFetcher(txSources []TxSource) *TxFetcher {
	tf := &TxFetcher{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		txSources:  make(map[string][]TxSource),
		rateLimits: make(map[string]time.Time),
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
func (tf *TxFetcher) FetchTx(asset string, txid string) ([]byte, error) {
	sources, ok := tf.txSources[asset]
	if !ok {
		return nil, fmt.Errorf("no explorers configured for asset %s", asset)
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no explorers available for asset %s", asset)
	}

	var lastErr error
	for i, source := range sources {
		if err := tf.applyRateLimit(source); err != nil {
			lastErr = err
			continue
		}

		txBytes, err := tf.fetchFromSource(source, txid)
		if err == nil {
			return txBytes, nil
		}

		// Preserve exact transaction not found error
		if strings.Contains(err.Error(), "transaction not found") {
			return nil, err
		}

		lastErr = fmt.Errorf("explorer %d failed: %w", i, err)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("all explorers failed")
	}

	return nil, lastErr
}

func (tf *TxFetcher) fetchFromSource(source TxSource, txid string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultFetchTimeout)
	defer cancel()

	url := fmt.Sprintf("%s%s", source.baseURL, fmt.Sprintf(source.txHexPath, txid))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := tf.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("transaction not found")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	txHex := string(body)
	txBytes, err := hex.DecodeString(txHex)
	if err != nil {
		return nil, fmt.Errorf("invalid hex in response: %w", err)
	}

	return txBytes, nil
}

func (tf *TxFetcher) applyRateLimit(source TxSource) error {
	tf.rateMutex.Lock()
	defer tf.rateMutex.Unlock()

	lastRequest := tf.rateLimits[source.name]
	now := time.Now()

	if !lastRequest.IsZero() {
		elapsed := now.Sub(lastRequest)
		if elapsed < source.rateLimit {
			sleepDuration := source.rateLimit - elapsed
			time.Sleep(sleepDuration)
			now = time.Now()
			tf.rateLimits[source.name] = now.Add(source.rateLimit)
			return nil
		}
	}

	tf.rateLimits[source.name] = now.Add(source.rateLimit)
	return nil
}
