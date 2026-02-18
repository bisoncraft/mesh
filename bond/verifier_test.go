package bond

import (
	"testing"
	"time"
)

// mockParser returns a mock bond parser function that returns fixed values or an error.
func mockParser(assetValue float64, expiry time.Time, idBytes []byte, err error) ParseFunc {
	return func(ver uint16, rawTx []byte) (float64, time.Time, []byte, error) {
		if err != nil {
			return 0, time.Time{}, nil, err
		}
		return assetValue, expiry, idBytes, nil
	}
}

func TestVerifier(t *testing.T) {
	t.Run("MissingParser", func(t *testing.T) {
		cfg := &Config{
			TxFetcher:  NewTxFetcher(nil),
			FetchPrice: func(ticker string) (float64, error) { return 1.0, nil },
			Assets:     []string{"unknown:asset"}, // Unknown asset
		}
		_, err := NewVerifier(cfg)
		if err == nil {
			t.Errorf("expected error for unsupported asset")
		}
	})

	t.Run("AssetIDToTicker", func(t *testing.T) {
		ticker, err := AssetIDToTicker(AssetBTC)
		if err != nil {
			t.Fatalf("AssetIDToTicker failed: %v", err)
		}
		if ticker != "BTC" {
			t.Errorf("expected ticker BTC, got %s", ticker)
		}

		ticker, err = AssetIDToTicker(AssetDCR)
		if err != nil {
			t.Fatalf("AssetIDToTicker failed: %v", err)
		}
		if ticker != "DCR" {
			t.Errorf("expected ticker DCR, got %s", ticker)
		}

		_, err = AssetIDToTicker("unknown:asset")
		if err == nil {
			t.Errorf("expected error for unknown asset")
		}
	})
}
