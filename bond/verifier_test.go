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
	t.Run("RegisterParser", func(t *testing.T) {
		cfg := &Config{
			TxFetcher:  NewTxFetcher(nil),
			FetchPrice: func(ticker string) (float64, error) { return 1.0, nil },
		}
		verifier := NewVerifier(cfg)

		expectedExpiry := time.Now().Add(24 * time.Hour)
		testIDBytes := []byte{1, 2, 3, 4, 5}

		verifier.RegisterParser(AssetDCR, mockParser(1.5, expectedExpiry, testIDBytes, nil))

		// Verify parser is registered
		parser, ok := verifier.parsers[AssetDCR]
		if !ok {
			t.Fatalf("expected parser to be registered for AssetDCR")
		}

		// Verify parser returns expected values
		assetValue, expiry, idBytes, err := parser(0, nil)
		if err != nil {
			t.Fatalf("parser failed: %v", err)
		}

		if assetValue != 1.5 {
			t.Errorf("expected asset value 1.5, got %f", assetValue)
		}

		if !expiry.Equal(expectedExpiry) {
			t.Errorf("expected expiry %v, got %v", expectedExpiry, expiry)
		}

		if len(idBytes) != len(testIDBytes) {
			t.Errorf("expected %d id bytes, got %d", len(testIDBytes), len(idBytes))
		}
	})

	t.Run("MissingParser", func(t *testing.T) {
		cfg := &Config{
			TxFetcher:  NewTxFetcher(nil),
			FetchPrice: func(ticker string) (float64, error) { return 1.0, nil },
		}
		verifier := NewVerifier(cfg)

		// Try to get parser for asset without registered parser
		verifier.parsersMtx.RLock()
		_, ok := verifier.parsers[AssetBTC]
		verifier.parsersMtx.RUnlock()

		if ok {
			t.Errorf("expected no parser registered for AssetBTC")
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

		_, err = AssetIDToTicker(999)
		if err == nil {
			t.Errorf("expected error for unknown asset")
		}
	})
}
