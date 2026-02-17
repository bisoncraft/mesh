package bond

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTxFetcher(t *testing.T) {
	t.Run("FetchTx", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			txHex := "0100000001"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(txHex))
			}))
			defer server.Close()

			sources := []TxSource{
				{
					name:      "test",
					asset:     AssetDCR,
					baseURL:   server.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
			}
			client := NewTxFetcher(sources)

			txBytes, err := client.FetchTx(context.Background(), AssetDCR, "abc123")
			if err != nil {
				t.Fatalf("FetchTx failed: %v", err)
			}

			expected, _ := hex.DecodeString(txHex)
			if len(txBytes) != len(expected) {
				t.Errorf("expected %d bytes, got %d", len(expected), len(txBytes))
			}
		})

		t.Run("NotFound", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			sources := []TxSource{
				{
					name:      "test",
					asset:     AssetDCR,
					baseURL:   server.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
			}
			client := NewTxFetcher(sources)

			_, err := client.FetchTx(context.Background(), AssetDCR, "missing")
			if err == nil {
				t.Fatal("expected error for 404")
			}
			if err.Error() != "transaction not found" {
				t.Errorf("expected 'transaction not found', got '%v'", err)
			}
		})

		t.Run("Fallback", func(t *testing.T) {
			server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("server error"))
			}))
			defer server1.Close()

			txHex := "0100000001"
			server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(txHex))
			}))
			defer server2.Close()

			sources := []TxSource{
				{
					name:      "test1",
					asset:     AssetDCR,
					baseURL:   server1.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
				{
					name:      "test2",
					asset:     AssetDCR,
					baseURL:   server2.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
			}
			client := NewTxFetcher(sources)

			txBytes, err := client.FetchTx(context.Background(), AssetDCR, "abc123")
			if err != nil {
				t.Fatalf("FetchTx failed: %v", err)
			}

			expected, _ := hex.DecodeString(txHex)
			if len(txBytes) != len(expected) {
				t.Errorf("expected %d bytes, got %d", len(expected), len(txBytes))
			}
		})

		t.Run("InvalidHex", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("not valid hex!@#$"))
			}))
			defer server.Close()

			sources := []TxSource{
				{
					name:      "test",
					asset:     AssetDCR,
					baseURL:   server.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
			}
			client := NewTxFetcher(sources)

			_, err := client.FetchTx(context.Background(), AssetDCR, "abc123")
			if err == nil {
				t.Fatal("expected error for invalid hex")
			}
		})

		t.Run("Timeout", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(2 * time.Second)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			sources := []TxSource{
				{
					name:      "test",
					asset:     AssetDCR,
					baseURL:   server.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
			}
			client := NewTxFetcher(sources)

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			_, err := client.FetchTx(ctx, AssetDCR, "abc123")
			if err == nil {
				t.Fatal("expected timeout error")
			}
		})

		t.Run("ContextCanceled", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(2 * time.Second)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			sources := []TxSource{
				{
					name:      "test",
					asset:     AssetDCR,
					baseURL:   server.URL,
					txHexPath: "/tx/hex/%s",
					rateLimit: 0,
				},
			}
			client := NewTxFetcher(sources)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := client.FetchTx(ctx, AssetDCR, "abc123")
			if err == nil {
				t.Fatal("expected context deadline exceeded error")
			}
		})

		t.Run("UnsupportedAsset", func(t *testing.T) {
			client := NewTxFetcher(nil)

			_, err := client.FetchTx(context.Background(), 999, "abc123")
			if err == nil {
				t.Fatal("expected error for unsupported asset")
			}
		})
	})

	t.Run("Initialization", func(t *testing.T) {
		t.Run("DefaultConfigs", func(t *testing.T) {
			client := NewTxFetcher(nil)

			dcrExplorers, ok := client.txSources[AssetDCR]
			if !ok || len(dcrExplorers) == 0 {
				t.Fatal("DCR explorers not configured")
			}

			btcExplorers, ok := client.txSources[AssetBTC]
			if !ok || len(btcExplorers) == 0 {
				t.Fatal("BTC explorers not configured")
			}

			if len(dcrExplorers) < 2 {
				t.Errorf("expected at least 2 DCR explorers, got %d", len(dcrExplorers))
			}

			if len(btcExplorers) < 2 {
				t.Errorf("expected at least 2 BTC explorers, got %d", len(btcExplorers))
			}
		})

		t.Run("CustomSources", func(t *testing.T) {
			customSources := []TxSource{
				{
					name:      "custom",
					asset:     99,
					baseURL:   "https://custom.example.com",
					txHexPath: "/api/tx/%s",
					rateLimit: 50 * time.Millisecond,
				},
			}
			client := NewTxFetcher(customSources)

			_, ok := client.txSources[99]
			if !ok {
				t.Fatal("custom explorer source not set")
			}
		})
	})

	t.Run("Live", func(t *testing.T) {
		t.Run("FetchBTC", func(t *testing.T) {
			client := NewTxFetcher(nil)

			// Bitcoin Pizza transaction from May 22, 2010
			txid := "a1075db55d416d3ca199f55b6084e2115b9345e16c5cf302fc80e9d5fbf5d48d"
			txBytes, err := client.FetchTx(context.Background(), AssetBTC, txid)
			if err != nil {
				t.Fatalf("failed to fetch BTC transaction: %v", err)
			}

			if len(txBytes) == 0 {
				t.Fatal("expected non-empty transaction bytes")
			}
		})

		t.Run("FetchDCR", func(t *testing.T) {
			client := NewTxFetcher(nil)

			// Known Decred mainnet transaction
			txid := "c655ccbf4ef501d0d25dce57a58e639d536b7c36fdece01fd29c930f0f6858cc"
			txBytes, err := client.FetchTx(context.Background(), AssetDCR, txid)
			if err != nil {
				t.Fatalf("failed to fetch DCR transaction: %v", err)
			}

			if len(txBytes) == 0 {
				t.Fatal("expected non-empty transaction bytes")
			}
		})
	})
}
