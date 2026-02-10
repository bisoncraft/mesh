package providers

import (
	"context"
	"io"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
)

// NewCoinpaprikaSource creates a Coinpaprika price source.
func NewCoinpaprikaSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	url := "https://api.coinpaprika.com/v1/tickers"
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, client, url, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return coinpaprikaParser(resp.Body)
	}
	return utils.NewUnlimitedSource(utils.UnlimitedSourceConfig{
		Name: "coinpaprika",
		// Free tier updates data up to every 10 minutes, and
		// allows 20,000 monthly requests (2m10s interval).
		MinPeriod:  (5 * time.Minute) / 2,
		FetchRates: fetchRates,
	})
}

func coinpaprikaParser(r io.Reader) (*sources.RateInfo, error) {
	var prices []*struct {
		Symbol string `json:"symbol"`
		Quotes struct {
			USD struct {
				Price float64 `json:"price"`
			} `json:"USD"`
		} `json:"quotes"`
	}
	if err := utils.StreamDecodeJSON(r, &prices); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(prices))
	us := make([]*sources.PriceUpdate, 0, len(prices))
	for _, p := range prices {
		if seen[p.Symbol] {
			continue
		}
		seen[p.Symbol] = true
		us = append(us, &sources.PriceUpdate{
			Ticker: sources.Ticker(p.Symbol),
			Price:  p.Quotes.USD.Price,
		})
	}
	return &sources.RateInfo{Prices: us}, nil
}
