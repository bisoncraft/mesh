package providers

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
)

func newBitcoreSource(coin string, client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	coin = strings.ToUpper(coin)
	name := fmt.Sprintf("%s.bitcore", strings.ToLower(coin))

	url := fmt.Sprintf("https://api.bitcore.io/api/%s/mainnet/fee/2", coin)
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, client, url, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return parseBitcoreResponse(sources.Network(coin), resp.Body)
	}

	return utils.NewUnlimitedSource(utils.UnlimitedSourceConfig{
		Name:       name,
		Weight:     0.25, // Lower weight due to estimatesmartfee variability
		MinPeriod:  30 * time.Second,
		FetchRates: fetchRates,
	})
}

func parseBitcoreResponse(netName sources.Network, r io.Reader) (*sources.RateInfo, error) {
	var resp struct {
		RatePerKB float64 `json:"feerate"`
	}
	if err := utils.StreamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.RatePerKB <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return &sources.RateInfo{
		FeeRates: []*sources.FeeRateUpdate{{
			Network: netName,
			FeeRate: uint64ToBigInt(uint64(math.Round(resp.RatePerKB * 1e5))),
		}},
	}, nil
}

// NewBitcoreBitcoinCashSource creates a Bitcore Bitcoin Cash fee rate source.
func NewBitcoreBitcoinCashSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	return newBitcoreSource("BCH", client, log)
}

// NewBitcoreDogecoinSource creates a Bitcore Dogecoin fee rate source.
func NewBitcoreDogecoinSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	return newBitcoreSource("DOGE", client, log)
}

// NewBitcoreLitecoinSource creates a Bitcore Litecoin fee rate source.
func NewBitcoreLitecoinSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	return newBitcoreSource("LTC", client, log)
}
