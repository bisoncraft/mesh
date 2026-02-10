package providers

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
)

// NewMempoolDotSpaceSource creates a mempool.space Bitcoin fee rate source.
// Real-time fee data updated per block. Rate limits undisclosed but enforced.
// MinPeriod is 1 minute since data is real-time and they recommend self-hosting
// for heavy use.
func NewMempoolDotSpaceSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	url := "https://mempool.space/api/v1/fees/recommended"
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, client, url, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return mempoolDotSpaceParser(resp.Body)
	}
	return utils.NewUnlimitedSource(utils.UnlimitedSourceConfig{
		Name:       "btc.mempooldotspace",
		MinPeriod:  time.Minute,
		FetchRates: fetchRates,
	})
}

func mempoolDotSpaceParser(r io.Reader) (*sources.RateInfo, error) {
	var resp struct {
		FastestFee uint64 `json:"fastestFee"`
	}
	if err := utils.StreamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.FastestFee == 0 {
		return nil, fmt.Errorf("zero fee rate returned")
	}
	return &sources.RateInfo{
		FeeRates: []*sources.FeeRateUpdate{{
			Network: "BTC",
			FeeRate: uint64ToBigInt(resp.FastestFee),
		}},
	}, nil
}
