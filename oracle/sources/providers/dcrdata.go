package providers

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/big"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
)

// NewDcrdataSource creates a dcrdata fee rate source.
// Decred blocks average ~5 minutes. Self-hosted infrastructure with configurable rate limits.
// MinPeriod is 30s as a reasonable default for block-based fee estimates.
func NewDcrdataSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	url := "https://explorer.dcrdata.org/insight/api/utils/estimatefee?nbBlocks=2"
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, client, url, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return dcrdataParser(resp.Body)
	}
	return utils.NewUnlimitedSource(utils.UnlimitedSourceConfig{
		Name:       "dcrdata",
		MinPeriod:  30 * time.Second, // Block-based fee data, ~5 min blocks
		FetchRates: fetchRates,
	})
}

var dcrdataParser = estimateFeeParser("DCR")

func estimateFeeParser(network sources.Network) func(io.Reader) (*sources.RateInfo, error) {
	return func(r io.Reader) (*sources.RateInfo, error) {
		var resp map[string]float64
		if err := utils.StreamDecodeJSON(r, &resp); err != nil {
			return nil, err
		}
		rate, ok := resp["2"]
		if !ok || len(resp) != 1 {
			return nil, fmt.Errorf("unexpected response format: %+v", resp)
		}
		if rate <= 0 {
			return nil, fmt.Errorf("fee rate must be positive, got %v", rate)
		}
		return &sources.RateInfo{
			FeeRates: []*sources.FeeRateUpdate{{
				Network: network,
				FeeRate: uint64ToBigInt(uint64(math.Round(rate * 1e5))),
			}},
		}, nil
	}
}

func uint64ToBigInt(val uint64) *big.Int {
	return new(big.Int).SetUint64(val)
}
