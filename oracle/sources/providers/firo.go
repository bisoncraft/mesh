package providers

import (
	"context"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
)

// NewFiroOrgSource creates a Firo network fee rate source.
// Third-party explorer with ~1 req/sec limit (CoinExplorer).
// MinPeriod is 30s as a conservative default for third-party explorers.
func NewFiroOrgSource(client utils.HTTPClient, log slog.Logger) *utils.UnlimitedSource {
	url := "https://explorer.firo.org/insight-api-zcoin/utils/estimatefee"
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, client, url, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return firoOrgParser(resp.Body)
	}
	return utils.NewUnlimitedSource(utils.UnlimitedSourceConfig{
		Name:       "firo.org",
		Weight:     0.25, // Lower weight due to estimatesmartfee variability
		MinPeriod:  30 * time.Second,
		FetchRates: fetchRates,
	})
}

var firoOrgParser = estimateFeeParser("FIRO")
