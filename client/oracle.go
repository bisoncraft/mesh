package client

import (
	"context"
	"math/big"

	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"google.golang.org/protobuf/proto"
)

// SubscribeToPriceOracle subscribes to live price updates for ticker (e.g. "BTC").
// handler is called with the USD price as a float64 for each update received.
func (c *Client) SubscribeToPriceOracle(ctx context.Context, ticker string, handler func(float64)) error {
	return c.Subscribe(ctx, protocols.PriceTopic(ticker), func(event TopicEvent) {
		if event.Type != TopicEventData {
			return
		}
		var priceUpdate protocolsPb.ClientPriceUpdate
		if err := proto.Unmarshal(event.Data, &priceUpdate); err != nil {
			c.log.Errorf("Failed to unmarshal price update for %s: %v", ticker, err)
			return
		}
		handler(priceUpdate.Price)
	})
}

// SubscribeToFeeRateOracle subscribes to live fee rate updates for network (e.g. "BTC").
// handler is called with the fee rate as a *big.Int for each update received.
func (c *Client) SubscribeToFeeRateOracle(ctx context.Context, network string, handler func(*big.Int)) error {
	return c.Subscribe(ctx, protocols.FeeRateTopic(network), func(event TopicEvent) {
		if event.Type != TopicEventData {
			return
		}
		var feeRateUpdate protocolsPb.ClientFeeRateUpdate
		if err := proto.Unmarshal(event.Data, &feeRateUpdate); err != nil {
			c.log.Errorf("Failed to unmarshal fee rate update for %s: %v", network, err)
			return
		}
		handler(new(big.Int).SetBytes(feeRateUpdate.FeeRate))
	})
}

