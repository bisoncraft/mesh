package client

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"

	"github.com/decred/slog"
	"google.golang.org/protobuf/proto"

	"github.com/bisoncraft/mesh/bond"
	meshclient "github.com/bisoncraft/mesh/client"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/protocols"
	pb "github.com/bisoncraft/mesh/protocols/pb"
)

// meshClient defines the requirements for an oracle mesh client.
type meshClient interface {
	Subscribe(ctx context.Context, topic string, handler meshclient.TopicHandler) error
	Run(ctx context.Context, bonds []*bond.BondParams) error
	WaitForConnection(ctx context.Context) error
}

// Config configures an oracle Client.
type Config struct {
	MeshClient meshClient
	Assets     []string // e.g., "BTC", "ETH", "LTC" (used for both price and fee rate)
	Logger     slog.Logger
}

// Verify validates the Config fields.
func (cfg *Config) Verify() error {
	var err error

	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if cfg.MeshClient == nil {
		err = errors.Join(err, errors.New("mesh client cannot be nil"))
	}
	if cfg.Logger == nil {
		err = errors.Join(err, errors.New("logger cannot be nil"))
	}
	if len(cfg.Assets) == 0 {
		err = errors.Join(err, errors.New("assets cannot be empty"))
	}
	if slices.Contains(cfg.Assets, "") {
		err = errors.Join(err, errors.New("asset names cannot be empty"))
	}

	return err
}

// Client subscribes to real-time oracle prices and fee rates from the mesh.
type Client struct {
	cfg *Config
	mc  meshClient
	log slog.Logger

	prices   map[oracle.Ticker]float64
	feeRates map[oracle.Network]*big.Int
	mtx      sync.RWMutex
}

// NewClient creates a new oracle client.
func NewClient(cfg *Config) (*Client, error) {
	if err := cfg.Verify(); err != nil {
		return nil, err
	}

	return &Client{
		cfg:      cfg,
		mc:       cfg.MeshClient,
		prices:   make(map[oracle.Ticker]float64),
		feeRates: make(map[oracle.Network]*big.Int),
		log:      cfg.Logger,
	}, nil
}

// Run subscribes to oracle topics and blocks until ctx is done.
func (c *Client) Run(ctx context.Context) error {
	if err := c.mc.WaitForConnection(ctx); err != nil {
		return err
	}

	err := c.subscribe(ctx)
	if err != nil {
		return err
	}

	<-ctx.Done()

	return nil
}

func (c *Client) subscribe(ctx context.Context) error {
	for _, asset := range c.cfg.Assets {
		asset := asset
		ticker := oracle.Ticker(asset)
		network := oracle.Network(asset)

		priceHandler := func(evt meshclient.TopicEvent) {
			if evt.Type != meshclient.TopicEventData {
				return
			}

			var update pb.ClientPriceUpdate
			if err := proto.Unmarshal(evt.Data, &update); err != nil {
				c.log.Errorf("Failed to unmarshal price update for %s: %v", asset, err)
				return
			}

			c.mtx.Lock()
			c.prices[ticker] = update.Price
			c.mtx.Unlock()
		}

		priceTopic := protocols.PriceTopicPrefix + asset
		if err := c.mc.Subscribe(ctx, priceTopic, priceHandler); err != nil {
			return fmt.Errorf("failed to subscribe to price topic %q: %w", priceTopic, err)
		}

		feeHandler := func(evt meshclient.TopicEvent) {
			if evt.Type != meshclient.TopicEventData {
				return
			}

			var update pb.ClientFeeRateUpdate
			if err := proto.Unmarshal(evt.Data, &update); err != nil {
				c.log.Errorf("Failed to unmarshal fee rate update for %s: %v", asset, err)
				return
			}

			c.mtx.Lock()
			c.feeRates[network] = new(big.Int).SetBytes(update.FeeRate)
			c.mtx.Unlock()
		}

		feeTopic := protocols.FeeRateTopicPrefix + asset
		if err := c.mc.Subscribe(ctx, feeTopic, feeHandler); err != nil {
			return fmt.Errorf("failed to subscribe to fee rate topic %q: %w", feeTopic, err)
		}
	}

	return nil
}

// GetPrice returns the latest cached USD price for ticker.
func (c *Client) GetPrice(ticker oracle.Ticker) (price float64, ok bool) {
	c.mtx.RLock()
	price, ok = c.prices[ticker]
	c.mtx.RUnlock()
	return
}

// GetFeeRate returns the latest cached fee rate for network.
func (c *Client) GetFeeRate(network oracle.Network) (feeRate *big.Int, ok bool) {
	c.mtx.RLock()
	cachedRate, ok := c.feeRates[network]
	c.mtx.RUnlock()

	if !ok {
		return nil, false
	}

	// Return a copy to prevent caller mutations from corrupting the cache.
	return new(big.Int).Set(cachedRate), true
}
