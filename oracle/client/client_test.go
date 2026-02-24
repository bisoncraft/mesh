package client

import (
	"context"
	"io"
	"math/big"
	"sync"
	"testing"

	"github.com/decred/slog"
	"google.golang.org/protobuf/proto"

	"github.com/bisoncraft/mesh/bond"
	meshclient "github.com/bisoncraft/mesh/client"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/protocols"
	pb "github.com/bisoncraft/mesh/protocols/pb"
)

type mockMeshClient struct {
	handlers         map[string]meshclient.TopicHandler
	subscribeErrFunc func() error
	runCh            chan struct{}
	connectedCh      chan struct{}
	mu               sync.Mutex
}

func newMockMeshClient() *mockMeshClient {
	return &mockMeshClient{
		handlers:    make(map[string]meshclient.TopicHandler),
		runCh:       make(chan struct{}),
		connectedCh: make(chan struct{}),
	}
}

func (m *mockMeshClient) Subscribe(ctx context.Context, topic string, handler meshclient.TopicHandler) error {
	if m.subscribeErrFunc != nil {
		if err := m.subscribeErrFunc(); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.handlers[topic] = handler
	m.mu.Unlock()
	return nil
}

func (m *mockMeshClient) Run(ctx context.Context, bonds []*bond.BondParams) error {
	close(m.runCh)
	close(m.connectedCh)
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockMeshClient) WaitForConnection(ctx context.Context) error {
	select {
	case <-m.connectedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mockMeshClient) deliverEvent(topic string, evt meshclient.TopicEvent) {
	m.mu.Lock()
	handler, ok := m.handlers[topic]
	m.mu.Unlock()

	if ok {
		handler(evt)
	}
}

func (m *mockMeshClient) deliverPriceUpdate(asset string, price float64) {
	topic := protocols.PriceTopicPrefix + asset
	update := &pb.ClientPriceUpdate{Price: price}
	data, _ := proto.Marshal(update)

	evt := meshclient.TopicEvent{
		Type: meshclient.TopicEventData,
		Data: data,
	}
	m.deliverEvent(topic, evt)
}

func (m *mockMeshClient) deliverFeeRateUpdate(asset string, feeRate *big.Int) {
	topic := protocols.FeeRateTopicPrefix + asset
	update := &pb.ClientFeeRateUpdate{FeeRate: feeRate.Bytes()}
	data, _ := proto.Marshal(update)

	evt := meshclient.TopicEvent{
		Type: meshclient.TopicEventData,
		Data: data,
	}
	m.deliverEvent(topic, evt)
}

func newTestLogger() slog.Logger {
	backend := slog.NewBackend(io.Discard)
	return backend.Logger("test")
}

func testConfig(assets []string) *Config {
	return &Config{
		MeshClient: newMockMeshClient(),
		Assets:     assets,
		Logger:     newTestLogger(),
	}
}

func setupClient(t *testing.T, mc *mockMeshClient, assets []string) *Client {
	cfg := &Config{
		MeshClient: mc,
		Assets:     assets,
		Logger:     newTestLogger(),
	}
	oc, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	if err := oc.subscribe(context.Background()); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	return oc
}

func TestClient(t *testing.T) {
	t.Run("ConfigValidation", testConfigValidation)
	t.Run("GetPrice", testGetPrice)
	t.Run("GetFeeRate", testGetFeeRate)
	t.Run("MultipleAssets", testMultipleAssets)
	t.Run("FeeRateCopy", testFeeRateCopy)
	t.Run("ConcurrentAccess", testConcurrentAccess)
	t.Run("EventFiltering", testEventFiltering)
	t.Run("MalformedData", testMalformedData)
}

func testConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
		{
			name: "nil mesh client",
			cfg: &Config{
				Assets: []string{"BTC"},
				Logger: newTestLogger(),
			},
			wantErr: true,
		},
		{
			name: "nil logger",
			cfg: &Config{
				MeshClient: newMockMeshClient(),
				Assets:     []string{"BTC"},
			},
			wantErr: true,
		},
		{
			name: "empty assets",
			cfg: &Config{
				MeshClient: newMockMeshClient(),
				Logger:     newTestLogger(),
				Assets:     []string{},
			},
			wantErr: true,
		},
		{
			name: "empty asset string",
			cfg: &Config{
				MeshClient: newMockMeshClient(),
				Logger:     newTestLogger(),
				Assets:     []string{"BTC", "", "ETH"},
			},
			wantErr: true,
		},
		{
			name:    "valid config",
			cfg:     testConfig([]string{"BTC"}),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func testGetPrice(t *testing.T) {
	t.Run("NoData", func(t *testing.T) {
		mc := newMockMeshClient()
		oc := setupClient(t, mc, []string{"BTC", "ETH"})

		for _, asset := range []string{"BTC", "ETH", "LTC"} {
			t.Run(asset, func(t *testing.T) {
				_, ok := oc.GetPrice(oracle.Ticker(asset))
				if ok {
					t.Error("GetPrice should return ok=false before any update")
				}
			})
		}
	})

	t.Run("Update", func(t *testing.T) {
		tests := []struct {
			asset string
			price float64
		}{
			{"BTC", 45123.50},
			{"ETH", 2500.00},
			{"LTC", 150.00},
		}

		for _, tt := range tests {
			t.Run(tt.asset, func(t *testing.T) {
				mc := newMockMeshClient()
				oc := setupClient(t, mc, []string{tt.asset})

				mc.deliverPriceUpdate(tt.asset, tt.price)

				price, ok := oc.GetPrice(oracle.Ticker(tt.asset))
				if !ok {
					t.Error("GetPrice should return ok=true after update")
				}
				if price != tt.price {
					t.Errorf("GetPrice = %v, want %v", price, tt.price)
				}
			})
		}
	})
}

func testGetFeeRate(t *testing.T) {
	t.Run("NoData", func(t *testing.T) {
		mc := newMockMeshClient()
		oc := setupClient(t, mc, []string{"BTC", "ETH"})

		for _, asset := range []string{"BTC", "ETH", "LTC"} {
			t.Run(asset, func(t *testing.T) {
				_, ok := oc.GetFeeRate(oracle.Network(asset))
				if ok {
					t.Error("GetFeeRate should return ok=false before any update")
				}
			})
		}
	})

	t.Run("Update", func(t *testing.T) {
		tests := []struct {
			asset   string
			feeRate *big.Int
		}{
			{"BTC", big.NewInt(12500)},
			{"ETH", big.NewInt(50)},
			{"LTC", big.NewInt(100)},
		}

		for _, tt := range tests {
			t.Run(tt.asset, func(t *testing.T) {
				mc := newMockMeshClient()
				oc := setupClient(t, mc, []string{tt.asset})

				mc.deliverFeeRateUpdate(tt.asset, tt.feeRate)

				feeRate, ok := oc.GetFeeRate(oracle.Network(tt.asset))
				if !ok {
					t.Error("GetFeeRate should return ok=true after update")
				}
				if feeRate.Cmp(tt.feeRate) != 0 {
					t.Errorf("GetFeeRate = %v, want %v", feeRate, tt.feeRate)
				}
			})
		}
	})
}

func testMultipleAssets(t *testing.T) {
	mc := newMockMeshClient()
	assets := []string{"BTC", "ETH", "LTC"}
	oc := setupClient(t, mc, assets)

	prices := map[string]float64{
		"BTC": 45000.0,
		"ETH": 2500.0,
		"LTC": 150.0,
	}
	for asset, price := range prices {
		mc.deliverPriceUpdate(asset, price)
	}

	feeRates := map[string]*big.Int{
		"BTC": big.NewInt(10000),
		"ETH": big.NewInt(50),
	}
	for asset, feeRate := range feeRates {
		mc.deliverFeeRateUpdate(asset, feeRate)
	}

	t.Run("Prices", func(t *testing.T) {
		for asset, expectedPrice := range prices {
			t.Run(asset, func(t *testing.T) {
				price, ok := oc.GetPrice(oracle.Ticker(asset))
				if !ok || price != expectedPrice {
					t.Errorf("GetPrice(%s) = (%v, %v), want (%v, true)", asset, price, ok, expectedPrice)
				}
			})
		}
	})

	t.Run("FeeRates", func(t *testing.T) {
		for asset, expectedFeeRate := range feeRates {
			t.Run(asset, func(t *testing.T) {
				feeRate, ok := oc.GetFeeRate(oracle.Network(asset))
				if !ok || feeRate.Cmp(expectedFeeRate) != 0 {
					t.Errorf("GetFeeRate(%s) = (%v, %v), want (%v, true)", asset, feeRate, ok, expectedFeeRate)
				}
			})
		}
	})
}

func testFeeRateCopy(t *testing.T) {
	mc := newMockMeshClient()
	oc := setupClient(t, mc, []string{"BTC"})

	expectedFeeRate := big.NewInt(12500)
	mc.deliverFeeRateUpdate("BTC", expectedFeeRate)

	feeRate1, ok := oc.GetFeeRate("BTC")
	if !ok {
		t.Fatalf("GetFeeRate should return ok=true")
	}
	feeRate1.Add(feeRate1, big.NewInt(1000))

	feeRate2, ok := oc.GetFeeRate("BTC")
	if !ok {
		t.Fatalf("GetFeeRate should return ok=true")
	}

	if feeRate2.Cmp(expectedFeeRate) != 0 {
		t.Errorf("Cache was mutated: got %v, want %v", feeRate2, expectedFeeRate)
	}
}

func testConcurrentAccess(t *testing.T) {
	mc := newMockMeshClient()
	oc := setupClient(t, mc, []string{"BTC", "ETH"})

	var wg sync.WaitGroup

	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			price := float64(45000 + idx*100)
			mc.deliverPriceUpdate("BTC", price)
			mc.deliverPriceUpdate("ETH", price/20)
			mc.deliverFeeRateUpdate("BTC", big.NewInt(int64(10000+idx*100)))
		}(i)
	}

	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = oc.GetPrice("BTC")
			_, _ = oc.GetPrice("ETH")
			_, _ = oc.GetFeeRate("BTC")
		}()
	}

	wg.Wait()
}

func testEventFiltering(t *testing.T) {
	mc := newMockMeshClient()
	oc := setupClient(t, mc, []string{"BTC"})

	nonDataEvents := []meshclient.TopicEventType{
		meshclient.TopicEventPeerSubscribed,
		meshclient.TopicEventPeerUnsubscribed,
	}

	for _, eventType := range nonDataEvents {
		topic := protocols.PriceTopicPrefix + "BTC"
		evt := meshclient.TopicEvent{
			Type: eventType,
			Data: nil,
		}
		mc.deliverEvent(topic, evt)

		_, ok := oc.GetPrice("BTC")
		if ok {
			t.Errorf("Non-data event (type %v) should be ignored", eventType)
		}
	}

	mc.deliverPriceUpdate("BTC", 45000.0)
	_, ok := oc.GetPrice("BTC")
	if !ok {
		t.Error("Valid data event should be processed")
	}
}

func testMalformedData(t *testing.T) {
	mc := newMockMeshClient()
	oc := setupClient(t, mc, []string{"BTC"})

	t.Run("MalformedPrice", func(t *testing.T) {
		priceTopic := protocols.PriceTopicPrefix + "BTC"
		evt := meshclient.TopicEvent{
			Type: meshclient.TopicEventData,
			Data: []byte{0xFF, 0xFF, 0xFF},
		}
		mc.deliverEvent(priceTopic, evt)

		_, ok := oc.GetPrice("BTC")
		if ok {
			t.Error("Malformed price data should be ignored")
		}

		mc.deliverPriceUpdate("BTC", 45000.0)
		price, ok := oc.GetPrice("BTC")
		if !ok || price != 45000.0 {
			t.Errorf("Valid price update failed: got (%v, %v), want (45000.0, true)", price, ok)
		}
	})

	t.Run("MalformedFeeRate", func(t *testing.T) {
		feeTopic := protocols.FeeRateTopicPrefix + "BTC"
		evt := meshclient.TopicEvent{
			Type: meshclient.TopicEventData,
			Data: []byte{0xFF, 0xFF, 0xFF},
		}
		mc.deliverEvent(feeTopic, evt)

		_, ok := oc.GetFeeRate("BTC")
		if ok {
			t.Error("Malformed fee rate data should be ignored")
		}

		mc.deliverFeeRateUpdate("BTC", big.NewInt(12500))
		feeRate, ok := oc.GetFeeRate("BTC")
		if !ok || feeRate.Cmp(big.NewInt(12500)) != 0 {
			t.Errorf("Valid fee rate update failed: got (%v, %v), want (12500, true)", feeRate, ok)
		}
	})
}
