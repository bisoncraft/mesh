package client

import (
	"context"
	"errors"
	"math/big"
	"os"
	"testing"

	"github.com/decred/slog"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"google.golang.org/protobuf/proto"
)

func TestSubscribeToPriceOracle(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("oracle_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	// Subscribe to price oracle for BTC.
	var receivedPrice float64
	err := c.SubscribeToPriceOracle(ctx, "BTC", func(price float64) {
		receivedPrice = price
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Verify subscribe call with correct topic.
	if len(mc.subscribeCalls) != 1 {
		t.Fatalf("expected 1 subscribe call, got %d", len(mc.subscribeCalls))
	}
	if mc.subscribeCalls[0] != "price.BTC" {
		t.Fatalf("expected topic %q, got %q", "price.BTC", mc.subscribeCalls[0])
	}

	// Send a price update (as the server does: direct ClientPriceUpdate, not wrapped).
	priceUpdate := &protocolsPb.ClientPriceUpdate{
		Price: 42000.5,
	}
	data, err := proto.Marshal(priceUpdate)
	if err != nil {
		t.Fatalf("failed to marshal price update: %v", err)
	}

	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_BROADCAST,
		Topic:       "price.BTC",
		Data:        data,
		Sender:      []byte(randomPeerID(t)),
	})

	// Verify handler was called with correct price.
	if receivedPrice != 42000.5 {
		t.Fatalf("expected price 42000.5, got %v", receivedPrice)
	}

	// Redundant subscription returns ErrRedundantSubscription.
	err = c.SubscribeToPriceOracle(ctx, "BTC", func(price float64) {})
	if !errors.Is(err, ErrRedundantSubscription) {
		t.Fatalf("expected ErrRedundantSubscription, got %v", err)
	}

	// Subscribe with network error.
	wantErr := errors.New("network error")
	mc.subscribeErr = wantErr
	err = c.SubscribeToPriceOracle(ctx, "ETH", func(price float64) {})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}

	// Topic remains registered even after wire error, for retry on reconnect.
	if _, err := c.topicRegistry.fetchHandler("price.ETH"); err != nil {
		t.Fatalf("price.ETH should remain registered after subscribe error for reconnect retry: %v", err)
	}
}

func TestSubscribeToFeeRateOracle(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("oracle_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	// Subscribe to fee rate oracle for BTC.
	var receivedFeeRate *big.Int
	err := c.SubscribeToFeeRateOracle(ctx, "BTC", func(feeRate *big.Int) {
		receivedFeeRate = feeRate
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Verify subscribe call with correct topic.
	if len(mc.subscribeCalls) != 1 {
		t.Fatalf("expected 1 subscribe call, got %d", len(mc.subscribeCalls))
	}
	if mc.subscribeCalls[0] != "fee_rate.BTC" {
		t.Fatalf("expected topic %q, got %q", "fee_rate.BTC", mc.subscribeCalls[0])
	}

	// Send a fee rate update (as the server does: direct ClientFeeRateUpdate, not wrapped).
	expectedFeeRate := big.NewInt(12345)
	feeRateUpdate := &protocolsPb.ClientFeeRateUpdate{
		FeeRate: expectedFeeRate.Bytes(),
	}
	data, err := proto.Marshal(feeRateUpdate)
	if err != nil {
		t.Fatalf("failed to marshal fee rate update: %v", err)
	}

	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_BROADCAST,
		Topic:       "fee_rate.BTC",
		Data:        data,
		Sender:      []byte(randomPeerID(t)),
	})

	// Verify handler was called with correct fee rate.
	if receivedFeeRate.Cmp(expectedFeeRate) != 0 {
		t.Fatalf("expected fee rate %v, got %v", expectedFeeRate, receivedFeeRate)
	}

	// Redundant subscription returns ErrRedundantSubscription.
	err = c.SubscribeToFeeRateOracle(ctx, "BTC", func(feeRate *big.Int) {})
	if !errors.Is(err, ErrRedundantSubscription) {
		t.Fatalf("expected ErrRedundantSubscription, got %v", err)
	}

	// Subscribe with network error.
	wantErr := errors.New("network error")
	mc.subscribeErr = wantErr
	err = c.SubscribeToFeeRateOracle(ctx, "ETH", func(feeRate *big.Int) {})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}

	// Topic remains registered even after wire error, for retry on reconnect.
	if _, err := c.topicRegistry.fetchHandler("fee_rate.ETH"); err != nil {
		t.Fatalf("fee_rate.ETH should remain registered after subscribe error for reconnect retry: %v", err)
	}
}

