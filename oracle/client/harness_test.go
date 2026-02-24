//go:build harness

package client

// Client tests expect the tatanka test harness to be running a node.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sync"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/crypto"
)

func fetchNodeAddr(index int) (string, error) {
	curUser, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("failed to resolve current user: %w", err)
	}

	whitelistPath := fmt.Sprintf("%s/%s", curUser.HomeDir, ".tatanka-test/whitelist.json")
	whitelist := struct {
		Peers []struct {
			ID      string `json:"id"`
			Address string `json:"address"`
		} `json:"peers"`
	}{}

	data, err := os.ReadFile(whitelistPath)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(data, &whitelist); err != nil {
		return "", err
	}

	if index < 0 || index >= len(whitelist.Peers) {
		return "", fmt.Errorf("no node found at provided index: %d", index)
	}

	node := whitelist.Peers[index]
	if node.Address == "" {
		return "", fmt.Errorf("peer at index %d missing address", index)
	}
	if node.ID == "" {
		return "", fmt.Errorf("peer at index %d missing peer ID", index)
	}

	return fmt.Sprintf("%s/p2p/%s", node.Address, node.ID), nil
}

// go test -v -tags=harness -run=TestOracleClientIntegration ./oracle/client
func TestOracleClientIntegration(t *testing.T) {
	logBackend := slog.NewBackend(os.Stdout)
	loggerMesh := logBackend.Logger("mesh")
	loggerMesh.SetLevel(slog.LevelDebug)
	loggerOracle := logBackend.Logger("oracle")
	loggerOracle.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeAddr, err := fetchNodeAddr(0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("node address is: %s", nodeAddr)

	meshPriv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	mc, err := meshclient.NewClient(&meshclient.Config{
		Port:            12368,
		PrivateKey:      meshPriv,
		RemotePeerAddrs: []string{nodeAddr},
		Logger:          loggerMesh,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := mc.Run(ctx, nil); err != nil && err != context.Canceled {
			loggerMesh.Errorf("mesh client error: %v", err)
		}
	}()

	oracleCfg := &Config{
		MeshClient: mc,
		Assets:     []string{"BTC", "ETH"},
		Logger:     loggerOracle,
	}

	oc, err := NewClient(oracleCfg)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := oc.Run(ctx); err != nil && err != context.Canceled {
			loggerOracle.Errorf("oracle client error: %v", err)
		}
	}()

	// Wait briefly for the oracle client to subscribe to oracle topics.
	// The server will send current oracle state when the client subscribes.
	time.Sleep(2 * time.Second)

	// Verify that oracle client receives prices from the mesh network
	// (Values may come from network history, not necessarily our published values)
	price, ok := oc.GetPrice("BTC")
	if !ok {
		t.Fatal("expected BTC price to be cached")
	}
	if price <= 0 {
		t.Errorf("expected positive BTC price, got %.2f", price)
	}
	t.Logf("Successfully received BTC price from network: %.2f", price)

	price, ok = oc.GetPrice("ETH")
	if !ok {
		t.Fatal("expected ETH price to be cached")
	}
	if price <= 0 {
		t.Errorf("expected positive ETH price, got %.2f", price)
	}
	t.Logf("Successfully received ETH price from network: %.2f", price)

	feeRate, ok := oc.GetFeeRate("BTC")
	if !ok {
		t.Fatal("expected BTC fee rate to be cached")
	}
	if feeRate.Sign() <= 0 {
		t.Errorf("expected positive BTC fee rate, got %s", feeRate.String())
	}
	t.Logf("Successfully received BTC fee rate from network: %s", feeRate.String())

	cancel()
	wg.Wait()
}
