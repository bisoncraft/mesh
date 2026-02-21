package tatanka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

func TestPeerstoreCacheSaveLoad(t *testing.T) {
	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatal(err)
	}

	hosts := mnet.Hosts()
	h := hosts[0]

	// Add known addresses for host 1 and host 2 to host 0's peerstore.
	addr1, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	addr2, _ := ma.NewMultiaddr("/ip4/5.6.7.8/tcp/5678")
	h.Peerstore().AddAddrs(hosts[1].ID(), []ma.Multiaddr{addr1}, 1<<62)
	h.Peerstore().AddAddrs(hosts[2].ID(), []ma.Multiaddr{addr2}, 1<<62)

	dir := t.TempDir()
	cachePath := filepath.Join(dir, "peerstore.json")

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	log.SetLevel(slog.LevelOff)

	getWhitelist := func() map[peer.ID]struct{} {
		return map[peer.ID]struct{}{
			hosts[1].ID(): {},
			hosts[2].ID(): {},
		}
	}
	cache := newPeerstoreCache(log, cachePath, h, getWhitelist)

	// Save with both peer IDs in the whitelist.
	cache.save()

	// Verify the file exists and has correct content.
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}

	var saved map[string][]string
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("Failed to unmarshal cache: %v", err)
	}

	if len(saved) != 2 {
		t.Fatalf("Expected 2 peers in cache, got %d", len(saved))
	}

	// Now load into a fresh host and verify addresses are added.
	mnet2, err := mocknet.WithNPeers(1)
	if err != nil {
		t.Fatal(err)
	}
	h2 := mnet2.Hosts()[0]

	cache2 := newPeerstoreCache(log, cachePath, h2, getWhitelist)
	cache2.load()

	addrs1 := h2.Peerstore().Addrs(hosts[1].ID())
	if len(addrs1) == 0 {
		t.Fatal("Expected addresses for host 1 after load")
	}
	addrs2 := h2.Peerstore().Addrs(hosts[2].ID())
	if len(addrs2) == 0 {
		t.Fatal("Expected addresses for host 2 after load")
	}
}

func TestPeerstoreCacheOnlyWhitelistPeers(t *testing.T) {
	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatal(err)
	}

	hosts := mnet.Hosts()
	h := hosts[0]

	addr1, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	addr2, _ := ma.NewMultiaddr("/ip4/5.6.7.8/tcp/5678")
	h.Peerstore().AddAddrs(hosts[1].ID(), []ma.Multiaddr{addr1}, 1<<62)
	h.Peerstore().AddAddrs(hosts[2].ID(), []ma.Multiaddr{addr2}, 1<<62)

	dir := t.TempDir()
	cachePath := filepath.Join(dir, "peerstore.json")

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	log.SetLevel(slog.LevelOff)

	// Only save host 1 (not host 2).
	cache := newPeerstoreCache(log, cachePath, h, func() map[peer.ID]struct{} {
		return map[peer.ID]struct{}{
			hosts[1].ID(): {},
		}
	})
	cache.save()

	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}

	var saved map[string][]string
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("Failed to unmarshal cache: %v", err)
	}

	if len(saved) != 1 {
		t.Fatalf("Expected 1 peer in cache, got %d", len(saved))
	}
	if _, ok := saved[hosts[1].ID().String()]; !ok {
		t.Fatal("Expected host 1 in cache")
	}
	if _, ok := saved[hosts[2].ID().String()]; ok {
		t.Fatal("Host 2 should not be in cache")
	}
}

func TestPeerstoreCacheSeedAddresses(t *testing.T) {
	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatal(err)
	}

	hosts := mnet.Hosts()
	h := hosts[0]

	// Build a full p2p multiaddr targeting hosts[1].
	addrStr := "/ip4/10.0.0.1/tcp/9999/p2p/" + hosts[1].ID().String()
	if err := seedBootstrapAddrs(h, []string{addrStr}); err != nil {
		t.Fatalf("seedBootstrapAddrs failed: %v", err)
	}

	// Verify address was added to peerstore.
	addrs := h.Peerstore().Addrs(hosts[1].ID())
	if len(addrs) == 0 {
		t.Fatal("Expected addresses after seedBootstrapAddrs")
	}

	expected, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/9999")
	found := false
	for _, a := range addrs {
		if a.Equal(expected) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Seeded address not found in peerstore")
	}
}

func TestPeerstoreCacheLoadNoFile(t *testing.T) {
	mnet, err := mocknet.WithNPeers(1)
	if err != nil {
		t.Fatal(err)
	}
	h := mnet.Hosts()[0]

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	log.SetLevel(slog.LevelOff)

	cache := newPeerstoreCache(log, "/nonexistent/path/peerstore.json", h, nil)
	// Should not panic or error.
	cache.load()
}
