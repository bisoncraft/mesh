package tatanka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestWhitelistSaveLoad(t *testing.T) {
	// Create test peer IDs and addresses
	peerID1 := randomPeerID(t)
	peerID2 := randomPeerID(t)

	addr1, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		t.Fatalf("Failed to create multiaddr 1: %v", err)
	}
	addr2, err := ma.NewMultiaddr("/ip6/::1/tcp/5678")
	if err != nil {
		t.Fatalf("Failed to create multiaddr 2: %v", err)
	}

	originalWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: peerID1, Addrs: []ma.Multiaddr{addr1}},
			{ID: peerID2, Addrs: []ma.Multiaddr{addr2}},
		},
	}

	whitelistFile := originalWhitelist.toFile()
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_whitelist.json")

	data, err := json.Marshal(whitelistFile)
	if err != nil {
		t.Fatalf("Failed to marshal whitelist to JSON: %v", err)
	}

	err = os.WriteFile(tempFile, data, 0644)
	if err != nil {
		t.Fatalf("Failed to write whitelist to file: %v", err)
	}

	loadedWhitelist, err := loadWhitelist(tempFile)
	if err != nil {
		t.Fatalf("Failed to load whitelist from file: %v", err)
	}

	if len(loadedWhitelist.peers) != len(originalWhitelist.peers) {
		t.Errorf("Expected %d peers, got %d", len(originalWhitelist.peers), len(loadedWhitelist.peers))
	}

	if !reflect.DeepEqual(loadedWhitelist.peers, originalWhitelist.peers) {
		t.Errorf("Loaded whitelist does not match original whitelist")
	}
}
