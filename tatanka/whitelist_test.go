package tatanka

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestWhitelistSaveLoad(t *testing.T) {
	peerID1 := randomPeerID(t)
	peerID2 := randomPeerID(t)

	originalWhitelist := types.NewWhitelist([]peer.ID{peerID1, peerID2})

	path := filepath.Join(t.TempDir(), "test_whitelist.json")
	if err := saveWhitelist(path, originalWhitelist); err != nil {
		t.Fatalf("Failed to save whitelist: %v", err)
	}

	loadedWhitelist, err := loadWhitelist(path)
	if err != nil {
		t.Fatalf("Failed to load whitelist from file: %v", err)
	}

	if len(loadedWhitelist.PeerIDs) != len(originalWhitelist.PeerIDs) {
		t.Errorf("Expected %d peers, got %d", len(originalWhitelist.PeerIDs), len(loadedWhitelist.PeerIDs))
	}

	if !loadedWhitelist.Equals(originalWhitelist) {
		t.Error("Loaded whitelist does not match original whitelist")
	}
}

func TestFlexibleWhitelistMatch(t *testing.T) {
	id1 := randomPeerID(t)
	id2 := randomPeerID(t)
	id3 := randomPeerID(t)
	id4 := randomPeerID(t)

	wlA := types.NewWhitelist([]peer.ID{id1, id2})
	wlB := types.NewWhitelist([]peer.ID{id1, id2, id3})

	// Exact match: my current matches their current.
	if !flexibleWhitelistMatch(wlA, nil, wlA.PeerIDsBytes(), nil) {
		t.Error("Expected match: exact current↔current")
	}

	// Cross-match: my proposed matches their current.
	if !flexibleWhitelistMatch(wlA, wlB, wlB.PeerIDsBytes(), nil) {
		t.Error("Expected match: my proposed matches their current")
	}

	// Cross-match: my current matches their proposed.
	if !flexibleWhitelistMatch(wlA, nil, wlB.PeerIDsBytes(), wlA.PeerIDsBytes()) {
		t.Error("Expected match: my current matches their proposed")
	}

	// No match: completely different whitelists.
	wlC := types.NewWhitelist([]peer.ID{id3, id4})
	if flexibleWhitelistMatch(wlA, nil, wlC.PeerIDsBytes(), nil) {
		t.Error("Expected no match: completely different whitelists")
	}

	// Nil proposed on both sides: only current compared.
	if !flexibleWhitelistMatch(wlA, nil, wlA.PeerIDsBytes(), nil) {
		t.Error("Expected match: nil proposed, current matches")
	}

	// No match even with proposed.
	if flexibleWhitelistMatch(wlA, nil, wlC.PeerIDsBytes(), wlC.PeerIDsBytes()) {
		t.Error("Expected no match: proposed also different")
	}
}

func TestSaveAndLoadWhitelist(t *testing.T) {
	id1 := randomPeerID(t)
	id2 := randomPeerID(t)

	wl := types.NewWhitelist([]peer.ID{id1, id2})

	path := filepath.Join(t.TempDir(), "wl.json")
	if err := saveWhitelist(path, wl); err != nil {
		t.Fatalf("saveWhitelist: %v", err)
	}

	loaded, err := loadWhitelist(path)
	if err != nil {
		t.Fatalf("loadWhitelist: %v", err)
	}

	if !wl.Equals(loaded) {
		t.Error("Round-tripped whitelist peer IDs differ")
	}

	if len(loaded.PeerIDs) != 2 {
		t.Fatalf("Expected 2 peers, got %d", len(loaded.PeerIDs))
	}
}

func TestInitWhitelist(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "requires_local_peer_when_loading_existing_whitelist",
			run: func(t *testing.T) {
				dataDir := t.TempDir()
				localPeerID := randomPeerID(t)
				otherPeerID := randomPeerID(t)

				if err := saveWhitelist(filepath.Join(dataDir, whitelistFileName), types.NewWhitelist([]peer.ID{otherPeerID})); err != nil {
					t.Fatalf("saveWhitelist: %v", err)
				}

				_, err := initWhitelist(dataDir, nil, false, localPeerID)
				if err == nil {
					t.Fatal("Expected error when local peer is missing from whitelist")
				}
				if !strings.Contains(err.Error(), "not included in whitelist") {
					t.Fatalf("Expected missing-local-peer error, got: %v", err)
				}
			},
		},
		{
			name: "accepts_local_peer_in_force_update",
			run: func(t *testing.T) {
				dataDir := t.TempDir()
				localPeerID := randomPeerID(t)
				otherPeerID := randomPeerID(t)

				wl, err := initWhitelist(dataDir, []peer.ID{localPeerID, otherPeerID}, true, localPeerID)
				if err != nil {
					t.Fatalf("initWhitelist: %v", err)
				}
				if _, found := wl.PeerIDs[localPeerID]; !found {
					t.Fatal("Expected whitelist to include local peer")
				}
			},
		},
		{
			name: "force_update_does_not_save_without_local_peer",
			run: func(t *testing.T) {
				dataDir := t.TempDir()
				localPeerID := randomPeerID(t)
				existingPeerID := randomPeerID(t)
				missingLocalPeerID := randomPeerID(t)

				originalWL := types.NewWhitelist([]peer.ID{localPeerID, existingPeerID})
				wlPath := filepath.Join(dataDir, whitelistFileName)
				if err := saveWhitelist(wlPath, originalWL); err != nil {
					t.Fatalf("saveWhitelist: %v", err)
				}

				_, err := initWhitelist(dataDir, []peer.ID{missingLocalPeerID}, true, localPeerID)
				if err == nil {
					t.Fatal("Expected error when force-updating with whitelist missing local peer")
				}
				if !strings.Contains(err.Error(), "not included in whitelist") {
					t.Fatalf("Expected missing-local-peer error, got: %v", err)
				}

				savedWL, err := loadWhitelist(wlPath)
				if err != nil {
					t.Fatalf("loadWhitelist: %v", err)
				}
				if !savedWL.Equals(originalWL) {
					t.Fatal("Expected existing whitelist to remain unchanged after failed force update")
				}
			},
		},
		{
			name: "first_run_saves_provided_whitelist",
			run: func(t *testing.T) {
				dataDir := t.TempDir()
				localPeerID := randomPeerID(t)
				otherPeerID := randomPeerID(t)

				wl, err := initWhitelist(dataDir, []peer.ID{localPeerID, otherPeerID}, false, localPeerID)
				if err != nil {
					t.Fatalf("initWhitelist: %v", err)
				}
				if _, found := wl.PeerIDs[localPeerID]; !found {
					t.Fatal("Expected returned whitelist to include local peer")
				}

				savedWL, err := loadWhitelist(filepath.Join(dataDir, whitelistFileName))
				if err != nil {
					t.Fatalf("loadWhitelist: %v", err)
				}
				if !savedWL.Equals(wl) {
					t.Fatal("Expected first-run provided whitelist to be saved")
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}
