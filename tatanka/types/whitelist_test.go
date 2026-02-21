package types

import (
	"encoding/json"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func randomPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("Failed to create peer ID: %v", err)
	}
	return id
}

func TestWhitelistEquals(t *testing.T) {
	id1 := randomPeerID(t)
	id2 := randomPeerID(t)
	id3 := randomPeerID(t)

	wl1 := NewWhitelist([]peer.ID{id1, id2})
	wl2 := NewWhitelist([]peer.ID{id2, id1}) // same IDs, different order
	wl3 := NewWhitelist([]peer.ID{id1, id3}) // different set
	wl4 := NewWhitelist([]peer.ID{id1})      // subset

	if !wl1.Equals(wl2) {
		t.Error("Expected equal peer IDs (same set, different order)")
	}
	if wl1.Equals(wl3) {
		t.Error("Expected unequal peer IDs (different set)")
	}
	if wl1.Equals(wl4) {
		t.Error("Expected unequal peer IDs (subset)")
	}
}

func TestPeerIDBytesRoundTrip(t *testing.T) {
	id1 := randomPeerID(t)
	id2 := randomPeerID(t)
	id3 := randomPeerID(t)

	wl := NewWhitelist([]peer.ID{id1, id2, id3})
	decoded := DecodePeerIDBytes(wl.PeerIDsBytes())

	if !wl.Equals(decoded) {
		t.Error("Round-tripped whitelist does not match original")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	id1 := randomPeerID(t)
	id2 := randomPeerID(t)
	id3 := randomPeerID(t)

	wl := NewWhitelist([]peer.ID{id1, id2, id3})

	data, err := json.Marshal(wl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded Whitelist
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !wl.Equals(&decoded) {
		t.Error("Round-tripped whitelist does not match original")
	}
}
