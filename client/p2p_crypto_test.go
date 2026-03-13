package client

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func mockPeerID() peer.ID {
	b := make([]byte, 32)
	rand.Read(b)
	return peer.ID(fmt.Sprintf("peer-%x", b))
}

// setupPairedManagers creates two encryption managers where the handshake
// request is sent from one to the other.
func setupPairedManagers() (alice encryptionManager, aliceID peer.ID, bob encryptionManager, bobID peer.ID) {
	idA := mockPeerID()
	idB := mockPeerID()

	var emA, emB encryptionManager

	sendHandshakeA := func(ctx context.Context, p peer.ID, nonce keyID, pubKey []byte) ([]byte, error) {
		if p != idB {
			return nil, fmt.Errorf("Alice expected to talk to Bob, got %s", p)
		}
		return emB.handleHandshakeRequest(idA, nonce[:], pubKey)
	}

	sendHandshakeB := func(ctx context.Context, p peer.ID, nonce keyID, pubKey []byte) ([]byte, error) {
		if p != idA {
			return nil, fmt.Errorf("Bob expected to talk to Alice, got %s", p)
		}
		return emA.handleHandshakeRequest(idB, nonce[:], pubKey)
	}

	emA = newEncryptionManager(idA, sendHandshakeA)
	emB = newEncryptionManager(idB, sendHandshakeB)

	return emA, idA, emB, idB
}

func TestEndToEndHandshake(t *testing.T) {
	alice, aliceID, bob, bobID := setupPairedManagers()
	ctx := context.Background()

	msgFromAlice := []byte("Hi bob")
	msgFromBob := []byte("Hi alice")

	// Alice encryption a message for bob. A handshake should be
	// performed.
	cipherA, keyIdA, err := alice.encryptPeerMessage(ctx, bobID, msgFromAlice, keyID{})
	if err != nil {
		t.Fatalf("Alice failed to encrypt: %v", err)
	}

	// Bob should be able to decrypt the message.
	plainA, keyIdB, err := bob.decryptPeerMessage(aliceID, cipherA)
	if err != nil {
		t.Fatalf("Bob failed to decrypt: %v", err)
	}
	if !bytes.Equal(plainA, msgFromAlice) {
		t.Errorf("Bob decrypted wrong content. Got %s", plainA)
	}
	if keyIdA != keyIdB {
		t.Error("Key IDs mismatch between peers")
	}

	// Make sure bob uses the same key for another message.
	cipherB, keyIdB2, err := bob.encryptPeerMessage(ctx, aliceID, msgFromBob, keyID{})
	if err != nil {
		t.Fatalf("Bob failed to encrypt reply: %v", err)
	}
	if keyIdB2 != keyIdB {
		t.Fatalf("Bob generate a new key rather than reusing the existing one")
	}

	// Check that alice can decrypt the message.
	plainB, _, err := alice.decryptPeerMessage(bobID, cipherB)
	if err != nil {
		t.Fatalf("Alice failed to decrypt reply: %v", err)
	}
	if !bytes.Equal(plainB, msgFromBob) {
		t.Errorf("Alice decrypted wrong content. Got %s", plainB)
	}
}

// encryptionManagerWithCounter creates an encryption manager with a
// sendHandshakeRequest function that mocks the network latency and counts
// the number of calls to the function.
func encryptionManagerWithCounter() (encryptionManager, *atomic.Int32) {
	handshakeCalls := &atomic.Int32{}

	mockSendHandshakeRequest := func(ctx context.Context, p peer.ID, id keyID, pubKey []byte) ([]byte, error) {
		handshakeCalls.Add(1)
		time.Sleep(10 * time.Millisecond)
		cpEphemeral, _ := ecdh.X25519().GenerateKey(rand.Reader)
		return cpEphemeral.PublicKey().Bytes(), nil
	}

	em := newEncryptionManager(mockPeerID(), mockSendHandshakeRequest)
	return em, handshakeCalls
}

func TestConcurrentEncryptions(t *testing.T) {
	em, handshakeCalls := encryptionManagerWithCounter()
	cpID := mockPeerID()
	ctx := context.Background()
	plaintext := []byte("hi")

	// Launch 50 concurrent encryption requests for the same peer
	// and make sure only one handshake is performed.
	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, _, err := em.encryptPeerMessage(ctx, cpID, plaintext, keyID{})
			if err != nil {
				t.Errorf("encryptPeerMessage failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if count := handshakeCalls.Load(); count != 1 {
		t.Errorf("Expected 1 handshake, got %d", count)
	}
}

func TestKeyRotation(t *testing.T) {
	alice, _, _, bobID := setupPairedManagers()
	ctx := context.Background()

	// Alice sends message A to bob, triggering a handshake
	cipherA, idA, err := alice.encryptPeerMessage(ctx, bobID, []byte("msgA"), keyID{})
	if err != nil {
		t.Fatal(err)
	}

	// Alice clears key A, forcing a new handshake.
	alice.clearEncryptionKey(bobID, idA)

	// Alice sends message B to bob, triggering a new handshake.
	_, idB, err := alice.encryptPeerMessage(ctx, bobID, []byte("msgB"), keyID{})
	if err != nil {
		t.Fatal(err)
	}
	if idA == idB {
		t.Fatal("Key ID did not change after forced clearing")
	}

	// Alice is still able to decrypt messages using the original key.
	plainA, usedID, err := alice.decryptPeerMessage(bobID, cipherA)
	if err != nil {
		t.Fatalf("Failed to decrypt old ciphertext after rotation: %v", err)
	}
	if usedID != idA {
		t.Error("Did not use the expected old key ID")
	}
	if !bytes.Equal(plainA, []byte("msgA")) {
		t.Errorf("Decrypted wrong content. Got %s", plainA)
	}
}

func TestMaxKeysEviction(t *testing.T) {
	alice, _, _, bobID := setupPairedManagers()
	ctx := context.Background()

	// Generate more keys than the maximum allowed, forcing an eviction of the oldest key.
	var firstCiphertext []byte
	for i := 0; i < maxKeys+1; i++ {
		cipher, id, err := alice.encryptPeerMessage(ctx, bobID, []byte("data"), keyID{})
		if err != nil {
			t.Fatal(err)
		}

		if i == 0 {
			firstCiphertext = cipher
		}

		alice.clearEncryptionKey(bobID, id)

		// After each clearing, the oldest ciphertext should still be decryptable
		// until we exceed maxKeys and it gets evicted on the final rotation.
		_, _, err = alice.decryptPeerMessage(bobID, firstCiphertext)
		if i < maxKeys {
			if err != nil {
				t.Fatalf("Failed to decrypt oldest ciphertext after clearing (iteration %d): %v", i, err)
			}
		} else {
			if err == nil {
				t.Error("Old key was not evicted after max keys limit")
			}
			if err != nil && err != ErrUnknownKeyID {
				t.Errorf("Unexpected error: %v, expected %v", err, ErrUnknownKeyID)
			}
		}
	}
}
