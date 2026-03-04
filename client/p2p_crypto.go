package client

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/crypto/hkdf"
)

const (
	gcmNonceSize = 12
	keyIDSize    = 16
	maxKeys      = 5 // maximum number of keys to store for a peer
	// keyTTL is the time after the key is removed from the cache.
	keyTTL = time.Hour
	// expireMainKey is the time before the main key is removed from the cache
	// where it should not be used as main key. This is used to prevent not
	// being able to decrypt messages during key rotation.
	expireMainKey = 10 * time.Minute
	keyGCInterval = 5 * time.Minute
)

// keyID is a unique identifier for an encryption key.
type keyID [keyIDSize]byte

var (
	ErrNoEncryptionKey = errors.New("no encryption key")
	ErrUnknownKeyID    = errors.New("unknown key ID")
)

type expiringKey struct {
	key        []byte
	expiration time.Time
}

type peerKeys struct {
	mtx  sync.Mutex
	keys map[keyID]*expiringKey
	// currentKeyID is the default key to use when encrypting messages.
	currentKeyID keyID // zero value if none

	inProgressHandshake sync.Mutex
}

// getCurrentKey returns the default key to use for encrypting messages. If
// there is no default key, false will be returned, indicating we need to do
// a handshake with the peer to get a new key.
func (p *peerKeys) getCurrentKey() ([]byte, keyID, bool) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.currentKeyID != (keyID{}) {
		if key, ok := p.keys[p.currentKeyID]; ok {
			if time.Now().Before(key.expiration.Add(-expireMainKey)) {
				return key.key, p.currentKeyID, true
			}
		}
	}

	return nil, keyID{}, false
}

// setKey adds a new key to the keys to be used with the peer and sets it as
// the current key. If the number of keys exceeds the maximum, the oldest key
// will be removed.
func (p *peerKeys) setKey(id keyID, key []byte) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.keys[id] = &expiringKey{
		key:        key,
		expiration: time.Now().Add(keyTTL),
	}

	p.currentKeyID = id

	if len(p.keys) > maxKeys {
		var (
			oldestID         keyID
			oldestExpiration time.Time
			haveOldest       bool
		)
		for id, key := range p.keys {
			if !haveOldest || key.expiration.Before(oldestExpiration) {
				oldestExpiration = key.expiration
				oldestID = id
				haveOldest = true
			}
		}
		if haveOldest {
			delete(p.keys, oldestID)
		}
	}
}

// getKeyByID returns the key for the given ID.
func (p *peerKeys) getKeyByID(id keyID) ([]byte, bool) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	wrapper, ok := p.keys[id]
	if !ok {
		return nil, false
	}

	return wrapper.key, true
}

func (p *peerKeys) clearCurrentKey(id keyID) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.currentKeyID == id {
		p.currentKeyID = keyID{}
	}
}

// gcKeys removes the expired keys from the keys map and clears the
// current key if it will expire soon.
func (p *peerKeys) gcKeys() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	now := time.Now()
	for keyID, keyWrapper := range p.keys {
		if now.After(keyWrapper.expiration) {
			delete(p.keys, keyID)
		}
	}

	if p.currentKeyID != (keyID{}) {
		wrapper, ok := p.keys[p.currentKeyID]
		if !ok {
			p.currentKeyID = keyID{}
			return
		}
		if now.After(wrapper.expiration.Add(-expireMainKey)) {
			p.currentKeyID = keyID{}
		}
	}
}

// encryptionManager manages the encryption keys used to conduct encrypted
// p2p communication with other mesh clients. In order to avoid handshake
// race conditions, multiple keys are stored for each peer, but only the
// latest key is used for encrypted messages initiated by this client.
// Encryption keys are ephemeral and expire after a certain time. Before
// the latest key expires, a new handshake is performed to get a new key.
type encryptionManager interface {
	encryptPeerMessage(context.Context, peer.ID, []byte, keyID) ([]byte, keyID, error)
	decryptPeerMessage(peer.ID, []byte) ([]byte, keyID, error)
	clearEncryptionKey(peer.ID, keyID)
	handleHandshakeRequest(peer.ID, []byte, []byte) ([]byte, error)
	run(context.Context)
}

type encryptionManagerImpl struct {
	mtx sync.Mutex
	// encryptionKeys should only be accessed through the getPeerKeys method.
	encryptionKeys map[peer.ID]*peerKeys

	ourPeerID            peer.ID
	sendHandshakeRequest func(context.Context, peer.ID, keyID, []byte) ([]byte, error)
}

func newEncryptionManager(ourPeerID peer.ID, sendHandshakeRequest func(context.Context, peer.ID, keyID, []byte) ([]byte, error)) *encryptionManagerImpl {
	return &encryptionManagerImpl{
		ourPeerID:            ourPeerID,
		sendHandshakeRequest: sendHandshakeRequest,
		encryptionKeys:       make(map[peer.ID]*peerKeys),
	}
}

// getPeerKeys returns the keys for the given peer. If the peer has no keys,
// a new peerKeys struct is created and returned.
func (em *encryptionManagerImpl) getPeerKeys(peerID peer.ID) *peerKeys {
	em.mtx.Lock()
	defer em.mtx.Unlock()

	keys, ok := em.encryptionKeys[peerID]
	if !ok {
		em.encryptionKeys[peerID] = &peerKeys{
			keys: make(map[keyID]*expiringKey),
		}
		keys = em.encryptionKeys[peerID]
	}

	return keys
}

// handleHandshakeRequest processes an incoming handshake request message. It
// returns the public key for the local client to be used in the handshake,
// and stores the shared key. This function assumes that the counterparty's
// signature of the handshake request was already verified.
func (em *encryptionManagerImpl) handleHandshakeRequest(cpPeerID peer.ID, nonceB, cpPubKeyB []byte) (ourPubKey []byte, err error) {
	if len(nonceB) != keyIDSize || len(cpPubKeyB) != 32 {
		return nil, fmt.Errorf("invalid request length")
	}

	var id keyID
	copy(id[:], nonceB)

	ourPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate response key: %w", err)
	}

	sharedKey, err := getSharedKey(ourPriv, cpPubKeyB, em.ourPeerID, cpPeerID, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get shared key: %w", err)
	}

	keyWrapper := em.getPeerKeys(cpPeerID)
	keyWrapper.setKey(id, sharedKey)

	return ourPriv.PublicKey().Bytes(), nil
}

func getSharedKey(privKey *ecdh.PrivateKey, cpPubKeyB []byte, ourPeerID, cpPeerID peer.ID, id keyID) ([]byte, error) {
	cpPub, err := ecdh.X25519().NewPublicKey(cpPubKeyB)
	if err != nil {
		return nil, fmt.Errorf("invalid counterparty handshake public key: %w", err)
	}
	sharedSecret, err := privKey.ECDH(cpPub)
	if err != nil {
		return nil, fmt.Errorf("failed to derive ECDH shared secret: %w", err)
	}

	hkdf := hkdf.New(sha256.New, sharedSecret, id[:], peerInfo(ourPeerID, cpPeerID))
	sharedKey := make([]byte, 32)
	if _, err := io.ReadFull(hkdf, sharedKey); err != nil {
		return nil, fmt.Errorf("failed to derive key: %w", err)
	}

	return sharedKey, nil
}

func peerInfo(a, b peer.ID) []byte {
	if a < b {
		return []byte("tatanka-p2p-v1|" + string(a) + "|" + string(b))
	}
	return []byte("tatanka-p2p-v1|" + string(b) + "|" + string(a))
}

func (em *encryptionManagerImpl) performKeyHandshake(ctx context.Context, cpPeerID peer.ID) (shared []byte, id keyID, err error) {
	if _, err := rand.Read(id[:]); err != nil {
		return nil, keyID{}, fmt.Errorf("failed to generate key ID: %w", err)
	}

	encryptionPrivKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to generate ephemeral key: %w", err)
	}

	encryptionPubKey := encryptionPrivKey.PublicKey().Bytes()

	cpPubB, err := em.sendHandshakeRequest(ctx, cpPeerID, id, encryptionPubKey)
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to send handshake request: %w", err)
	}

	sharedKey, err := getSharedKey(encryptionPrivKey, cpPubB, em.ourPeerID, cpPeerID, id)
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to get shared key: %w", err)
	}

	return sharedKey, id, nil
}

func (em *encryptionManagerImpl) getPeerEncryptionKey(ctx context.Context, cpPeerID peer.ID, expectedID keyID) ([]byte, keyID, error) {
	keyWrapper := em.getPeerKeys(cpPeerID)

	// If expected ID is provided, use it.
	if expectedID != (keyID{}) {
		k, found := keyWrapper.getKeyByID(expectedID)
		if !found {
			return nil, keyID{}, ErrUnknownKeyID
		}
		return k, expectedID, nil
	}

	// If no expected ID is provided, use the current key if it is available.
	k, id, found := keyWrapper.getCurrentKey()
	if found {
		return k, id, nil
	}

	// If no current key is available, perform a handshake to get a new key.
	keyWrapper.inProgressHandshake.Lock()
	defer keyWrapper.inProgressHandshake.Unlock()

	// Check if another goroutine already performed a handshake.
	k, id, found = keyWrapper.getCurrentKey()
	if found {
		return k, id, nil
	}

	// Perform a handshake to get a new key.
	shared, id, err := em.performKeyHandshake(ctx, cpPeerID)
	if err != nil {
		return nil, keyID{}, err
	}

	// Store the new key.
	keyWrapper.setKey(id, shared)

	return shared, id, nil
}

// encryptPeerMessage encrypts a message for the given peer and returns the
// ciphertext and the ID of the key used to encrypt the message. The requiredID
// is the ID of the key that must be used to encrypt the message. If requiredID
// is empty, the current key is used if it is available, otherwise a new handshake
// is performed to get a new key. requiredID is used to always use the same key to
// encrypt a response as was used by the peer to encrypt the request.
func (em *encryptionManagerImpl) encryptPeerMessage(ctx context.Context, cpPeerID peer.ID, plaintext []byte, requiredID keyID) (ciphertext []byte, usedID keyID, err error) {
	key, usedID, err := em.getPeerEncryptionKey(ctx, cpPeerID, requiredID)
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to get peer encryption key: %w", err)
	}

	aad := messageAAD(em.ourPeerID, cpPeerID, usedID)
	cipher, err := encryptAES(key, plaintext, aad)
	if err != nil {
		return nil, keyID{}, err
	}

	prepended := append(usedID[:], cipher...)

	return prepended, usedID, nil
}

// decryptPeerMessage decrypts a message for the given peer and returns the
// plaintext and the ID of the key used to decrypt the message.
func (em *encryptionManagerImpl) decryptPeerMessage(cpPeerID peer.ID, ciphertext []byte) ([]byte, keyID, error) {
	if len(ciphertext) < keyIDSize {
		return nil, keyID{}, errors.New("ciphertext too short for key ID")
	}

	var id keyID
	copy(id[:], ciphertext[:keyIDSize])
	cipher := ciphertext[keyIDSize:]

	keyWrapper := em.getPeerKeys(cpPeerID)
	key, found := keyWrapper.getKeyByID(id)
	if !found {
		return nil, keyID{}, ErrUnknownKeyID
	}

	aad := messageAAD(cpPeerID, em.ourPeerID, id)
	plaintext, err := decryptAES(key, cipher, aad)
	if err != nil {
		return nil, keyID{}, err
	}

	return plaintext, id, nil
}

// messageAAD builds the additional authenticated data for AES-GCM. The peer
// IDs are sorted canonically so both sender and receiver produce the same AAD.
func messageAAD(a, b peer.ID, id keyID) []byte {
	info := peerInfo(a, b)
	aad := make([]byte, 0, len(info)+keyIDSize)
	aad = append(aad, info...)
	aad = append(aad, id[:]...)
	return aad
}

// clearCurrentKey clears the current key, forcing a new handshake with the peer
// the next time we need to encrypt a message for the peer. This should be called
// when the peer notifies us that they no longer have the encryption key we used
// to send them a message. The id of the keys is passed in, to avoid a race condition
// where the current key was already updated.
func (em *encryptionManagerImpl) clearEncryptionKey(cpPeerID peer.ID, id keyID) {
	keyWrapper := em.getPeerKeys(cpPeerID)
	keyWrapper.clearCurrentKey(id)
}

// gcKeys removes the expired keys from the keys map and clears the
// current key if it will expire soon.
func (em *encryptionManagerImpl) gcKeys() {
	peerKeysCopy := make(map[peer.ID]*peerKeys)
	em.mtx.Lock()
	maps.Copy(peerKeysCopy, em.encryptionKeys)
	em.mtx.Unlock()

	for _, keyWrapper := range peerKeysCopy {
		keyWrapper.gcKeys()
	}
}

// run starts the encryption manager's garbage collection loop.
func (em *encryptionManagerImpl) run(ctx context.Context) {
	gcTicker := time.NewTicker(keyGCInterval)
	defer gcTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gcTicker.C:
			em.gcKeys()
		}
	}
}

func encryptAES(key, plaintext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %v", err)
	}

	nonce := make([]byte, gcmNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %v", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, aad)

	return ciphertext, nil
}

func decryptAES(key, ciphertext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %v", err)
	}

	if len(ciphertext) < gcmNonceSize {
		return nil, errors.New("ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:gcmNonceSize], ciphertext[gcmNonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %v", err)
	}

	return plaintext, nil
}
