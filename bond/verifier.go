package bond

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"decred.org/dcrdex/dex"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/decred/dcrd/crypto/blake256"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ParseFunc is a function that parses a bond transaction and returns asset value, expiry and account id bytes.
type ParseFunc func(ver uint16, rawTx []byte) (amount float64, lockTime time.Time, idBytes []byte, err error)

// AccountIDFromPublicKey generates an account ID from a public key using BLAKE256 hash.
func AccountIDFromPublicKey(pubKey crypto.PubKey) ([]byte, error) {
	pubKeyBytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	hash := blake256.Sum256(pubKeyBytes)
	return hash[:], nil
}

// Config holds the configuration for Verifier.
type Config struct {
	TxFetcher  *TxFetcher
	FetchPrice func(ticker string) (float64, error)
}

type Verifier struct {
	config     *Config
	parsers    map[uint32]ParseFunc
	parsersMtx sync.RWMutex
	txFetcher  *TxFetcher
	fetchPrice func(ticker string) (float64, error)
}

// NewVerifier creates a new bond verifier with the provided configuration.
func NewVerifier(cfg *Config) *Verifier {
	return &Verifier{
		config:     cfg,
		parsers:    make(map[uint32]ParseFunc),
		txFetcher:  cfg.TxFetcher,
		fetchPrice: cfg.FetchPrice,
	}
}

// RegisterParser registers a bond parser function for an asset.
func (v *Verifier) RegisterParser(asset uint32, parser ParseFunc) {
	v.parsersMtx.Lock()
	v.parsers[asset] = parser
	v.parsersMtx.Unlock()
}

// AssetIDToTicker converts an asset ID to its ticker string.
func AssetIDToTicker(assetID uint32) (string, error) {
	switch assetID {
	case AssetBTC:
		return "BTC", nil
	case AssetDCR:
		return "DCR", nil
	default:
		return "", fmt.Errorf("unknown asset %d", assetID)
	}
}

// DecodeCoinID decodes a coin ID into its transaction hash and output index.
func DecodeCoinID(coinID dex.Bytes) (string, uint32, error) {
	if len(coinID) != 36 {
		return "", 0, fmt.Errorf("coin ID wrong length. expected 36, got %d", len(coinID))
	}
	var txHash chainhash.Hash
	copy(txHash[:], coinID[:32])
	vout := binary.BigEndian.Uint32(coinID[32:])
	return txHash.String(), vout, nil
}

// VerifyBond verifies a single bond transaction and returns asset value, expiry, and account ID.
func (v *Verifier) VerifyBond(assetID uint32, bondID []byte, peerID peer.ID) (bool, float64, time.Time, string, error) {
	v.parsersMtx.RLock()
	parser, ok := v.parsers[assetID]
	v.parsersMtx.RUnlock()

	if !ok {
		return false, 0, time.Time{}, "", fmt.Errorf("no parser registered for asset %d", assetID)
	}

	txHash, _, err := DecodeCoinID(bondID)
	if err != nil {
		return false, 0, time.Time{}, "", fmt.Errorf("failed to decode coin id: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	rawTx, err := v.txFetcher.FetchTx(ctx, assetID, txHash)
	if err != nil {
		return false, 0, time.Time{}, "", fmt.Errorf("failed to fetch transaction: %w", err)
	}

	pubKey, err := peerID.ExtractPublicKey()
	if err != nil {
		return false, 0, time.Time{}, "", fmt.Errorf("failed to extract public key: %w", err)
	}

	expectedAcctID, err := AccountIDFromPublicKey(pubKey)
	if err != nil {
		return false, 0, time.Time{}, "", fmt.Errorf("failed to generate expected bond account id: %w", err)
	}

	assetValue, expiry, acctID, err := parser(0, rawTx)
	if err != nil {
		return false, 0, time.Time{}, "", fmt.Errorf("failed to parse bond: %w", err)
	}

	if !bytes.Equal(acctID, expectedAcctID) {
		return false, 0, time.Time{}, "", fmt.Errorf("account id mismatch: expected %x, got %x", expectedAcctID, acctID)
	}

	return true, assetValue, expiry, string(acctID), nil
}

// VerifyBondStub is a stub implementation that returns a default valid bond with raw values.
// TODO: use VerifyBond instead once clients are ready to perform actual verification.
func (v *Verifier) VerifyBondStub(assetID uint32, bondID []byte, peerID peer.ID) (bool, float64, time.Time, string, error) {
	return true, 1.0, time.Now().Add(time.Hour * 12), "account-id", nil
}
