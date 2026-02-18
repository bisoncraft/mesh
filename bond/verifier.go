package bond

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"decred.org/dcrdex/dex"
	"github.com/bisoncraft/mesh/bond/assets/btc"
	"github.com/bisoncraft/mesh/bond/assets/dcr"
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
	Assets     []string
}

type Verifier struct {
	config     *Config
	parsers    map[string]ParseFunc
	txFetcher  *TxFetcher
	fetchPrice func(ticker string) (float64, error)
}

// parserForAsset returns the appropriate parser for the given asset ID.
func parserForAsset(assetID string) (ParseFunc, error) {
	switch assetID {
	case AssetBTC:
		return btc.ParseBondTx, nil
	case AssetDCR:
		return dcr.ParseBondTx, nil
	default:
		return nil, fmt.Errorf("unsupported asset %s", assetID)
	}
}

// NewVerifier creates a new bond verifier with the provided configuration.
func NewVerifier(cfg *Config) (*Verifier, error) {
	parsers := make(map[string]ParseFunc)
	for _, assetID := range cfg.Assets {
		parser, err := parserForAsset(assetID)
		if err != nil {
			return nil, err
		}
		parsers[assetID] = parser
	}

	return &Verifier{
		config:     cfg,
		parsers:    parsers,
		txFetcher:  cfg.TxFetcher,
		fetchPrice: cfg.FetchPrice,
	}, nil
}

// AssetIDToTicker converts an asset ID to its ticker string.
func AssetIDToTicker(assetID string) (string, error) {
	switch assetID {
	case AssetBTC:
		return "BTC", nil
	case AssetDCR:
		return "DCR", nil
	default:
		return "", fmt.Errorf("unknown asset %s", assetID)
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
func (v *Verifier) VerifyBond(assetID string, bondID []byte, peerID peer.ID) (bool, float64, time.Time, string, error) {
	parser, ok := v.parsers[assetID]
	if !ok {
		return false, 0, time.Time{}, "", fmt.Errorf("no parser registered for asset %s", assetID)
	}

	txHash, _, err := DecodeCoinID(bondID)
	if err != nil {
		return false, 0, time.Time{}, "", fmt.Errorf("failed to decode coin id: %w", err)
	}

	rawTx, err := v.txFetcher.FetchTx(assetID, txHash)
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
func (v *Verifier) VerifyBondStub(assetID string, bondID []byte, peerID peer.ID) (bool, float64, time.Time, string, error) {
	return true, 1.0, time.Now().Add(time.Hour * 12), "account-id", nil
}
