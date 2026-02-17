package btc

import (
	"bytes"
	"fmt"
	"time"

	"decred.org/dcrdex/server/asset/btc"
	"github.com/btcsuite/btcd/wire"
)

// ParseBondTx parses a BTC bond transaction and returns asset value, expiry, and account ID bytes.
func ParseBondTx(ver uint16, rawTx []byte) (float64, time.Time, []byte, error) {
	msgTx := &wire.MsgTx{}
	err := msgTx.Deserialize(bytes.NewReader(rawTx))
	if err != nil {
		return 0, time.Time{}, nil, fmt.Errorf("failed to deserialize transaction: %w", err)
	}

	amt, _, _, t, accountID, err := btc.ParseBondTx(ver, msgTx, nil, true)
	if err != nil {
		return 0, time.Time{}, nil, fmt.Errorf("failed to parse bond: %w", err)
	}

	idBytes := accountID[:]
	amount := float64(amt) / 1e8
	lockTime := time.Unix(t, 0)

	return amount, lockTime, idBytes, nil
}
