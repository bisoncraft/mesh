package bond

import (
	"fmt"
	"strconv"
	"strings"
)

// BondID represents a bond identifier.
type BondID struct {
	Asset uint32
	TxID  string
	Vout  uint32
}

// ParseBondID parses a bond ID string in format "assetid:txid:vout" into a BondID struct.
func ParseBondID(id string) (*BondID, error) {
	parts := strings.Split(id, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid bond ID format, expected 'assetid:txid:vout', got '%s'", id)
	}

	assetID, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid asset ID '%s': %w", parts[0], err)
	}

	vout, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid vout '%s': %w", parts[2], err)
	}

	return &BondID{
		Asset: uint32(assetID),
		TxID:  parts[1],
		Vout:  uint32(vout),
	}, nil
}

// String formats a BondID back to "assetid:txid:vout" string format.
func (b *BondID) String() string {
	return fmt.Sprintf("%d:%s:%d", b.Asset, b.TxID, b.Vout)
}
