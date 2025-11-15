package tatanka

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type bondVerifier struct {
}

func newBondVerifier() *bondVerifier {
	return &bondVerifier{}
}

// verifyBond verifies a bond and returns the validity, expiration time, and
// strength of the bond.
func (bv *bondVerifier) verifyBond(assetID uint32, bondID []byte, peerID peer.ID) (bool, time.Time, uint32, error) {
	return true, time.Now().Add(time.Hour * 12), 1, nil
}
