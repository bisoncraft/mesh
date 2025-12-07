package bond

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
)

const (
	// MinRequiredBondStrength is the minimum required bond strength for a client.
	MinRequiredBondStrength = 1
)

// BondParams contains the parameters of a bond.
type BondParams struct {
	ID       string
	Expiry   time.Time
	Strength uint32
}

// BondInfo stores the bond information for a client.
type BondInfo struct {
	mtx           sync.RWMutex
	totalStrength atomic.Uint32
	// storedBonds are the bond IDs that have been stored for this client
	storedBonds map[string]struct{}
	// bondParams are sorted in order of increasing expiry time to enable
	// efficient removal of expired bonds.
	bondParams []*BondParams
}

// NewBondInfo initializes bond information for a client.
func NewBondInfo() *BondInfo {
	return &BondInfo{
		storedBonds: make(map[string]struct{}),
		bondParams:  make([]*BondParams, 0),
	}
}

// AddBond adds a bond to the client bond info, maintaining sorted order.
func (bi *BondInfo) AddBonds(bonds []*BondParams, now time.Time) {
	bi.mtx.Lock()
	defer bi.mtx.Unlock()

	// Make sure none of bonds are already stored, and that none of the new
	// bonds are duplicates.
	newBonds := make([]*BondParams, 0, len(bonds))
	newBondIDs := make(map[string]struct{}, len(bonds))
	for _, bond := range bonds {
		if _, ok := bi.storedBonds[bond.ID]; ok {
			continue
		}
		if _, ok := newBondIDs[bond.ID]; ok {
			continue
		}
		newBondIDs[bond.ID] = struct{}{}
		newBonds = append(newBonds, bond)
	}
	if len(newBonds) == 0 {
		return
	}

	// Only add unexpired bonds.
	bondsToAdd := make([]*BondParams, 0, len(newBonds))
	for _, bond := range newBonds {
		if !bond.Expiry.After(now) {
			continue
		}
		bi.storedBonds[bond.ID] = struct{}{}
		_ = bi.totalStrength.Add(bond.Strength)
		bondsToAdd = append(bondsToAdd, bond)
	}

	// Sort the bonds by expiry time
	bi.bondParams = append(bi.bondParams, bondsToAdd...)
	sort.Slice(bi.bondParams, func(i, j int) bool {
		return bi.bondParams[i].Expiry.Before(bi.bondParams[j].Expiry)
	})
}

// BondStrength returns the total bond strength of the the provided client bond info.
func (bi *BondInfo) BondStrength() uint32 {
	return bi.totalStrength.Load()
}

// ClearExpiredBonds clears expired bonds from the client bond info.
func (bi *BondInfo) ClearExpiredBonds(now time.Time) {
	bi.mtx.Lock()
	defer bi.mtx.Unlock()

	firstUnexpiredIndex := len(bi.bondParams)
	for i, bondParams := range bi.bondParams {
		if bondParams.Expiry.After(now) {
			firstUnexpiredIndex = i
			break
		}
		bi.totalStrength.Add(-bondParams.Strength)
		delete(bi.storedBonds, bondParams.ID)
	}

	bi.bondParams = bi.bondParams[firstUnexpiredIndex:]
}

// RemoveBondAtIndex removes the bond at the provided index.
func (cbi *BondInfo) RemoveBondAtIndex(index uint32) error {
	cbi.mtx.RLock()
	bondSize := len(cbi.bondParams)
	cbi.mtx.RUnlock()

	if bondSize == 0 {
		// No bond to remove
		return nil
	}

	if index > uint32(bondSize)-1 {
		return fmt.Errorf("index %d exceeds max bonds index range %d", index, bondSize-1)
	}

	cbi.mtx.Lock()
	cbi.bondParams = append(cbi.bondParams[:index], cbi.bondParams[index+1:]...)
	cbi.mtx.Unlock()

	return nil
}

// PostBondReqFromBondInfo converts the provided bond info into a post bond request.
func PostBondReqFromBondInfo(info *BondInfo) (*protocolsPb.PostBondRequest, error) {
	if len(info.bondParams) == 0 {
		return nil, fmt.Errorf("no bonds provided")
	}

	req := &protocolsPb.PostBondRequest{}
	for _, bp := range info.bondParams {
		req.Bonds = append(req.Bonds, &protocolsPb.Bond{BondID: []byte(bp.ID)})
	}

	return req, nil
}
