package bond

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BondParams contains the parameters of a bond.
type BondParams struct {
	ID       string
	Expiry   time.Time
	Strength uint32
}

// BondInfo stores the bond information for a client.
type BondInfo struct {
	mtx           sync.Mutex
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
