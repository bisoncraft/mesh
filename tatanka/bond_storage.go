package tatanka

import (
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// bondParams contains the parameters of a bond.
type bondParams struct {
	id       string
	expiry   time.Time
	strength uint32
}

// clientBondInfo stores the bond information for a single client.
type clientBondInfo struct {
	mtx           sync.Mutex
	totalStrength uint32
	// storedBonds are the bond IDs that have been stored for this client
	storedBonds map[string]struct{}
	// bondParams are sorted in order of increasing expiry time to enable
	// efficient removal of expired bonds.
	bondsParams []*bondParams
}

func newClientBondInfo() *clientBondInfo {
	return &clientBondInfo{
		storedBonds: make(map[string]struct{}),
		bondsParams: make([]*bondParams, 0),
	}
}

// addBond adds a bond to the client bond info, maintaining sorted order.
//
// mtx MUST be write locked when calling this method.
func (cbi *clientBondInfo) addBonds(bonds []*bondParams, now time.Time) {
	// Make sure none of bonds are already stored, and that none of the new
	// bonds are duplicates.
	newBonds := make([]*bondParams, 0, len(bonds))
	newBondIDs := make(map[string]struct{}, len(bonds))
	for _, bond := range bonds {
		if _, ok := cbi.storedBonds[bond.id]; ok {
			continue
		}
		if _, ok := newBondIDs[bond.id]; ok {
			continue
		}
		newBondIDs[bond.id] = struct{}{}
		newBonds = append(newBonds, bond)
	}
	if len(newBonds) == 0 {
		return
	}

	// Only add unexpired bonds.
	bondsToAdd := make([]*bondParams, 0, len(newBonds))
	for _, bond := range newBonds {
		if !bond.expiry.After(now) {
			continue
		}
		cbi.storedBonds[bond.id] = struct{}{}
		cbi.totalStrength += bond.strength
		bondsToAdd = append(bondsToAdd, bond)
	}

	// Sort the bonds by expiry time
	cbi.bondsParams = append(cbi.bondsParams, bondsToAdd...)
	sort.Slice(cbi.bondsParams, func(i, j int) bool {
		return cbi.bondsParams[i].expiry.Before(cbi.bondsParams[j].expiry)
	})
}

// clearExpiredBonds clears expired bonds from the client bond info.
//
// mtx MUST be write locked when calling this method.
func (cbi *clientBondInfo) clearExpiredBonds(now time.Time) {
	firstUnexpiredIndex := len(cbi.bondsParams)
	for i, bondParams := range cbi.bondsParams {
		if bondParams.expiry.After(now) {
			firstUnexpiredIndex = i
			break
		}
		cbi.totalStrength -= bondParams.strength
		delete(cbi.storedBonds, bondParams.id)
	}

	cbi.bondsParams = cbi.bondsParams[firstUnexpiredIndex:]
}

type bondStorage interface {
	addBonds(peerID peer.ID, bonds []*bondParams) uint32
	getBondStrength(peerID peer.ID) uint32
}

// memoryBondStorage stores the bond information for all clients in memory.
// TODO: we should only store currently connected clients in this map. Other
// previously verified bonds should be stored in a database to avoid having to
// re-verify them.
type memoryBondStorage struct {
	mtx     sync.RWMutex
	clients map[peer.ID]*clientBondInfo
	timeNow func() time.Time
}

// newMemoryBondStorage creates a new memory bond storage with the given function
// to get the current time so that it can be mocked in tests.
func newMemoryBondStorage(timeNow func() time.Time) *memoryBondStorage {
	return &memoryBondStorage{
		clients: make(map[peer.ID]*clientBondInfo),
		timeNow: timeNow,
	}
}

// addBonds adds bonds to the bond storage for a given client.
func (bs *memoryBondStorage) addBonds(peerID peer.ID, bonds []*bondParams) uint32 {
	bs.mtx.Lock()
	clientInfo, ok := bs.clients[peerID]
	if !ok {
		clientInfo = newClientBondInfo()
		bs.clients[peerID] = clientInfo
	}
	bs.mtx.Unlock()

	clientInfo.mtx.Lock()
	defer clientInfo.mtx.Unlock()

	now := bs.timeNow()
	clientInfo.clearExpiredBonds(now)
	clientInfo.addBonds(bonds, now)

	return clientInfo.totalStrength
}

// getBondStrength returns the total strength of all bonds for a given client.
func (bs *memoryBondStorage) getBondStrength(peerID peer.ID) uint32 {
	bs.mtx.RLock()
	clientInfo, ok := bs.clients[peerID]
	if !ok {
		bs.mtx.RUnlock()
		return 0
	}
	bs.mtx.RUnlock()

	clientInfo.mtx.Lock()
	defer clientInfo.mtx.Unlock()

	clientInfo.clearExpiredBonds(bs.timeNow())
	return clientInfo.totalStrength
}
