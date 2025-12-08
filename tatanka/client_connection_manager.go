package tatanka

import (
	"sync"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

// clientConnectionInfo contains information about a client's connection to
// a tatanka node.
type clientConnectionInfo struct {
	connected bool
	timestamp int64
}

// clientConnectionManager stores information about which tatanka nodes
// clients are connected to and the addresses they can be reached at.
type clientConnectionManager struct {
	log slog.Logger

	mtx                  sync.RWMutex
	clientConnectionInfo map[peer.ID]map[peer.ID]*clientConnectionInfo
}

func newClientConnectionManager(log slog.Logger) *clientConnectionManager {
	return &clientConnectionManager{
		log:                  log,
		clientConnectionInfo: make(map[peer.ID]map[peer.ID]*clientConnectionInfo),
	}
}

// updateClientConnectionInfo updates the client connection info for a given client
// to a tatanka node. Only the most recent update is stored.
func (c *clientConnectionManager) updateClientConnectionInfo(update *clientConnectionUpdate) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.clientConnectionInfo[update.clientID]; !ok {
		c.clientConnectionInfo[update.clientID] = make(map[peer.ID]*clientConnectionInfo)
	}

	curr := c.clientConnectionInfo[update.clientID][update.reporterID]
	if curr != nil && curr.timestamp > update.timestamp {
		c.log.Debugf(
			"Skipping client connection update for %s from reporter %s: "+
				"timestamp %d is older than current timestamp %d",
			update.clientID.ShortString(), update.reporterID.ShortString(), update.timestamp, curr.timestamp,
		)
		return
	}

	c.clientConnectionInfo[update.clientID][update.reporterID] = &clientConnectionInfo{
		connected: update.connected,
		timestamp: update.timestamp,
	}
}

// getTatankaPeersForClient returns the tatanka peer IDs that have reported
// that the client is connected to them.
func (c *clientConnectionManager) getTatankaPeersForClient(client peer.ID) []peer.ID {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	reporters := c.clientConnectionInfo[client]
	if len(reporters) == 0 {
		return nil
	}

	peers := make([]peer.ID, 0, len(reporters))
	for reporterID, info := range reporters {
		if info != nil && info.connected {
			peers = append(peers, reporterID)
		}
	}

	return peers
}
