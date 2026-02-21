package tatanka

import (
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/tatanka/admin"
	"github.com/bisoncraft/mesh/tatanka/types"
)

// adminNotifier abstracts the admin server notifications so that callers
// don't need nil-checks when the admin UI is disabled.
type adminNotifier interface {
	BroadcastOracleUpdate(msgType string, snapshot *oracle.OracleSnapshot)
	BroadcastPeerUpdate(pi admin.PeerInfo)
	BroadcastWhitelistState(ws *types.WhitelistState)
	BroadcastWhitelistUpdate(wu admin.WhitelistUpdate)
}

// noopAdminNotifier is used when the admin UI is disabled (AdminPort <= 0).
type noopAdminNotifier struct{}

func (noopAdminNotifier) BroadcastOracleUpdate(string, *oracle.OracleSnapshot) {}
func (noopAdminNotifier) BroadcastPeerUpdate(admin.PeerInfo)                   {}
func (noopAdminNotifier) BroadcastWhitelistState(*types.WhitelistState)        {}
func (noopAdminNotifier) BroadcastWhitelistUpdate(admin.WhitelistUpdate)       {}

// liveAdminNotifier delegates to a real admin.Server.
type liveAdminNotifier struct {
	server *admin.Server
}

func (n *liveAdminNotifier) BroadcastOracleUpdate(msgType string, snapshot *oracle.OracleSnapshot) {
	n.server.BroadcastOracleUpdate(msgType, snapshot)
}

func (n *liveAdminNotifier) BroadcastPeerUpdate(pi admin.PeerInfo) {
	n.server.BroadcastPeerUpdate(pi)
}

func (n *liveAdminNotifier) BroadcastWhitelistState(ws *types.WhitelistState) {
	n.server.BroadcastWhitelistState(ws)
}

func (n *liveAdminNotifier) BroadcastWhitelistUpdate(wu admin.WhitelistUpdate) {
	n.server.BroadcastWhitelistUpdate(wu)
}
