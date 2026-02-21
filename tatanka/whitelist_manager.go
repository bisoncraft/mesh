package tatanka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

// timestampedWhitelistState wraps a WhitelistState with a timestamp used for
// staleness checks during gossip.
type timestampedWhitelistState struct {
	types.WhitelistState
	timestamp int64
}

// whitelistManagerConfig holds configuration for creating a whitelistManager.
type whitelistManagerConfig struct {
	log                 slog.Logger
	peerID              peer.ID
	whitelist           *types.Whitelist
	isConnected         func(peer.ID) bool
	whitelistUpdated    func(newWl *types.Whitelist)
	broadcastLocalState func(ws *types.WhitelistState)
}

// whitelistManager manages whitelist updates and state transitions.
// It accepts updates to the whitelist states of peers and the local node,
// and checks for consensus on a transition using a two-phase flow:
//  1. Ready phase: for overlap peers (present in both current and proposed
//     whitelists), require a 2/3 quorum over the full overlap set and no
//     online overlap peer disagreement.
//  2. Commit phase: once ready, require every online overlap peer to be ready
//     (or already switched) before switching Current to Proposed.
//
// Because quorum is computed from the full overlap set, offline overlap peers
// can block transition progress until they reconnect or are removed.
type whitelistManager struct {
	log         slog.Logger
	peerID      peer.ID
	isConnected func(peer.ID) bool

	mtx        sync.RWMutex
	localState types.WhitelistState
	peerStates map[peer.ID]*timestampedWhitelistState

	whitelistUpdated    func(newWl *types.Whitelist)
	broadcastLocalState func(ws *types.WhitelistState)
}

func newWhitelistManager(cfg *whitelistManagerConfig) *whitelistManager {
	return &whitelistManager{
		log:         cfg.log,
		peerID:      cfg.peerID,
		isConnected: cfg.isConnected,
		localState: types.WhitelistState{
			Current: cfg.whitelist,
		},
		peerStates:          make(map[peer.ID]*timestampedWhitelistState),
		whitelistUpdated:    cfg.whitelistUpdated,
		broadcastLocalState: cfg.broadcastLocalState,
	}
}

// heartbeat publishes the local node's current state if a proposal is active.
func (wm *whitelistManager) heartbeat() {
	wm.mtx.RLock()
	if wm.localState.Proposed == nil {
		wm.mtx.RUnlock()
		return
	}
	stateCopy := wm.localState.DeepCopy()
	wm.mtx.RUnlock()

	wm.broadcastLocalState(stateCopy)
}

// run periodically heartbeats the local whitelist state while a proposal is
// active.
func (wm *whitelistManager) run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wm.heartbeat()
		}
	}
}

func (wm *whitelistManager) getWhitelist() *types.Whitelist {
	wm.mtx.RLock()
	defer wm.mtx.RUnlock()
	return wm.localState.Current.DeepCopy()
}

// getLocalWhitelistState returns a snapshot of the local node's whitelist state.
func (wm *whitelistManager) getLocalWhitelistState() *types.WhitelistState {
	wm.mtx.RLock()
	defer wm.mtx.RUnlock()
	return wm.localState.DeepCopy()
}

// getPeerWhitelistState returns a copy of the whitelist state for a peer.
func (wm *whitelistManager) getPeerWhitelistState(peerID peer.ID) *types.WhitelistState {
	wm.mtx.RLock()
	defer wm.mtx.RUnlock()

	if peerID == wm.peerID {
		return wm.localState.DeepCopy()
	}

	state := wm.peerStates[peerID]
	if state == nil {
		return nil
	}

	return state.WhitelistState.DeepCopy()
}

// updatePeerWhitelistState records a peer's whitelist state and triggers a
// transition check. Passing a nil state clears the peer's stored state,
// regardless of the timestamp.
// Returns true if the stored state for that peer changed.
func (wm *whitelistManager) updatePeerWhitelistState(peerID peer.ID, ws *types.WhitelistState, timestamp int64) bool {
	if peerID == wm.peerID {
		return false
	}
	if ws != nil && ws.Current == nil {
		return false
	}

	doUpdate := func() (updated, localStateChanged, whitelistSwitched bool, localStateCopy *types.WhitelistState) {
		wm.mtx.Lock()
		defer wm.mtx.Unlock()

		existing, exists := wm.peerStates[peerID]

		// If no update, return early
		if (ws == nil && !exists) || (ws != nil && existing != nil && existing.timestamp >= timestamp) {
			return false, false, false, nil
		}

		updated = true

		// Update the peer's state
		if ws == nil {
			delete(wm.peerStates, peerID)
		} else {
			wsCopy := ws.DeepCopy()
			wm.peerStates[peerID] = &timestampedWhitelistState{
				WhitelistState: *wsCopy,
				timestamp:      timestamp,
			}
		}

		// Check if this update causes a transition.
		localStateChanged, whitelistSwitched = wm.checkTransitionLocked()
		if localStateChanged {
			localStateCopy = wm.localState.DeepCopy()
		}

		return updated, localStateChanged, whitelistSwitched, localStateCopy
	}

	updated, localStateChanged, whitelistSwitched, localStateCopy := doUpdate()
	if !updated {
		return false
	}
	if localStateChanged {
		wm.broadcastLocalState(localStateCopy)
	}
	if whitelistSwitched {
		wm.whitelistUpdated(localStateCopy.Current)
	}

	return true
}

// proposeWhitelist sets the proposed whitelist. Connections are not changed
// until the transition completes. The proposed whitelist must include the
// local node.
func (wm *whitelistManager) proposeWhitelist(wl *types.Whitelist) error {
	if wl == nil || wl.PeerIDs == nil {
		return errors.New("proposed whitelist cannot be nil")
	}
	if _, ok := wl.PeerIDs[wm.peerID]; !ok {
		return errors.New("proposed whitelist must include the local node")
	}

	wm.mtx.Lock()
	wm.localState.Proposed = wl.DeepCopy()
	wm.localState.Ready = false
	_, whitelistSwitched := wm.checkTransitionLocked()
	stateCopy := wm.localState.DeepCopy()
	wm.mtx.Unlock()

	wm.broadcastLocalState(stateCopy)
	if whitelistSwitched {
		wm.whitelistUpdated(stateCopy.Current)
	}

	return nil
}

// clearProposal removes the active proposal.
func (wm *whitelistManager) clearProposal() {
	wm.mtx.Lock()
	if wm.localState.Proposed == nil {
		wm.mtx.Unlock()
		return
	}
	wm.localState.Proposed = nil
	wm.localState.Ready = false
	stateCopy := wm.localState.DeepCopy()
	wm.mtx.Unlock()

	wm.broadcastLocalState(stateCopy)
}

// forceWhitelist unilaterally replaces the current whitelist and clears
// any proposed whitelist.
func (wm *whitelistManager) forceWhitelist(wl *types.Whitelist) error {
	if wl == nil || wl.PeerIDs == nil {
		return errors.New("force whitelist cannot be nil")
	}
	if _, ok := wl.PeerIDs[wm.peerID]; !ok {
		return errors.New("force whitelist must include the local node")
	}

	wm.mtx.Lock()
	if wl.Equals(wm.localState.Current) {
		wm.mtx.Unlock()
		return errors.New("force whitelist must be different from the current whitelist")
	}
	wm.localState.Current = wl.DeepCopy()
	wm.localState.Proposed = nil
	wm.localState.Ready = false
	stateCopy := wm.localState.DeepCopy()
	wm.mtx.Unlock()

	wm.broadcastLocalState(stateCopy)
	wm.whitelistUpdated(wl)

	wm.log.Infof("Force whitelist applied with %d peers", len(wl.PeerIDs))

	return nil
}

// checkTransitionResult indicates the outcome of a transition check.
type checkTransitionResult uint8

const (
	transitionNoChange checkTransitionResult = iota
	transitionMarkReady
	transitionUnmarkReady
	transitionWhitelist
)

// checkTransitionLocked checks if the current whitelist state requires a change
// and if so, performs the change.
// This function MUST be called with the lock held.
func (wm *whitelistManager) checkTransitionLocked() (localStateChanged, whitelistSwitched bool) {
	if wm.localState.Proposed == nil {
		return false, false
	}

	connectedPeers := make(map[peer.ID]bool)
	for pid := range wm.localState.Current.PeerIDs {
		connectedPeers[pid] = wm.isConnected(pid)
	}
	for pid := range wm.localState.Proposed.PeerIDs {
		if _, exists := connectedPeers[pid]; !exists {
			connectedPeers[pid] = wm.isConnected(pid)
		}
	}

	switch checkTransition(
		wm.localState,
		wm.peerID,
		wm.peerStates,
		connectedPeers,
	) {
	case transitionMarkReady:
		wm.localState.Ready = true
		localStateChanged = true
		wm.log.Infof("Whitelist transition: ready to switch")
	case transitionUnmarkReady:
		wm.localState.Ready = false
		localStateChanged = true
		wm.log.Infof("Whitelist transition: ready state regressed, resetting")
	case transitionWhitelist:
		wm.localState.Current = wm.localState.Proposed
		wm.localState.Proposed = nil
		wm.localState.Ready = false
		localStateChanged = true
		whitelistSwitched = true
		wm.log.Infof("Whitelist transition complete: switched to new whitelist with %d peers", len(wm.localState.Current.PeerIDs))
	}
	return localStateChanged, whitelistSwitched
}

// checkTransition checks if a transition is warranted based on the current
// whitelist state of the local node and all the peers.
func checkTransition(
	localState types.WhitelistState,
	localPeerID peer.ID,
	peerStates map[peer.ID]*timestampedWhitelistState,
	connectedPeers map[peer.ID]bool,
) checkTransitionResult {
	proposed := localState.Proposed
	if proposed == nil {
		return transitionNoChange
	}

	currentPeers := localState.Current.PeerIDs
	proposedPeers := proposed.PeerIDs

	// Compute overlap: peers in both current and proposed whitelists.
	overlap := make(map[peer.ID]struct{})
	for pid := range currentPeers {
		if _, ok := proposedPeers[pid]; ok {
			overlap[pid] = struct{}{}
		}
	}

	nOverlap := len(overlap)
	if nOverlap == 0 {
		// Should never happen as the local node must be in the proposed whitelist
		return transitionNoChange
	}
	threshold := (2*nOverlap + 2) / 3 // ceil(2/3 * |overlap|)

	onlineAgreeing := 0
	onlineNotAgreeing := 0

	for pid := range overlap {
		if pid == localPeerID {
			onlineAgreeing++
			continue
		}

		pp, hasPP := peerStates[pid]
		connected := connectedPeers[pid]

		if !connected {
			// Disconnected overlap peers are not counted as disagreeing, but the
			// fixed threshold above still includes them, so they can block readiness.
			continue
		}

		if !hasPP {
			onlineNotAgreeing++
			continue
		}

		peerProposedMatchesOurs := pp.Proposed != nil && pp.Proposed.Equals(proposed)
		alreadySwitched := pp.Current.Equals(proposed)

		if peerProposedMatchesOurs || alreadySwitched {
			onlineAgreeing++
		} else {
			onlineNotAgreeing++
		}
	}

	// Phase 1: Ready check. Ready requires quorum over the full overlap set
	// and no online overlap peer actively disagreeing.
	shouldBeReady := onlineAgreeing >= threshold && onlineNotAgreeing == 0
	if !shouldBeReady {
		if localState.Ready {
			return transitionUnmarkReady
		}
		return transitionNoChange
	}

	// Phase 2: Commit check — all online overlap peers must be ready or
	// already switched.
	canCommit := true
	for pid := range overlap {
		if pid == localPeerID {
			continue
		}
		if !connectedPeers[pid] {
			continue
		}
		pp, hasPP := peerStates[pid]
		if !hasPP {
			canCommit = false
			break
		}
		alreadySwitched := pp.Current.Equals(proposed)
		if !pp.Ready && !alreadySwitched {
			canCommit = false
			break
		}
	}

	if canCommit {
		return transitionWhitelist
	}
	if !localState.Ready {
		return transitionMarkReady
	}
	return transitionNoChange
}
