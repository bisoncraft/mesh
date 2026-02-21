package tatanka

import (
	"io"
	"sync"
	"testing"

	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestCheckTransition tests the core transition logic.
func TestCheckTransition(t *testing.T) {
	local := randomPeerID(t)
	var p [5]peer.ID
	for i := range p {
		p[i] = randomPeerID(t)
	}

	wl := types.NewWhitelist

	ps := func(current, proposed *types.Whitelist, ready bool) *timestampedWhitelistState {
		return &timestampedWhitelistState{
			WhitelistState: types.WhitelistState{
				Current:  current,
				Proposed: proposed,
				Ready:    ready,
			},
		}
	}

	// Standard whitelists: overlap={local, p0}, threshold=2
	cur2 := wl([]peer.ID{local, p[0], p[1]})
	prop2 := wl([]peer.ID{local, p[0], p[2]})

	// Larger overlap: overlap={local, p0, p1}, threshold=2
	cur3 := wl([]peer.ID{local, p[0], p[1], p[2]})
	prop3 := wl([]peer.ID{local, p[0], p[1], p[3]})

	tests := []struct {
		name       string
		localState types.WhitelistState
		peerStates map[peer.ID]*timestampedWhitelistState
		connected  map[peer.ID]bool
		want       checkTransitionResult
	}{
		{
			name:       "no_proposal",
			localState: types.WhitelistState{Current: cur2},
			want:       transitionNoChange,
		},
		{
			name: "single_overlap_commits_immediately",
			// only the local peer is in both whitelists, so it can transition immediately
			localState: types.WhitelistState{
				Current:  wl([]peer.ID{local, p[0]}),
				Proposed: wl([]peer.ID{local, p[1]}),
			},
			want: transitionWhitelist,
		},
		{
			name: "below_threshold_peer_disconnected",
			// p0 not connected → agreeing=1, threshold=2
			localState: types.WhitelistState{Current: cur2, Proposed: prop2},
			want:       transitionNoChange,
		},
		{
			name: "connected_no_state_disagrees",
			// p0 connected but no stored state → counts as disagreeing
			localState: types.WhitelistState{Current: cur2, Proposed: prop2},
			connected:  map[peer.ID]bool{p[0]: true},
			want:       transitionNoChange,
		},
		{
			name:       "different_proposal_disagrees",
			localState: types.WhitelistState{Current: cur2, Proposed: prop2},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur2, wl([]peer.ID{local, p[0], p[4]}), false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionNoChange,
		},
		{
			name: "matching_proposed_marks_ready",
			// p0 agrees (proposed matches) but not Ready → can't commit
			localState: types.WhitelistState{Current: cur2, Proposed: prop2},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur2, prop2, false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionMarkReady,
		},
		{
			name: "already_switched_commits_immediately",
			// p0 Current equals our Proposed — already switched
			localState: types.WhitelistState{Current: cur2, Proposed: prop2},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(prop2, nil, false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionWhitelist,
		},
		{
			name: "threshold_met_disagreer_blocks",
			// overlap={local, p0, p1}, threshold=2. p0 agrees, p1 disagrees.
			localState: types.WhitelistState{Current: cur3, Proposed: prop3},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur3, prop3, false),
				p[1]: ps(cur3, wl([]peer.ID{local, p[0], p[1], p[4]}), false),
			},
			connected: map[peer.ID]bool{p[0]: true, p[1]: true},
			want:      transitionNoChange,
		},
		{
			name: "disconnected_does_not_disagree",
			// overlap={local, p0, p1}, threshold=2. p0 agrees, p1 offline.
			// agreeing=2, no disagreers → ready
			localState: types.WhitelistState{Current: cur3, Proposed: prop3},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur3, prop3, false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionMarkReady,
		},
		{
			name: "regression_below_threshold",
			// p0 disconnected → agreeing=1 < threshold=2
			localState: types.WhitelistState{Current: cur2, Proposed: prop2, Ready: true},
			want:       transitionUnmarkReady,
		},
		{
			name:       "regression_disagreer",
			localState: types.WhitelistState{Current: cur2, Proposed: prop2, Ready: true},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur2, wl([]peer.ID{local, p[0], p[4]}), false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionUnmarkReady,
		},
		{
			name: "regression_connected_no_state",
			// connected but no stored state → disagrees
			localState: types.WhitelistState{Current: cur2, Proposed: prop2, Ready: true},
			connected:  map[peer.ID]bool{p[0]: true},
			want:       transitionUnmarkReady,
		},

		{
			name: "commit_blocked_peer_not_ready",
			// p0 agrees (proposed matches) but not Ready → can't commit
			localState: types.WhitelistState{Current: cur2, Proposed: prop2, Ready: true},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur2, prop2, false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionNoChange,
		},
		{
			name:       "commit_all_ready",
			localState: types.WhitelistState{Current: cur2, Proposed: prop2, Ready: true},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur2, prop2, true),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionWhitelist,
		},
		{
			name: "commit_peer_already_switched",
			// p0 Current = our Proposed, not explicitly Ready → still commits
			localState: types.WhitelistState{Current: cur2, Proposed: prop2, Ready: true},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(prop2, nil, false),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionWhitelist,
		},
		{
			name: "commit_disconnected_peer_skipped",
			// overlap={local, p0, p1}. p0 ready, p1 disconnected → p1 skipped
			localState: types.WhitelistState{Current: cur3, Proposed: prop3, Ready: true},
			peerStates: map[peer.ID]*timestampedWhitelistState{
				p[0]: ps(cur3, prop3, true),
			},
			connected: map[peer.ID]bool{p[0]: true},
			want:      transitionWhitelist,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkTransition(tc.localState, local, tc.peerStates, tc.connected)
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// whitelistManager — method tests (wiring, callbacks, input validation)
// ────────────────────────────────────────────────────────────────────────────

type wmHarness struct {
	wm      *whitelistManager
	localID peer.ID
	peers   []peer.ID

	mu               sync.Mutex
	stateUpdates     []*types.WhitelistState
	whitelistUpdates []*types.Whitelist
	connected        map[peer.ID]bool
}

func newWMHarness(t *testing.T, numPeers int) *wmHarness {
	t.Helper()

	localID := randomPeerID(t)
	peers := make([]peer.ID, numPeers)
	for i := range peers {
		peers[i] = randomPeerID(t)
	}

	allPeers := make([]peer.ID, 0, numPeers+1)
	allPeers = append(allPeers, localID)
	allPeers = append(allPeers, peers...)

	h := &wmHarness{
		localID:   localID,
		peers:     peers,
		connected: make(map[peer.ID]bool),
	}

	h.wm = newWhitelistManager(&whitelistManagerConfig{
		log:    slog.NewBackend(io.Discard).Logger("test"),
		peerID: localID,
		isConnected: func(pid peer.ID) bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.connected[pid]
		},
		whitelist: types.NewWhitelist(allPeers),
		whitelistUpdated: func(newWl *types.Whitelist) {
			h.mu.Lock()
			h.whitelistUpdates = append(h.whitelistUpdates, newWl)
			h.mu.Unlock()
		},
		broadcastLocalState: func(ws *types.WhitelistState) {
			h.mu.Lock()
			h.stateUpdates = append(h.stateUpdates, ws)
			h.mu.Unlock()
		},
	})

	return h
}

func (h *wmHarness) setConnected(pids ...peer.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, pid := range pids {
		h.connected[pid] = true
	}
}

func (h *wmHarness) resetRecords() {
	h.mu.Lock()
	h.stateUpdates = nil
	h.whitelistUpdates = nil
	h.mu.Unlock()
}

func TestWMInitialState(t *testing.T) {
	h := newWMHarness(t, 2)

	state := h.wm.getLocalWhitelistState()
	if state.Current == nil || len(state.Current.PeerIDs) != 3 {
		t.Fatalf("expected 3-peer current whitelist, got %v", state.Current)
	}
	if state.Proposed != nil {
		t.Fatal("Proposed should be nil initially")
	}
	if state.Ready {
		t.Fatal("Ready should be false initially")
	}
}

func TestWMGetPeerWhitelistState(t *testing.T) {
	h := newWMHarness(t, 2)

	t.Run("local_returns_own_state", func(t *testing.T) {
		got := h.wm.getPeerWhitelistState(h.localID)
		expected := h.wm.getLocalWhitelistState()
		if !got.Current.Equals(expected.Current) {
			t.Fatal("local peer lookup should match getLocalWhitelistState")
		}
	})

	t.Run("unknown_returns_nil", func(t *testing.T) {
		if h.wm.getPeerWhitelistState(randomPeerID(t)) != nil {
			t.Fatal("expected nil for unknown peer")
		}
	})

	t.Run("stored_peer", func(t *testing.T) {
		ws := &types.WhitelistState{Current: types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})}
		h.wm.updatePeerWhitelistState(h.peers[0], ws, 100)

		got := h.wm.getPeerWhitelistState(h.peers[0])
		if got == nil || !got.Current.Equals(ws.Current) {
			t.Fatal("stored peer state should be retrievable")
		}
	})
}

func TestWMProposeWhitelist(t *testing.T) {
	t.Run("stores_and_broadcasts", func(t *testing.T) {
		h := newWMHarness(t, 2)
		proposed := types.NewWhitelist([]peer.ID{h.localID, h.peers[0], randomPeerID(t)})
		if err := h.wm.proposeWhitelist(proposed); err != nil {
			t.Fatal(err)
		}

		state := h.wm.getLocalWhitelistState()
		if !state.Proposed.Equals(proposed) {
			t.Fatal("Proposed doesn't match")
		}
		if state.Ready {
			t.Fatal("should not be ready")
		}

		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.stateUpdates) != 1 {
			t.Fatalf("expected 1 broadcast, got %d", len(h.stateUpdates))
		}
	})

	t.Run("rejects_nil", func(t *testing.T) {
		h := newWMHarness(t, 1)
		if err := h.wm.proposeWhitelist(nil); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("rejects_without_local", func(t *testing.T) {
		h := newWMHarness(t, 2)
		if err := h.wm.proposeWhitelist(types.NewWhitelist([]peer.ID{h.peers[0]})); err == nil {
			t.Fatal("expected error")
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.stateUpdates) != 0 {
			t.Fatal("should not broadcast on error")
		}
	})

	t.Run("deep_copies_input", func(t *testing.T) {
		h := newWMHarness(t, 1)
		proposed := types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})
		h.wm.proposeWhitelist(proposed)

		proposed.PeerIDs[randomPeerID(t)] = struct{}{}

		state := h.wm.getLocalWhitelistState()
		if len(state.Proposed.PeerIDs) != 2 {
			t.Fatal("stored proposal should not be affected by caller mutation")
		}
	})
}

func TestWMClearProposal(t *testing.T) {
	t.Run("clears_and_broadcasts", func(t *testing.T) {
		h := newWMHarness(t, 2)
		h.wm.proposeWhitelist(types.NewWhitelist([]peer.ID{h.localID, h.peers[0], randomPeerID(t)}))
		h.resetRecords()

		h.wm.clearProposal()

		if h.wm.getLocalWhitelistState().Proposed != nil {
			t.Fatal("Proposed should be nil after clear")
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.stateUpdates) != 1 {
			t.Fatalf("expected 1 broadcast, got %d", len(h.stateUpdates))
		}
	})

	t.Run("noop_without_proposal", func(t *testing.T) {
		h := newWMHarness(t, 1)
		h.wm.clearProposal()

		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.stateUpdates) != 0 {
			t.Fatal("should not broadcast on noop")
		}
	})
}

func TestWMForceWhitelist(t *testing.T) {
	t.Run("replaces_current_and_clears_proposal", func(t *testing.T) {
		h := newWMHarness(t, 2)
		h.wm.proposeWhitelist(types.NewWhitelist([]peer.ID{h.localID, h.peers[0], randomPeerID(t)}))
		// Store a peer state to verify it survives force.
		peerWS := &types.WhitelistState{Current: types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})}
		h.wm.updatePeerWhitelistState(h.peers[0], peerWS, 100)
		h.resetRecords()

		forced := types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})
		if err := h.wm.forceWhitelist(forced); err != nil {
			t.Fatal(err)
		}

		state := h.wm.getLocalWhitelistState()
		if !state.Current.Equals(forced) {
			t.Fatal("Current should match forced")
		}
		if state.Proposed != nil {
			t.Fatal("Proposed should be nil after force")
		}
		if h.wm.getPeerWhitelistState(h.peers[0]) == nil {
			t.Fatal("peer state should be preserved after force")
		}

		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.stateUpdates) != 1 {
			t.Fatalf("expected 1 broadcast, got %d", len(h.stateUpdates))
		}
		if len(h.whitelistUpdates) != 1 || !h.whitelistUpdates[0].Equals(forced) {
			t.Fatal("expected whitelistUpdated with forced whitelist")
		}
	})

	t.Run("rejects_nil", func(t *testing.T) {
		h := newWMHarness(t, 1)
		if err := h.wm.forceWhitelist(nil); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("rejects_without_local", func(t *testing.T) {
		h := newWMHarness(t, 2)
		if err := h.wm.forceWhitelist(types.NewWhitelist([]peer.ID{h.peers[0]})); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("rejects_same_whitelist", func(t *testing.T) {
		h := newWMHarness(t, 2)
		if err := h.wm.forceWhitelist(h.wm.getWhitelist()); err == nil {
			t.Fatal("expected error for identical whitelist")
		}
	})
}

func TestWMUpdatePeerState(t *testing.T) {
	t.Run("stores_and_retrieves", func(t *testing.T) {
		h := newWMHarness(t, 2)
		ws := &types.WhitelistState{Current: types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})}

		if !h.wm.updatePeerWhitelistState(h.peers[0], ws, 100) {
			t.Fatal("expected true for new state")
		}

		got := h.wm.getPeerWhitelistState(h.peers[0])
		if got == nil || !got.Current.Equals(ws.Current) {
			t.Fatal("stored state should match")
		}

		// No proposal active → no transition → no broadcast.
		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.stateUpdates) != 0 {
			t.Fatalf("expected 0 broadcasts, got %d", len(h.stateUpdates))
		}
	})

	t.Run("rejects_self", func(t *testing.T) {
		h := newWMHarness(t, 1)
		ws := &types.WhitelistState{Current: types.NewWhitelist([]peer.ID{h.localID})}
		if h.wm.updatePeerWhitelistState(h.localID, ws, 100) {
			t.Fatal("self-update should return false")
		}
	})

	t.Run("rejects_nil_current", func(t *testing.T) {
		h := newWMHarness(t, 1)
		if h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{}, 100) {
			t.Fatal("nil Current should return false")
		}
		if h.wm.getPeerWhitelistState(h.peers[0]) != nil {
			t.Fatal("nil Current should not be stored")
		}
	})

	t.Run("stale_timestamp_rejected", func(t *testing.T) {
		h := newWMHarness(t, 2)
		wl1 := types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})
		wl2 := types.NewWhitelist([]peer.ID{h.localID, h.peers[1]})

		h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{Current: wl1}, 200)

		if h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{Current: wl2}, 100) {
			t.Fatal("older timestamp should return false")
		}
		if h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{Current: wl2}, 200) {
			t.Fatal("equal timestamp should return false")
		}

		got := h.wm.getPeerWhitelistState(h.peers[0])
		if !got.Current.Equals(wl1) {
			t.Fatal("state should not change from stale update")
		}
	})

	t.Run("nil_clears_existing", func(t *testing.T) {
		h := newWMHarness(t, 1)
		ws := &types.WhitelistState{Current: types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})}
		h.wm.updatePeerWhitelistState(h.peers[0], ws, 100)

		if !h.wm.updatePeerWhitelistState(h.peers[0], nil, 0) {
			t.Fatal("nil clear should return true")
		}
		if h.wm.getPeerWhitelistState(h.peers[0]) != nil {
			t.Fatal("state should be nil after clear")
		}
	})

	t.Run("nil_noop_for_absent", func(t *testing.T) {
		h := newWMHarness(t, 1)
		if h.wm.updatePeerWhitelistState(h.peers[0], nil, 0) {
			t.Fatal("nil for absent peer should return false")
		}
	})

	t.Run("deep_copies_input", func(t *testing.T) {
		h := newWMHarness(t, 1)
		original := types.NewWhitelist([]peer.ID{h.localID, h.peers[0]})
		ws := &types.WhitelistState{Current: original}
		h.wm.updatePeerWhitelistState(h.peers[0], ws, 100)

		// Mutate caller-owned state after update.
		ws.Current.PeerIDs[randomPeerID(t)] = struct{}{}

		got := h.wm.getPeerWhitelistState(h.peers[0])
		if len(got.Current.PeerIDs) != 2 {
			t.Fatalf("expected 2 peers in stored state, got %d", len(got.Current.PeerIDs))
		}
	})
}

// TestWMTransitionEndToEnd verifies the full propose → agree → ready →
// commit flow through the whitelistManager methods.
func TestWMTransitionEndToEnd(t *testing.T) {
	h := newWMHarness(t, 2) // current = {local, p0, p1}
	h.setConnected(h.peers[0])

	proposed := types.NewWhitelist([]peer.ID{h.localID, h.peers[0], randomPeerID(t)})
	// overlap = {local, p0}, threshold = 2

	if err := h.wm.proposeWhitelist(proposed); err != nil {
		t.Fatal(err)
	}
	h.resetRecords()

	// Step 1: Peer reports matching proposal → local becomes ready.
	current := h.wm.getWhitelist()
	h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{
		Current:  current,
		Proposed: proposed,
	}, 100)

	state := h.wm.getLocalWhitelistState()
	if !state.Ready {
		t.Fatal("should be ready after peer agrees")
	}

	h.mu.Lock()
	if len(h.stateUpdates) != 1 {
		t.Fatalf("expected 1 broadcast for ready, got %d", len(h.stateUpdates))
	}
	h.mu.Unlock()
	h.resetRecords()

	// Step 2: Peer reports ready → whitelist commits.
	h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{
		Current:  current,
		Proposed: proposed,
		Ready:    true,
	}, 200)

	state = h.wm.getLocalWhitelistState()
	if !state.Current.Equals(proposed) {
		t.Fatal("Current should be the proposed whitelist after commit")
	}
	if state.Proposed != nil {
		t.Fatal("Proposed should be nil after commit")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.stateUpdates) != 1 {
		t.Fatalf("expected 1 broadcast for commit, got %d", len(h.stateUpdates))
	}
	if len(h.whitelistUpdates) != 1 || !h.whitelistUpdates[0].Equals(proposed) {
		t.Fatal("expected whitelistUpdated with new whitelist")
	}
}

// TestWMNilClearTriggersRegression verifies that clearing a peer's state via
// nil update re-evaluates the transition and regresses readiness when the
// agreeing peer is removed.
func TestWMNilClearTriggersRegression(t *testing.T) {
	h := newWMHarness(t, 2) // current = {local, p0, p1}
	h.setConnected(h.peers[0])

	proposed := types.NewWhitelist([]peer.ID{h.localID, h.peers[0], randomPeerID(t)})
	if err := h.wm.proposeWhitelist(proposed); err != nil {
		t.Fatal(err)
	}

	// Peer agrees → local becomes ready.
	h.wm.updatePeerWhitelistState(h.peers[0], &types.WhitelistState{
		Current:  h.wm.getWhitelist(),
		Proposed: proposed,
	}, 100)

	if !h.wm.getLocalWhitelistState().Ready {
		t.Fatal("should be ready before clearing peer state")
	}
	h.resetRecords()

	// Clear the agreeing peer → ready should regress.
	if !h.wm.updatePeerWhitelistState(h.peers[0], nil, 0) {
		t.Fatal("nil clear should return true")
	}

	state := h.wm.getLocalWhitelistState()
	if state.Ready {
		t.Fatal("should regress from ready after peer state clear")
	}
	if state.Proposed == nil {
		t.Fatal("proposal should remain pending")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.stateUpdates) != 1 {
		t.Fatalf("expected 1 broadcast for regression, got %d", len(h.stateUpdates))
	}
}
