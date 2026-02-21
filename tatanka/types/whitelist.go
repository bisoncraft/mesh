package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// WhitelistState contains a node's whitelist-related state. Current is the
// active whitelist. Proposed, when non-nil, is the whitelist the node wants
// to transition to. Ready indicates the node has observed sufficient
// agreement on the proposal (phase 1 of the two-phase commit) and will
// commit the switch once all ready peers are confirmed.
type WhitelistState struct {
	Current  *Whitelist `json:"current"`
	Proposed *Whitelist `json:"proposed,omitempty"`
	Ready    bool       `json:"ready"`
}

// DeepCopy returns a fully independent copy of the whitelist state.
func (ws *WhitelistState) DeepCopy() *WhitelistState {
	if ws == nil {
		return nil
	}
	return &WhitelistState{
		Current:  ws.Current.DeepCopy(),
		Proposed: ws.Proposed.DeepCopy(),
		Ready:    ws.Ready,
	}
}

// Whitelist is a set of peer IDs authorized to participate in the mesh.
// The hash is lazily computed and cached for fast equality comparisons
// during consensus checks.
type Whitelist struct {
	PeerIDs  map[peer.ID]struct{}
	hashOnce sync.Once
	hashVal  string
}

// DeepCopy returns a fully independent copy of the whitelist.
func (w *Whitelist) DeepCopy() *Whitelist {
	if w == nil {
		return nil
	}
	peerIDs := make(map[peer.ID]struct{}, len(w.PeerIDs))
	for pid := range w.PeerIDs {
		peerIDs[pid] = struct{}{}
	}
	return &Whitelist{PeerIDs: peerIDs}
}

// NewWhitelist creates a new Whitelist from a slice of peer IDs.
func NewWhitelist(peerIDs []peer.ID) *Whitelist {
	set := make(map[peer.ID]struct{}, len(peerIDs))
	for _, pid := range peerIDs {
		set[pid] = struct{}{}
	}
	return &Whitelist{PeerIDs: set}
}

// hashPeerIDSet returns a deterministic hex hash for a set of peer IDs.
// Returns an empty string for nil or empty sets.
func hashPeerIDSet(ids map[peer.ID]struct{}) string {
	if len(ids) == 0 {
		return ""
	}
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id.String())
	}
	sort.Strings(sorted)
	h := sha256.Sum256([]byte(strings.Join(sorted, ",")))
	return hex.EncodeToString(h[:])
}

// Hash returns a deterministic hex hash for the whitelist's peer IDs.
// The result is cached after the first call.
func (w *Whitelist) Hash() string {
	w.hashOnce.Do(func() {
		w.hashVal = hashPeerIDSet(w.PeerIDs)
	})
	return w.hashVal
}

// Equals returns true if both whitelists contain exactly the same
// set of peer IDs.
func (w *Whitelist) Equals(other *Whitelist) bool {
	if w == nil || other == nil {
		return w == other
	}
	return w.Hash() == other.Hash()
}

// MarshalJSON serializes the whitelist as a sorted JSON array of peer ID strings.
func (w *Whitelist) MarshalJSON() ([]byte, error) {
	strs := make([]string, 0, len(w.PeerIDs))
	for pid := range w.PeerIDs {
		strs = append(strs, pid.String())
	}
	sort.Strings(strs)
	return json.Marshal(strs)
}

// UnmarshalJSON deserializes a JSON array of peer ID strings into the whitelist.
func (w *Whitelist) UnmarshalJSON(data []byte) error {
	var strs []string
	if err := json.Unmarshal(data, &strs); err != nil {
		return err
	}
	w.PeerIDs = make(map[peer.ID]struct{}, len(strs))
	for _, s := range strs {
		pid, err := peer.Decode(s)
		if err != nil {
			return err
		}
		w.PeerIDs[pid] = struct{}{}
	}
	return nil
}

// PeerIDsBytes returns the whitelist peer IDs as byte slices.
func (w *Whitelist) PeerIDsBytes() [][]byte {
	ids := make([][]byte, 0, len(w.PeerIDs))
	for pid := range w.PeerIDs {
		ids = append(ids, []byte(pid))
	}
	return ids
}

// DecodePeerIDBytes decodes a slice of byte-encoded peer IDs (as produced by
// PeerIDsBytes) into a Whitelist. Entries that fail to decode are silently
// skipped. A nil or empty input returns nil.
func DecodePeerIDBytes(raw [][]byte) *Whitelist {
	if len(raw) == 0 {
		return nil
	}
	set := make(map[peer.ID]struct{}, len(raw))
	for _, b := range raw {
		pid, err := peer.IDFromBytes(b)
		if err != nil {
			continue
		}
		set[pid] = struct{}{}
	}
	return &Whitelist{PeerIDs: set}
}
