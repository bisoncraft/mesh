package admin

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
)

type proposeWhitelistRequest struct {
	Peers []string `json:"peers"`
}

// handleProposeWhitelist sets (POST) or clears (DELETE) a whitelist proposal.
func (s *Server) handleProposeWhitelist(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		var req proposeWhitelistRequest
		if err := decodeStrictJSONBody(w, r, &req); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		if len(req.Peers) == 0 {
			http.Error(w, "peers list cannot be empty", http.StatusBadRequest)
			return
		}

		// A node cannot propose a whitelist that removes itself.
		state := s.getState()
		if !slices.Contains(req.Peers, state.OurPeerID) {
			http.Error(w, "proposed whitelist must include our own peer", http.StatusBadRequest)
			return
		}

		if err := s.proposeWhitelist(req.Peers); err != nil {
			http.Error(w, "failed to set proposal: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "proposal set"})

	case "DELETE":
		s.clearProposal()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "proposal cleared"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

type adoptWhitelistRequest struct {
	PeerID string `json:"peer_id"`
}

// handleAdoptWhitelist replaces our whitelist with a mismatched peer's current whitelist.
func (s *Server) handleAdoptWhitelist(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req adoptWhitelistRequest
	if err := decodeStrictJSONBody(w, r, &req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.PeerID == "" {
		http.Error(w, "peer_id is required", http.StatusBadRequest)
		return
	}

	state := s.getState()
	pi, found := state.Peers[req.PeerID]

	if !found {
		http.Error(w, "peer not found", http.StatusNotFound)
		return
	}

	if pi.State != StateWhitelistMismatch {
		http.Error(w, "peer is not in whitelist_mismatch state", http.StatusBadRequest)
		return
	}

	if pi.WhitelistState == nil || pi.WhitelistState.Current == nil || len(pi.WhitelistState.Current.PeerIDs) == 0 {
		http.Error(w, "peer has no whitelist to adopt", http.StatusBadRequest)
		return
	}

	// Convert the peer's current whitelist to string slice for forceWhitelist.
	peerWlStrings := make([]string, 0, len(pi.WhitelistState.Current.PeerIDs))
	for pid := range pi.WhitelistState.Current.PeerIDs {
		peerWlStrings = append(peerWlStrings, pid.String())
	}

	if err := s.forceWhitelist(peerWlStrings); err != nil {
		http.Error(w, "failed to adopt whitelist: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "whitelist adopted"})
}

func decodeStrictJSONBody(w http.ResponseWriter, r *http.Request, dest any) error {
	r.Body = http.MaxBytesReader(w, r.Body, adminRequestBodyLimitBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}
