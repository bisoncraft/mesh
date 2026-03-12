package tatanka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bisoncraft/mesh/oracle/sources/utils"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/libp2p/go-libp2p/core/peer"
)

// saveWhitelist writes the whitelist to disk atomically.
func saveWhitelist(path string, wl *types.Whitelist) error {
	data, err := json.Marshal(wl)
	if err != nil {
		return err
	}
	return utils.AtomicWriteFile(path, data)
}

// loadWhitelist loads a whitelist from disk.
func loadWhitelist(path string) (*types.Whitelist, error) {
	if path == "" {
		return nil, errors.New("no whitelist path provided")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var wl types.Whitelist
	if err := json.Unmarshal(data, &wl); err != nil {
		return nil, err
	}
	return &wl, nil
}

// flexibleWhitelistMatch returns true when any of {myCurrentWl, myProposedWl}
// matches any of {theirCurrent, theirProposed}. myProposedWl may be nil.
func flexibleWhitelistMatch(myCurrentWl, myProposedWl *types.Whitelist, theirCurrent, theirProposed [][]byte) bool {
	myWhitelists := []*types.Whitelist{myCurrentWl}
	if myProposedWl != nil {
		myWhitelists = append(myWhitelists, myProposedWl)
	}

	theirCurrentWl := types.DecodePeerIDBytes(theirCurrent)
	theirProposedWl := types.DecodePeerIDBytes(theirProposed)

	for _, myWl := range myWhitelists {
		if theirCurrentWl != nil && myWl.Equals(theirCurrentWl) {
			return true
		}
		if theirProposedWl != nil && myWl.Equals(theirProposedWl) {
			return true
		}
	}
	return false
}

// initWhitelist initializes the whitelist from the provided peers or the file on disk.
// If forceUpdate is true, the providedPeers will be saved to the file on disk and used
// as the current whitelist. Otherwise, the existing whitelist in the file will be used,
// unless this is the first time the file is created. If the file already exists, forceUpdate
// is false, and the providedPeers do not match the existing whitelist, an error is returned.
func initWhitelist(dataDir string, providedPeers []peer.ID, forceUpdate bool, localPeerID peer.ID) (*types.Whitelist, error) {
	wlPath := filepath.Join(dataDir, whitelistFileName)

	var providedWL *types.Whitelist
	if len(providedPeers) > 0 {
		// Make sure that the local peer ID is included in the provided whitelist.
		providedWL = types.NewWhitelist(providedPeers)
		if _, found := providedWL.PeerIDs[localPeerID]; !found {
			return nil, fmt.Errorf("local node peer ID %s is not included in whitelist", localPeerID)
		}
	}

	if forceUpdate {
		if providedWL == nil {
			return nil, errors.New("force whitelist update requires provided peers")
		}
		if err := saveWhitelist(wlPath, providedWL); err != nil {
			return nil, err
		}
		return providedWL, nil
	}

	existingWL, err := loadWhitelist(wlPath)
	if err != nil {
		if os.IsNotExist(err) && providedWL != nil {
			if err := saveWhitelist(wlPath, providedWL); err != nil {
				return nil, err
			}
			return providedWL, nil
		}
		return nil, err
	}

	if providedWL != nil && !existingWL.Equals(providedWL) {
		return nil, fmt.Errorf("provided whitelist does not match existing whitelist in %s; use --forcewl to overwrite", wlPath)
	}

	if _, found := existingWL.PeerIDs[localPeerID]; !found {
		return nil, fmt.Errorf("local node peer ID %s is not included in whitelist", localPeerID)
	}

	return existingWL, nil
}
