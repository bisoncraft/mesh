package tatanka

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type whitelistPeer struct {
	ID      string `json:"id"`
	Address string `json:"address,omitempty"`
}

type whitelistFile struct {
	Peers []whitelistPeer `json:"peers"`
}

// whitelist is the parsed whitelist file.
type whitelist struct {
	peers []*peer.AddrInfo
}

func (m *whitelist) allPeerIDs() map[peer.ID]struct{} {
	allPeerIDs := make(map[peer.ID]struct{}, len(m.peers))
	for _, peer := range m.peers {
		allPeerIDs[peer.ID] = struct{}{}
	}
	return allPeerIDs
}

func (m *whitelist) toFile() whitelistFile {
	peers := make([]whitelistPeer, len(m.peers))

	for i, peer := range m.peers {
		address := ""
		if len(peer.Addrs) > 0 {
			address = peer.Addrs[0].String()
		}
		peers[i] = whitelistPeer{
			ID:      peer.ID.String(),
			Address: address,
		}
	}

	return whitelistFile{
		Peers: peers,
	}
}

// peerIDsBytes returns the whitelist peer IDs as byte slices.
func (m *whitelist) peerIDsBytes() [][]byte {
	ids := make([][]byte, 0, len(m.peers))
	for _, p := range m.peers {
		ids = append(ids, []byte(p.ID))
	}
	return ids
}

func loadWhitelist(path string) (*whitelist, error) {
	if path == "" {
		return nil, errors.New("no whitelist path provided")
	}

	var file whitelistFile
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, err
	}

	peers := make([]*peer.AddrInfo, 0, len(file.Peers))

	for _, pData := range file.Peers {
		peerID, err := peer.Decode(pData.ID)
		if err != nil {
			return nil, err
		}

		addrs := make([]ma.Multiaddr, 0, 1)
		if pData.Address != "" {
			addr, err := ma.NewMultiaddr(pData.Address)
			if err != nil {
				return nil, err
			}
			addrs = append(addrs, addr)
		}

		peers = append(peers, &peer.AddrInfo{ID: peerID, Addrs: addrs})
	}
	return &whitelist{
		peers: peers,
	}, nil
}
