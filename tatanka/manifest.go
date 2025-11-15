package tatanka

import (
	"encoding/json"
	"errors"
	"math/big"
	"os"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type bootstrapPeer struct {
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
}

// bondAsset is a bond asset a peer can use to bond to the mesh.
type bondAsset struct {
	AssetID uint32   `json:"asset_id"`
	Amount  *big.Int `json:"amount"`
}

// manifestFile is the JSON format of the manifest file.
type manifestFile struct {
	BootstrapPeers    []bootstrapPeer `json:"bootstrap_peers"`
	NonBootstrapPeers []string        `json:"non_bootstrap_peers"`
	BondAssets        []*bondAsset    `json:"bond_assets"`
}

// manifest is the parsed manifest file.
type manifest struct {
	bootstrapPeers    []*peer.AddrInfo
	nonBootstrapPeers []peer.ID
	bondAssets        []*bondAsset
}

func (m *manifest) allPeerIDs() map[peer.ID]struct{} {
	allPeerIDs := make(map[peer.ID]struct{}, len(m.bootstrapPeers)+len(m.nonBootstrapPeers))
	for _, peer := range m.bootstrapPeers {
		allPeerIDs[peer.ID] = struct{}{}
	}
	for _, peer := range m.nonBootstrapPeers {
		allPeerIDs[peer] = struct{}{}
	}
	return allPeerIDs
}

func (m *manifest) toManifestFile() manifestFile {
	bootstrapPeers := make([]bootstrapPeer, len(m.bootstrapPeers))
	for i, peerInfo := range m.bootstrapPeers {
		addrs := make([]string, len(peerInfo.Addrs))
		for j, addr := range peerInfo.Addrs {
			addrs[j] = addr.String()
		}
		bootstrapPeers[i] = bootstrapPeer{
			ID:        peerInfo.ID.String(),
			Addresses: addrs,
		}
	}

	nonBootstrapPeers := make([]string, len(m.nonBootstrapPeers))
	for i, peerID := range m.nonBootstrapPeers {
		nonBootstrapPeers[i] = peerID.String()
	}

	return manifestFile{
		BootstrapPeers:    bootstrapPeers,
		NonBootstrapPeers: nonBootstrapPeers,
		BondAssets:        m.bondAssets,
	}
}

func loadManifest(path, url string) (*manifest, error) {
	if path == "" && url == "" {
		return nil, errors.New("no manifest path or URL provided")
	}
	if path != "" && url != "" {
		return nil, errors.New("both manifest path and URL provided")
	}

	var mf manifestFile
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &mf); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("URL manifest not supported yet")
	}

	// Parse the peer ID and addresses for the bootstrap peers
	bootstrapPeers := make([]*peer.AddrInfo, 0, len(mf.BootstrapPeers))
	for _, pData := range mf.BootstrapPeers {
		id, err := peer.Decode(pData.ID)
		if err != nil {
			return nil, err
		}

		addrs := make([]ma.Multiaddr, len(pData.Addresses))
		for i, addr := range pData.Addresses {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				return nil, err
			}
			addrs[i] = maddr
		}

		bootstrapPeers = append(bootstrapPeers, &peer.AddrInfo{
			ID:    id,
			Addrs: addrs,
		})
	}

	// Parse the peer IDs for the non-bootstrap peers
	nonBootstrapPeers := make([]peer.ID, 0, len(mf.NonBootstrapPeers))
	for _, peerID := range mf.NonBootstrapPeers {
		id, err := peer.Decode(peerID)
		if err != nil {
			return nil, err
		}
		nonBootstrapPeers = append(nonBootstrapPeers, id)
	}

	return &manifest{
		bootstrapPeers:    bootstrapPeers,
		nonBootstrapPeers: nonBootstrapPeers,
		bondAssets:        mf.BondAssets,
	}, nil
}
