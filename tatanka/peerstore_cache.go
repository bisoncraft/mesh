package tatanka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

// peerstoreCache persists known peer addresses to disk so that a node can
// reconnect after a cold restart without relying on bootstrap flags.
type peerstoreCache struct {
	path         string
	log          slog.Logger
	host         host.Host
	getWhitelist func() map[peer.ID]struct{}
	writingMtx   sync.Mutex
}

func newPeerstoreCache(log slog.Logger, path string, host host.Host, getWhitelist func() map[peer.ID]struct{}) *peerstoreCache {
	return &peerstoreCache{
		path:         path,
		log:          log,
		host:         host,
		getWhitelist: getWhitelist,
	}
}

// load reads cached addresses from disk and adds them to the peerstore.
// No-op if the file doesn't exist.
func (c *peerstoreCache) load() {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		c.log.Warnf("Failed to read peerstore cache: %v", err)
		return
	}

	var cache map[string][]string
	if err := json.Unmarshal(data, &cache); err != nil {
		c.log.Warnf("Failed to parse peerstore cache: %v", err)
		return
	}

	for pidStr, addrStrs := range cache {
		pid, err := peer.Decode(pidStr)
		if err != nil {
			c.log.Warnf("Skipping invalid peer ID in cache: %s", pidStr)
			continue
		}
		addrs := make([]ma.Multiaddr, 0, len(addrStrs))
		for _, a := range addrStrs {
			addr, err := ma.NewMultiaddr(a)
			if err != nil {
				c.log.Warnf("Skipping invalid address in cache for %s: %s", pidStr, a)
				continue
			}
			addrs = append(addrs, addr)
		}
		if len(addrs) > 0 {
			c.host.Peerstore().AddAddrs(pid, addrs, peerstore.PermanentAddrTTL)
		}
	}

	c.log.Infof("Loaded peerstore cache with %d peers", len(cache))
}

// save writes the current addresses for the whitelist peers to the disk.
func (c *peerstoreCache) save() {
	c.writingMtx.Lock()
	defer c.writingMtx.Unlock()

	cache := make(map[string][]string)
	for pid := range c.getWhitelist() {
		addrs := c.host.Peerstore().Addrs(pid)
		if len(addrs) == 0 {
			continue
		}
		addrStrs := make([]string, len(addrs))
		for i, a := range addrs {
			addrStrs[i] = a.String()
		}
		cache[pid.String()] = addrStrs
	}

	data, err := json.Marshal(cache)
	if err != nil {
		c.log.Errorf("Failed to marshal peerstore cache: %v", err)
		return
	}

	if err := atomicWriteFile(c.path, data); err != nil {
		c.log.Errorf("Failed to write peerstore cache: %v", err)
		return
	}
}

// run periodically persists peerstore addresses for current whitelist peers.
func (c *peerstoreCache) run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.save()
		}
	}
}

// parseBootstrapAddrs parses multiaddr strings into peer.AddrInfo slices,
// merging multiple addresses for the same peer.
func parseBootstrapAddrs(addrs []string) ([]peer.AddrInfo, error) {
	peerMap := make(map[peer.ID]*peer.AddrInfo)

	for _, addrStr := range addrs {
		maddr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse address %q: %w", addrStr, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse peer info from %q: %w", addrStr, err)
		}

		if existing, ok := peerMap[info.ID]; ok {
			existing.Addrs = append(existing.Addrs, info.Addrs...)
		} else {
			peerMap[info.ID] = info
		}
	}

	result := make([]peer.AddrInfo, 0, len(peerMap))
	for _, info := range peerMap {
		result = append(result, *info)
	}
	return result, nil
}

// seedBootstrapAddrs adds the given addresses to the peerstore.
// If any address is invalid an error is returned.
func seedBootstrapAddrs(host host.Host, addrs []string) error {
	infos, err := parseBootstrapAddrs(addrs)
	if err != nil {
		return err
	}
	for _, info := range infos {
		host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	}
	return nil
}
