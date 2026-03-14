package tatanka

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"
	eventbus "github.com/libp2p/go-libp2p/p2p/host/eventbus"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// runPeerIdentificationUpdates updates the peerstore, cached peerstore, and published
// bootstrap list to only contain the addresses present in the identify event.
func (t *TatankaNode) runPeerIdentificationUpdates(ctx context.Context) {
	sub, err := t.node.EventBus().Subscribe(&event.EvtPeerIdentificationCompleted{}, eventbus.BufSize(32))
	if err != nil {
		t.log.Warnf("Failed to subscribe to peer identification updates; using timer fallback only: %v", err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-sub.Out():
			if !ok {
				return
			}
			t.handlePeerIdentificationCompleted(evt.(event.EvtPeerIdentificationCompleted))
		}
	}
}

func (t *TatankaNode) handlePeerIdentificationCompleted(evt event.EvtPeerIdentificationCompleted) {
	if evt.Peer == t.node.ID() {
		return
	}

	if _, ok := t.whitelistManager.getWhitelist().PeerIDs[evt.Peer]; !ok {
		return
	}

	addrs := t.authoritativePeerAddrs(evt)
	addrs = filterPersistentPeerAddrs(addrs, t.localDevMode())
	if len(addrs) == 0 {
		t.log.Debugf("Ignoring identified addresses for %s: filtered result empty", evt.Peer)
		return
	}

	ps := t.node.Peerstore()
	ps.ClearAddrs(evt.Peer)
	ps.AddAddrs(evt.Peer, addrs, peerstore.PermanentAddrTTL)

	if t.peerstoreCache != nil {
		t.peerstoreCache.save()
	}
	if t.bootstrapList != nil {
		t.bootstrapList.publish()
	}

	t.log.Debugf("Reconciled identified addresses for %s (%d addresses)", evt.Peer, len(addrs))
}

// authoritativePeerAddrs prefers signed peer record addresses and falls back
// to identify listen addresses.
func (t *TatankaNode) authoritativePeerAddrs(evt event.EvtPeerIdentificationCompleted) []ma.Multiaddr {
	if evt.SignedPeerRecord == nil {
		return dedupeMultiaddrs(evt.ListenAddrs)
	}

	addrs, err := signedPeerRecordAddrs(evt.SignedPeerRecord, evt.Peer)
	if err != nil {
		t.log.Warnf("Failed to decode signed peer record for %s; using listen addrs: %v", evt.Peer, err)
		return dedupeMultiaddrs(evt.ListenAddrs)
	}

	return dedupeMultiaddrs(addrs)
}

func signedPeerRecordAddrs(env *record.Envelope, peerID peer.ID) ([]ma.Multiaddr, error) {
	rec, err := env.Record()
	if err != nil {
		return nil, err
	}
	peerRec, ok := rec.(*peer.PeerRecord)
	if !ok {
		return nil, fmt.Errorf("signed peer record is not a peer record")
	}
	if peerRec.PeerID != peerID {
		return nil, fmt.Errorf("signed peer record peer ID does not match event peer ID")
	}
	return peerRec.Addrs, nil
}

func filterPersistentPeerAddrs(addrs []ma.Multiaddr, localDevMode bool) []ma.Multiaddr {
	if localDevMode {
		return addrs
	}
	return ma.FilterAddrs(addrs, manet.IsPublicAddr)
}

func dedupeMultiaddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(addrs))
	deduped := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		key := addr.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, addr)
	}
	return deduped
}
