package tatanka

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/martonp/tatanka-mesh/codec"
	pb "github.com/martonp/tatanka-mesh/tatanka/pb"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// connectToPeerTimeout is the timeout for attempting to connect to a peer.
	connectToPeerTimeout = 10 * time.Second
	// baseRetryDelay is the initial backoff duration after a failed connection.
	baseRetryDelay = 2 * time.Second
	// maxRetryDelay is the maximum backoff duration.
	maxRetryDelay = time.Minute
	// discoveryAttemptFrequency controls how often we attempt peer discovery.
	// Discovery is attempted every N consecutive dial failures.
	discoveryAttemptFrequency = 5
)

var (
	errWhitelistMismatch = errors.New("whitelist mismatch")
	errDiscoveryNotFound = errors.New("discovery not found")
)

// AdminUpdateCallback is called when connection states change
type AdminUpdateCallback func(peerID peer.ID, connected bool, whitelistMismatch bool, addresses []string, peerWhitelist []string)

// retryState tracks exponential backoff state for connection retries.
type retryState struct {
	failures int
	backoff  time.Duration
}

// onSuccess resets the retry state after a successful connection.
func (r *retryState) onSuccess() {
	r.failures = 0
	r.backoff = baseRetryDelay
}

// onFailure increments the failure count and doubles the backoff (up to max).
func (r *retryState) onFailure() {
	r.failures++
	r.backoff *= 2
	if r.backoff > maxRetryDelay {
		r.backoff = maxRetryDelay
	}
}

// peerTracker manages the connection lifecycle of a single peer.
type peerTracker struct {
	peerID peer.ID
	m      *meshConnectionManager
	ctx    context.Context
	cancel context.CancelFunc
	// signalReconnect is used to signal the tracker to attempt to reconnect
	// immediately.
	signalReconnect chan struct{}
	// whitelistMismatch is set when the most recent connection attempt failed
	// specifically due to whitelist mismatch. If this is the case, we know
	// there's no reason to try discovery.
	whitelistMismatch bool
	// peerWhitelist contains the peer's whitelist if we received it during
	// whitelist verification.
	peerWhitelist []string
	// initialCh is closed after the first connection attempt.
	initialCh   chan struct{}
	initialOnce sync.Once
}

func (t *peerTracker) markInitial() {
	t.initialOnce.Do(func() {
		close(t.initialCh)
	})
}

// getAddresses returns the known addresses for this peer as strings.
func (t *peerTracker) getAddresses() []string {
	addrs := t.m.node.Peerstore().Addrs(t.peerID)
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}

// run starts the main event loop for a single peer connection.
func (t *peerTracker) run() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	retry := &retryState{backoff: baseRetryDelay}

	resetTimerWithJitter := func(duration time.Duration) {
		jitter := time.Duration(rand.Intn(int(duration / 10)))
		timer.Reset(duration + jitter)
	}

	success := func() {
		t.m.adminCallback(t.peerID, true, false, t.getAddresses(), nil)
		t.markInitial()
		retry.onSuccess()
		resetTimerWithJitter(maxRetryDelay)
	}

	forceReconnect := func() {
		retry.onSuccess()
		timer.Reset(0)
	}

	failure := func() {
		t.m.adminCallback(t.peerID, false, t.whitelistMismatch, t.getAddresses(), t.peerWhitelist)
		t.markInitial()
		resetTimerWithJitter(retry.backoff)
		retry.onFailure()
	}

	// shouldAttemptDiscovery determines if we should query other peers for
	// addresses of our target peer on this pass.
	//
	// - If whitelist mismatch occurred, discovery won't help (peer has wrong config).
	// - If we have addresses: try them first, only discover every N failures
	//   (addresses might just be temporarily unreachable).
	// - If we have no addresses: discover immediately on first attempt (pass 0),
	//   then every N failures.
	shouldAttemptDiscovery := func() bool {
		if t.whitelistMismatch {
			return false
		}

		hasAddrs := len(t.m.node.Peerstore().Addrs(t.peerID)) > 0

		if hasAddrs {
			return retry.failures != 0 && retry.failures%discoveryAttemptFrequency == 0
		}

		return retry.failures%discoveryAttemptFrequency == 0
	}

	for {
		select {
		case <-t.ctx.Done():
			return

		case <-t.signalReconnect:
			// Do not force a reconnect if we just disconnected due to a whitelist mismatch.
			if t.whitelistMismatch {
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			forceReconnect()
		case <-timer.C:
			if t.m.node.Network().Connectedness(t.peerID) == network.Connected {
				success()
				continue
			}

			if shouldAttemptDiscovery() {
				if !t.discoverAddresses() {
					failure()
					continue
				}
			}

			if len(t.m.node.Peerstore().Addrs(t.peerID)) == 0 {
				failure()
				continue
			}

			err := t.connect()
			if err == nil {
				t.m.log.Debugf("Connected to peer %s", t.peerID)
				success()
			} else {
				failure()
				t.m.log.Warnf("Failed to connect to peer %s (Attempt %d): %v", t.peerID, retry.failures, err)
			}
		}
	}
}

// connect dials the peer and performs the whitelist handshake.
func (t *peerTracker) connect() error {
	dialCtx, dialCancel := context.WithTimeout(t.ctx, connectToPeerTimeout)
	defer dialCancel()

	t.whitelistMismatch = false

	if err := t.m.node.Connect(dialCtx, peer.AddrInfo{ID: t.peerID}); err != nil {
		return err
	}

	err := t.verifyWhitelist()
	if err != nil {
		if errors.Is(err, errWhitelistMismatch) {
			t.whitelistMismatch = true
		}
		return fmt.Errorf("failed to verify whitelist for peer %s: %w", t.peerID, err)
	}

	return nil
}

// discoverAddresses asks connected whitelist peers for the address of the target.
func (t *peerTracker) discoverAddresses() bool {
	whitelist := t.m.getWhitelist()

	var wg sync.WaitGroup
	var success atomic.Bool

	for _, p := range whitelist.peers {
		if p.ID == t.m.node.ID() || p.ID == t.peerID {
			continue
		}
		if t.m.node.Network().Connectedness(p.ID) != network.Connected {
			continue
		}

		wg.Add(1)
		go func(helper peer.ID) {
			defer wg.Done()
			addrs, err := t.sendDiscoveryRequest(helper, t.peerID)
			if err == nil && len(addrs) > 0 {
				t.m.log.Debugf("Discovered address for %s via %s", t.peerID, helper)
				t.m.node.Peerstore().AddAddrs(t.peerID, addrs, peerstore.PermanentAddrTTL)
				success.Store(true)
			} else if !errors.Is(err, errDiscoveryNotFound) {
				t.m.log.Warnf("Failed to discover addresses for %s via %s: %v", t.peerID, helper, err)
			}
		}(p.ID)
	}

	wg.Wait()

	return success.Load()
}

// verifyWhitelist sends a whitelist request to the peer and verifies the response.
// If the response is a success, we protect the connection and return nil. Otherwise,
// we close the connection and return an error.
func (t *peerTracker) verifyWhitelist() error {
	var success bool
	defer func() {
		if success {
			t.m.node.ConnManager().Protect(t.peerID, "tatanka-node")
		} else {
			_ = t.m.node.Network().ClosePeer(t.peerID)
		}
	}()

	stream, err := t.m.node.NewStream(t.ctx, t.peerID, whitelistProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	req := &pb.WhitelistRequest{PeerIDs: t.m.getWhitelist().peerIDsBytes()}
	if err := codec.WriteLengthPrefixedMessage(stream, req); err != nil {
		return err
	}

	resp := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		return err
	}

	if resp.GetSuccess() != nil {
		success = true
		return nil
	}

	if resp.GetMismatch() != nil {
		// Extract peer's whitelist from mismatch response
		peerIDs := resp.GetMismatch().GetPeerIDs()
		t.peerWhitelist = make([]string, len(peerIDs))
		for i, peerIDBytes := range peerIDs {
			peerID, err := peer.IDFromBytes(peerIDBytes)
			if err != nil {
				t.m.log.Warnf("Failed to parse peer ID from mismatch response: %v", err)
				continue
			}
			t.peerWhitelist[i] = peerID.String()
		}
	}

	return fmt.Errorf("%w for peer %s", errWhitelistMismatch, t.peerID)
}

// meshConnectionManager manages the connections to the whitelist peers.
type meshConnectionManager struct {
	log       slog.Logger
	node      host.Host
	ctx       context.Context
	whitelist atomic.Value // *whitelist

	trackersMtx  sync.RWMutex
	peerTrackers map[peer.ID]*peerTracker

	initialCh     chan struct{}
	initialOnce   sync.Once
	initialErr    atomic.Value // error
	adminCallback AdminUpdateCallback
}

func newMeshConnectionManager(log slog.Logger, node host.Host, whitelist *whitelist, adminCallback AdminUpdateCallback) *meshConnectionManager {
	m := &meshConnectionManager{
		log:           log,
		node:          node,
		peerTrackers:  make(map[peer.ID]*peerTracker),
		initialCh:     make(chan struct{}),
		adminCallback: adminCallback,
	}
	m.whitelist.Store(whitelist)

	// Add all bootstrap addresses to the peerstore.
	for _, whitelistPeer := range whitelist.peers {
		if whitelistPeer.ID == m.node.ID() || len(whitelistPeer.Addrs) == 0 {
			continue
		}
		m.node.Peerstore().AddAddrs(whitelistPeer.ID, whitelistPeer.Addrs, peerstore.PermanentAddrTTL)
	}

	// Register for network events to react instantly to disconnects.
	node.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			m.triggerReconnect(conn.RemotePeer())
		},
	})

	return m
}

func (m *meshConnectionManager) getWhitelist() *whitelist {
	return m.whitelist.Load().(*whitelist)
}

// triggerReconnect signals the specific peer tracker to wake up and retry immediately.
func (m *meshConnectionManager) triggerReconnect(pid peer.ID) {
	m.trackersMtx.RLock()
	defer m.trackersMtx.RUnlock()
	if t, ok := m.peerTrackers[pid]; ok {
		select {
		case t.signalReconnect <- struct{}{}:
		default:
		}
	}
}

// triggerReconnectAll triggers a reconnect for all trackers.
func (m *meshConnectionManager) triggerReconnectAll() {
	m.trackersMtx.RLock()
	defer m.trackersMtx.RUnlock()
	for _, t := range m.peerTrackers {
		select {
		case t.signalReconnect <- struct{}{}:
		default:
		}
	}
}

// startTracker initializes and runs a new tracker loop for a peer.
func (m *meshConnectionManager) startTracker(ctx context.Context, pid peer.ID) *peerTracker {
	m.trackersMtx.Lock()
	defer m.trackersMtx.Unlock()

	if tracker, exists := m.peerTrackers[pid]; exists {
		return tracker
	}

	ctx, cancel := context.WithCancel(ctx)
	t := &peerTracker{
		peerID:          pid,
		m:               m,
		ctx:             ctx,
		cancel:          cancel,
		signalReconnect: make(chan struct{}, 1),
		initialCh:       make(chan struct{}),
		initialOnce:     sync.Once{},
	}
	m.peerTrackers[pid] = t
	go t.run()

	return t
}

// stopTracker cancels and removes a tracker (used when whitelist changes).
func (m *meshConnectionManager) stopTracker(pid peer.ID) {
	m.trackersMtx.Lock()
	defer m.trackersMtx.Unlock()

	if t, exists := m.peerTrackers[pid]; exists {
		t.cancel()
		// If we stop tracking a peer (e.g. whitelist update), make sure the connection
		// isn't artificially kept alive.
		m.node.ConnManager().Unprotect(pid, "tatanka-node")
		delete(m.peerTrackers, pid)
	}
}

// sendDiscoveryRequest sends a discovery request to reqPeerID for the addresses of targetPeerID.
func (t *peerTracker) sendDiscoveryRequest(reqPeerID, targetPeerID peer.ID) ([]ma.Multiaddr, error) {
	s, err := t.m.node.NewStream(t.ctx, reqPeerID, discoveryProtocol)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()

	request := &pb.DiscoveryRequest{Id: []byte(targetPeerID)}
	if err := codec.WriteLengthPrefixedMessage(s, request); err != nil {
		return nil, err
	}

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(s, response); err != nil {
		return nil, err
	}

	if response.GetSuccess() != nil {
		addrs := make([]ma.Multiaddr, 0, len(response.GetSuccess().GetAddrs()))
		for _, addr := range response.GetSuccess().GetAddrs() {
			addr, err := ma.NewMultiaddrBytes(addr)
			if err != nil {
				return nil, err
			}
			addrs = append(addrs, addr)
		}
		return addrs, nil
	}

	return nil, errDiscoveryNotFound
}

func (m *meshConnectionManager) updateWhitelist(whitelist *whitelist) {
	m.whitelist.Store(whitelist)
	// TODO: Stop trackers for peers removed from whitelist and
	// start trackers for new peers.
}

// waitInitial blocks until the initial connectivity pass is marked complete.
func (m *meshConnectionManager) waitInitial(ctx context.Context) {
	select {
	case <-m.initialCh:
	case <-ctx.Done():
	}
}

// markInitial marks the initial connectivity pass as complete.
func (m *meshConnectionManager) markInitial() {
	m.initialOnce.Do(func() {
		close(m.initialCh)
	})
}

// run starts the connection manager.
func (m *meshConnectionManager) run(ctx context.Context) {
	m.ctx = ctx

	whitelist := m.getWhitelist()
	allTrackers := make(map[peer.ID]*peerTracker)

	waitForAllTrackers := func() {
		for _, t := range allTrackers {
			select {
			case <-t.initialCh:
			case <-ctx.Done():
				return
			}
		}
	}

	// PASS 1: Start trackers for peers with addresses. Wait for
	// them all to finish their initial connection loop before
	// starting trackers for peers without addresses.
	for _, p := range whitelist.peers {
		if p.ID == m.node.ID() {
			continue
		}
		hasAddrs := len(m.node.Peerstore().Addrs(p.ID)) > 0
		if hasAddrs {
			allTrackers[p.ID] = m.startTracker(ctx, p.ID)
		}
	}

	waitForAllTrackers()

	// PASS 2: Start peers WITHOUT addresses as well.
	for _, p := range whitelist.peers {
		if p.ID == m.node.ID() {
			continue
		}
		// no-op if already started.
		t := m.startTracker(ctx, p.ID)
		allTrackers[p.ID] = t
	}

	waitForAllTrackers()

	m.markInitial()

	recoveryCh := notifyOnConnectivityRecovery(ctx)
	for {
		select {
		case <-ctx.Done():
			// Cleanup
			m.trackersMtx.Lock()
			for _, t := range m.peerTrackers {
				t.cancel()
			}
			m.trackersMtx.Unlock()
			return
		case <-recoveryCh:
			m.log.Infof("Internet connectivity recovered...")
			m.triggerReconnectAll()
		}
	}
}

// notifyOnConnectivityRecovery returns a channel that signals when the system
// transitions from "Offline" to "Online" using a TCP handshake check.
func notifyOnConnectivityRecovery(ctx context.Context) <-chan struct{} {
	notifyCh := make(chan struct{}, 1)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		wasOnline := checkConnectivity()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				isOnline := checkConnectivity()
				if !wasOnline && isOnline {
					select {
					case notifyCh <- struct{}{}:
					default:
					}
				}
				wasOnline = isOnline
			}
		}
	}()

	return notifyCh
}

// checkConnectivity attempts to open a TCP connection to a public DNS.
// We use TCP to ensure a full round-trip handshake occurs.
func checkConnectivity() bool {
	// Try Cloudflare
	if isReachable("1.1.1.1:53") {
		return true
	}
	// Fallback to Google
	if isReachable("8.8.8.8:53") {
		return true
	}
	return false
}

func isReachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		conn.Close()
		return true
	}
	return false
}
