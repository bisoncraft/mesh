package tatanka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// connectCheckInterval is the interval between attempts to connect to peers
	// wo are not connected to.
	connectCheckInterval = 1 * time.Minute

	// discoveryInterval is the interval between attempts to discover new peers
	// from bootstrap peers.
	discoveryInterval = 5 * time.Minute

	// connectToPeerTimeout is the timeout for attempting to connect to a peer.
	connectToPeerTimeout = 10 * time.Second

	// discoverPeersTimeout is the timeout for attempting to discover new peers
	// from a bootstrap peer.
	discoverPeersTimeout = 20 * time.Second

	// privateKeyFileName is the name of the file that contains the private key
	// for the tatanka node.
	privateKeyFileName = "p.key"

	// forwardRelayProtocol is the protocol used to forward a relay message between two tatanka nodes.
	forwardRelayProtocol = "/tatanka/forward-relay/1.0.0"
)

// Config is the configuration for the tatanka node
type Config struct {
	DataDir      string
	Logger       slog.Logger
	ListenIP     string
	ListenPort   int
	MetricsPort  int
	ManifestPath string
	ManifestURL  string
}

// Option is a functional option for configuring TatankaNode.
type Option func(*TatankaNode)

// WithHost sets a pre-created libp2p host (e.g., for testing with mocks).
func WithHost(h host.Host) Option {
	return func(n *TatankaNode) {
		n.node = h
	}
}

// TatankaNode is a permissioned node in the tatanka mesh
type TatankaNode struct {
	config       *Config
	node         host.Host
	log          slog.Logger
	manifest     *manifest
	privateKey   crypto.PrivKey
	bondVerifier *bondVerifier
	bondStorage  bondStorage

	gossipSub               *gossipSub
	clientConnectionManager *clientConnectionManager
	subscriptionManager     *subscriptionManager
	pushStreamManager       *pushStreamManager

	metricsServer *http.Server
}

// NewTatankaNode creates a new TatankaNode with the given configuration and options.
func NewTatankaNode(config *Config, opts ...Option) (*TatankaNode, error) {
	privateKey, err := getOrCreatePrivateKey(filepath.Join(config.DataDir, privateKeyFileName))
	if err != nil {
		return nil, err
	}

	manifest, err := loadManifest(config.ManifestPath, config.ManifestURL)
	if err != nil {
		return nil, err
	}

	t := &TatankaNode{
		config:                  config,
		log:                     config.Logger,
		manifest:                manifest,
		privateKey:              privateKey,
		clientConnectionManager: newClientConnectionManager(config.Logger),
		subscriptionManager:     newSubscriptionManager(),
		bondVerifier:            newBondVerifier(),
		bondStorage:             newMemoryBondStorage(time.Now),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *TatankaNode) getManifestPeers() map[peer.ID]struct{} {
	return t.manifest.allPeerIDs()
}

func (t *TatankaNode) handleBroadcastMessage(msg *protocolsPb.PushMessage) {
	clients := t.subscriptionManager.clientsForTopic(msg.Topic)
	if len(clients) > 0 {
		t.pushStreamManager.distribute(clients, msg)
	}
}

func (t *TatankaNode) handleClientConnectionMessage(update *clientConnectionUpdate) {
	t.clientConnectionManager.updateClientConnectionInfo(update)
}

// Run starts the tatanka node and blocks until the context is done.
func (t *TatankaNode) Run(ctx context.Context) error {
	wg := sync.WaitGroup{}

	// Setup libp2p node if not provided in options.
	if t.node == nil {
		// TODO: which protocols to use??
		listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", t.config.ListenIP, t.config.ListenPort)
		var err error
		t.node, err = libp2p.New(
			libp2p.Identity(t.privateKey),
			libp2p.ListenAddrStrings(listenAddr),
			// EnableRelayService for p2p communication between clients
			libp2p.EnableRelayService(),
		)
		if err != nil {
			return err
		}
	}

	t.log.Infof("Node ID: %s", t.node.ID().String())

	listenAddrs := t.node.Network().ListenAddresses()
	t.log.Infof("Listening on: ")
	for _, addr := range listenAddrs {
		t.log.Infof("  %s", addr.String())
	}

	var err error
	t.gossipSub, err = newGossipSub(ctx, &gossipSubCfg{
		node:                          t.node,
		log:                           t.config.Logger,
		getManifestPeers:              t.getManifestPeers,
		handleBroadcastMessage:        t.handleBroadcastMessage,
		handleClientConnectionMessage: t.handleClientConnectionMessage,
	})
	if err != nil {
		return err
	}

	t.pushStreamManager = newPushStreamManager(t.config.Logger, func(client peer.ID, timestamp time.Time, connected bool) {
		err := t.gossipSub.publishClientConnectionMessage(ctx, &clientConnectionUpdate{
			clientID:   client,
			reporterID: t.node.ID(),
			timestamp:  timestamp.UnixMilli(),
			connected:  connected,
		})
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			t.log.Errorf("Publishing client connection message failed: %v", err)
		}
	})

	t.setupStreamHandlers()
	t.setupObservability()

	go func() {
		t.log.Infof("Metrics available on :%d/metrics", t.config.MetricsPort)
		t.log.Infof("Profiler available on :%d/debug/pprof", t.config.MetricsPort)
		if err := t.metricsServer.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				t.log.Errorf("Failed to start metrics server: %v", err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := t.gossipSub.run(ctx); err != nil {
			t.log.Errorf("Gossip sub failed: %v", err)
		} else {
			t.log.Infof("Gossip sub stopped.")
		}
	}()

	// Maintain mesh connectivity
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.maintainMeshConnectivity(ctx)
	}()

	wg.Wait()

	t.log.Infof("Shutting down tatanka node...")
	err = t.metricsServer.Shutdown(ctx)
	if err != nil {
		return err
	}

	err = t.node.Close()
	if err != nil {
		return err
	}

	t.log.Infof("Tatanka node shutdown complete.")

	return nil
}

func getOrCreatePrivateKey(filePath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
			if err != nil {
				return nil, err
			}
			bytes, err := crypto.MarshalPrivateKey(priv)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(filePath, bytes, 0600); err != nil {
				return nil, err
			}
			return priv, nil
		}
		return nil, err
	}
	priv, err := crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, err
	}

	// TODO: zero data bytes, encrypt priv key on disk

	return priv, nil
}

func (t *TatankaNode) setupStreamHandlers() {
	t.setStreamHandler(protocols.PostBondsProtocol, t.handlePostBonds, requireNoPermission)
	t.setStreamHandler(protocols.DiscoveryProtocol, t.handleDiscovery, requireAny(t.isManifestPeer, t.requireBonds))
	t.setStreamHandler(forwardRelayProtocol, t.handleForwardRelay, t.isManifestPeer)
	t.setStreamHandler(protocols.ClientSubscribeProtocol, t.handleClientSubscribe, t.requireBonds)
	t.setStreamHandler(protocols.ClientPublishProtocol, t.handleClientPublish, t.requireBonds)
	t.setStreamHandler(protocols.ClientPushProtocol, t.handleClientPush, t.requireBonds)
	t.setStreamHandler(protocols.ClientRelayMessageProtocol, t.handleClientRelayMessage, t.requireBonds)
}

func (t *TatankaNode) setupObservability() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	t.metricsServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", t.config.MetricsPort),
		Handler: mux,
	}
}

// discoverPeers queries the given peer for the list of other tatanka nodes that
// it is connected to.
func (t *TatankaNode) discoverPeers(ctx context.Context, peerToQuery *peer.AddrInfo) ([]peer.AddrInfo, error) {
	t.log.Tracef("Discovering peers from %s", peerToQuery.ID)

	s, err := t.node.NewStream(ctx, peerToQuery.ID, protocols.DiscoveryProtocol)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()

	response := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(s, response); err != nil {
		return nil, err
	}

	// Handle response types
	discoveryResponse := response.GetDiscoveryResponse()
	if discoveryResponse != nil {
		// Success - process the discovery response
		discoveredPeers := make([]peer.AddrInfo, 0, len(discoveryResponse.Peers))
		for _, pbPeer := range discoveryResponse.Peers {
			peerInfo, err := pbPeerInfoToLibp2p(pbPeer)
			if err != nil {
				t.log.Errorf("Failed to parse peer info: %v", err)
				continue
			}

			discoveredPeers = append(discoveredPeers, peerInfo)
		}

		t.log.Tracef("Discovered %d peers from %s", len(discoveredPeers), peerToQuery.ID)
		return discoveredPeers, nil
	}

	// Handle error responses
	if errResp := response.GetError(); errResp != nil {
		if errResp.GetUnauthorized() != nil {
			return nil, fmt.Errorf("peer returned unauthorized error")
		}
		if msg := errResp.GetMessage(); msg != "" {
			return nil, fmt.Errorf("peer returned error: %s", msg)
		}
		return nil, fmt.Errorf("peer returned error response")
	}

	// Unknown response type
	return nil, fmt.Errorf("unexpected response type for discovery protocol: %T", response.Response)
}

func (t *TatankaNode) refreshPeersFromBootstrap(ctx context.Context) {
	var discoveredPeers []peer.AddrInfo
	var wg sync.WaitGroup
	var discoveredLock sync.Mutex

	for _, bootPeer := range t.manifest.bootstrapPeers {
		if bootPeer.ID == t.node.ID() {
			continue
		}

		wg.Add(1)
		go func(bootInfo *peer.AddrInfo) {
			defer wg.Done()

			// Connect to bootstrap peer if not already connected
			if t.node.Network().Connectedness(bootInfo.ID) != network.Connected {
				t.log.Infof("Connecting to bootstrap peer %s", bootInfo.ID)

				ctxConnect, cancel := context.WithTimeout(ctx, connectToPeerTimeout)
				if err := t.node.Connect(ctxConnect, *bootInfo); err != nil {
					t.log.Infof("Failed to connect to bootstrap peer %s: %v", bootInfo.ID, err)
					cancel()
					return
				}

				t.log.Infof("Successfully connected to bootstrap peer %s", bootInfo.ID)
				cancel()
			}

			// Perform discovery
			t.log.Debugf("Discovering peers from bootstrap node %s", bootInfo.ID)

			ctxDiscover, cancel := context.WithTimeout(ctx, discoverPeersTimeout)
			peers, err := t.discoverPeers(ctxDiscover, bootInfo)
			cancel()
			if err != nil {
				t.log.Errorf("Discovery from %s failed: %v", bootInfo.ID, err)
				return
			}

			// Merge results (thread-safe)
			discoveredLock.Lock()
			discoveredPeers = append(discoveredPeers, peers...)
			discoveredLock.Unlock()
		}(bootPeer)
	}

	wg.Wait()

	peerMap := make(map[peer.ID]map[string]ma.Multiaddr)
	for _, pInfo := range discoveredPeers {
		if peerMap[pInfo.ID] == nil {
			peerMap[pInfo.ID] = map[string]ma.Multiaddr{}
		}

		for _, addr := range pInfo.Addrs {
			peerMap[pInfo.ID][addr.String()] = addr
		}
	}

	for p, peerAddrs := range peerMap {
		if p == t.node.ID() {
			continue
		}

		addrs := make([]ma.Multiaddr, 0, len(peerAddrs))
		for _, addr := range peerAddrs {
			addrs = append(addrs, addr)
		}

		t.node.Peerstore().AddAddrs(p, addrs, discoveryInterval+time.Minute)
	}
}

func (t *TatankaNode) connectToPeers(ctx context.Context) {
	var wg sync.WaitGroup

	for peerID := range t.manifest.allPeerIDs() {
		if peerID == t.node.ID() || t.node.Network().Connectedness(peerID) == network.Connected {
			continue
		}

		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()

			ctxConnect, cancel := context.WithTimeout(ctx, connectToPeerTimeout)
			defer cancel()
			if err := t.node.Connect(ctxConnect, peer.AddrInfo{ID: peerID}); err != nil {
				t.log.Tracef("Failed to connect to %s: %v", peerID, err)
			} else {
				t.log.Debugf("Successfully connected to mesh peer %s", peerID)
			}
		}(peerID)
	}

	wg.Wait()
}

func (t *TatankaNode) maintainMeshConnectivity(ctx context.Context) {
	connectTicker := time.NewTicker(connectCheckInterval)
	discoverTicker := time.NewTicker(discoveryInterval)
	defer connectTicker.Stop()
	defer discoverTicker.Stop()

	t.refreshPeersFromBootstrap(ctx)
	t.connectToPeers(ctx)

	for {
		select {
		case <-connectTicker.C:
			t.log.Tracef("Performing periodic connectivity check...")
			t.connectToPeers(ctx)
		case <-discoverTicker.C:
			t.log.Tracef("Performing periodic peer discovery...")
			t.refreshPeersFromBootstrap(ctx)
		case <-ctx.Done():
			t.log.Debugf("Stopping connectivity maintenance.")
			return
		}
	}
}
