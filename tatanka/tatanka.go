package tatanka

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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
	pb "github.com/martonp/tatanka-mesh/tatanka/pb"
	ma "github.com/multiformats/go-multiaddr"
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
)

// Config is the configuration for the tatanka node
type Config struct {
	DataDir      string
	Logger       slog.Logger
	ListenIP     string
	ListenPort   int
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
	config     *Config
	node       host.Host
	log        slog.Logger
	manifest   *manifest
	privateKey crypto.PrivKey

	gossipSub               *gossipSub
	clientConnectionManager *clientConnectionManager
	subscriptionManager     *subscriptionManager
	pushStreamManager       *pushStreamManager
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
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *TatankaNode) getManifestPeers() map[peer.ID]struct{} {
	return t.manifest.allPeerIDs()
}

func (t *TatankaNode) handleBroadcastMessage(msg *protocolsPb.ClientPushMessage) {
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
		var addrs []ma.Multiaddr
		if connected {
			addrs = t.node.Peerstore().Addrs(client)
		}

		err := t.gossipSub.publishClientConnectionMessage(ctx, &clientConnectionUpdate{
			clientID:   client,
			reporterID: t.node.ID(),
			addrs:      addrs,
			timestamp:  timestamp.UnixMilli(),
			connected:  connected,
		})
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			t.log.Errorf("Publishing client connection message failed: %v", err)
		}
	})

	t.setupStreamHandlers()

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
	t.node.SetStreamHandler(protocols.DiscoveryProtocol, t.handleDiscovery)
	t.node.SetStreamHandler(protocols.ClientSubscribeProtocol, t.handleClientSubscribe)
	t.node.SetStreamHandler(protocols.ClientPublishProtocol, t.handleClientPublish)
	t.node.SetStreamHandler(protocols.ClientPushProtocol, t.handleClientPush)
	t.node.SetStreamHandler(protocols.ClientAddrProtocol, t.handleClientAddr)
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

	if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
		return nil, err
	}

	buf := bufio.NewReader(s)

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(buf, response); err != nil {
		return nil, err
	}

	discoveredPeers := make([]peer.AddrInfo, 0, len(response.Peers))
	for _, pbPeer := range response.Peers {
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

	peerMap := make(map[peer.ID][]ma.Multiaddr)
	for _, pInfo := range discoveredPeers {
		// TODO: this will cause duplicates..
		peerMap[pInfo.ID] = append(peerMap[pInfo.ID], pInfo.Addrs...)
	}

	for p, addrs := range peerMap {
		if p == t.node.ID() {
			continue
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
