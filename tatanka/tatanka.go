package tatanka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/tatanka/admin"
	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// privateKeyFileName is the name of the file that contains the private key
	// for the tatanka node.
	privateKeyFileName = "p.key"

	// forwardRelayProtocol is the protocol used to forward a relay message between two tatanka nodes.
	forwardRelayProtocol = "/tatanka/forward-relay/1.0.0"

	// discoveryProtocol is the protocol used to query a tatanka node for the addresses of
	// a given peer.
	discoveryProtocol = "/tatanka/discovery/1.0.0"

	// whitelistProtocol is the protocol used to verify the whitelist alignment of a tatanka node.
	whitelistProtocol = "/tatanka/whitelist/1.0.0"

	// clientInfractionsSnapshotProtocol is the protocol used to query a tatanka
	// node for a snapshot of active client infractions.
	clientInfractionsSnapshotProtocol = "/tatanka/client-infractions-snapshot/1.0.0"
)

// Config is the configuration for the tatanka node
type Config struct {
	DataDir       string
	Logger        slog.Logger
	ListenIP      string
	ListenPort    int
	MetricsPort   int
	AdminPort     int
	WhitelistPath string

	// Oracle Configuration
	CMCKey        string
	TatumKey      string
	CryptoApisKey string
}

// Option is a functional option for configuring TatankaNode.
type Option func(*TatankaNode)

// WithHost sets a pre-created libp2p host (e.g., for testing with mocks).
func WithHost(h host.Host) Option {
	return func(n *TatankaNode) {
		n.node = h
	}
}

// Oracle defines the requirements for implementing an oracle.
type Oracle interface {
	Run(ctx context.Context)
	MergePrices(sourcedUpdate *oracle.SourcedPriceUpdate) map[oracle.Ticker]float64
	MergeFeeRates(sourcedUpdate *oracle.SourcedFeeRateUpdate) map[oracle.Network]*big.Int
	Prices() map[oracle.Ticker]float64
	FeeRates() map[oracle.Network]*big.Int
	GetSourceWeight(sourceName string) float64
}

// TatankaNode is a permissioned node in the tatanka mesh
type TatankaNode struct {
	config               *Config
	node                 host.Host
	log                  slog.Logger
	whitelist            atomic.Value // *whitelist
	readyCh              chan struct{}
	readyOnce            sync.Once
	readyErr             atomic.Value // error
	privateKey           crypto.PrivKey
	bondVerifier         *bondVerifier
	bondStorage          bondStorage
	banManager           *banManager
	broadcastRateLimiter *broadcastRateLimiter

	gossipSub               *gossipSub
	clientConnectionManager *clientConnectionManager
	subscriptionManager     *subscriptionManager
	pushStreamManager       *pushStreamManager
	connectionManager       *meshConnectionManager
	adminServer             *admin.Server

	metricsServer *http.Server

	oracle Oracle
}

// NewTatankaNode creates a new TatankaNode with the given configuration and options.
func NewTatankaNode(config *Config, opts ...Option) (*TatankaNode, error) {
	privateKey, err := getOrCreatePrivateKey(filepath.Join(config.DataDir, privateKeyFileName))
	if err != nil {
		return nil, err
	}

	whitelist, err := loadWhitelist(config.WhitelistPath)
	if err != nil {
		return nil, err
	}

	t := &TatankaNode{
		config:                  config,
		log:                     config.Logger,
		privateKey:              privateKey,
		clientConnectionManager: newClientConnectionManager(config.Logger),
		subscriptionManager:     newSubscriptionManager(),
		bondVerifier:            newBondVerifier(),
		bondStorage:             newMemoryBondStorage(time.Now),
		readyCh:                 make(chan struct{}),
	}
	t.whitelist.Store(whitelist)

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *TatankaNode) getWhitelist() *whitelist {
	return t.whitelist.Load().(*whitelist)
}

func (t *TatankaNode) getWhitelistPeers() map[peer.ID]struct{} {
	return t.getWhitelist().allPeerIDs()
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

func (t *TatankaNode) handleClientInfractionMessage(msg *pb.ClientInfractionMsg) {
	err := t.banManager.recordRemoteInfraction(msg)
	if err != nil {
		t.log.Errorf("Failed to record remote infraction: %v", err)
		return
	}
}

// Run starts the tatanka node and blocks until the context is done.
func (t *TatankaNode) Run(ctx context.Context) error {
	wg := sync.WaitGroup{}

	// Setup libp2p node if not provided in options.
	if t.node == nil {
		listenAddrs := []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", t.config.ListenPort),
			fmt.Sprintf("/ip6/::/tcp/%d", t.config.ListenPort),
		}
		var err error
		t.node, err = libp2p.New(
			libp2p.Identity(t.privateKey),
			libp2p.ListenAddrStrings(listenAddrs...),
			// EnableRelayService for p2p communication between clients
			libp2p.EnableRelayService(),
		)
		if err != nil {
			t.markReady(err)
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
		getWhitelistPeers:             t.getWhitelistPeers,
		handleBroadcastMessage:        t.handleBroadcastMessage,
		handleClientConnectionMessage: t.handleClientConnectionMessage,
		handleOracleUpdate:            t.handleOracleUpdate,
		handleClientInfractionMessage: t.handleClientInfractionMessage,
	})
	if err != nil {
		t.markReady(err)
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

	// Only create oracle if not provided (e.g., via test setup)
	if t.oracle == nil {
		t.oracle, err = oracle.New(&oracle.Config{
			Log:           t.config.Logger,
			CMCKey:        t.config.CMCKey,
			TatumKey:      t.config.TatumKey,
			CryptoApisKey: t.config.CryptoApisKey,
			PublishUpdate: t.gossipSub.publishOracleUpdate,
		})
		if err != nil {
			return fmt.Errorf("failed to create oracle: %v", err)
		}
	}

	t.banManager = newBanManager(&banManagerConfig{
		disconnectClient:  t.pushStreamManager.disconnectClientByIP,
		nodeID:            t.node.ID(),
		publishInfraction: t.gossipSub.publishClientInfractionMessage,
		now:               time.Now,
		log:               t.config.Logger,
	})

	t.broadcastRateLimiter = newBroadcastRateLimiter(&rateLimitConfig{
		recordInfraction: t.banManager.recordInfraction,
		now:              time.Now,
		log:              t.config.Logger,
	})

	// Create admin callback function and setup the admin server if configured.
	adminCallback := func(peerID peer.ID, connected bool, whitelistMismatch bool, addresses []string, peerWhitelist []string) {
	}
	if t.config.AdminPort > 0 {
		adminAddr := fmt.Sprintf(":%d", t.config.AdminPort)
		server := admin.NewServer(t.config.Logger, adminAddr)
		whitelistIDs := t.getWhitelist().allPeerIDs()
		whitelist := make([]string, 0, len(whitelistIDs))
		for id := range whitelistIDs {
			whitelist = append(whitelist, id.String())
		}
		server.UpdateWhitelist(whitelist)

		adminCallback = func(peerID peer.ID, connected, whitelistMismatch bool, addresses []string, peerWhitelist []string) {
			state := admin.StateDisconnected
			switch {
			case connected:
				state = admin.StateConnected
			case whitelistMismatch:
				state = admin.StateWhitelistMismatch
			}
			server.UpdateConnectionState(peerID, state, addresses, peerWhitelist)
		}

		t.adminServer = server
	}

	t.connectionManager = newMeshConnectionManager(t.config.Logger, t.node, t.getWhitelist(), adminCallback)

	t.log.Infof("Admin interface available (or not) on :%d", t.config.AdminPort)

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

	// Start admin server if configured
	if t.adminServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.log.Infof("Admin interface available on :%d", t.config.AdminPort)
			if err := t.adminServer.Start(ctx); err != nil {
				if !errors.Is(err, http.ErrServerClosed) {
					t.log.Errorf("Failed to start admin server: %v", err)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := t.gossipSub.run(ctx); err != nil {
			t.log.Errorf("Gossip sub failed: %v", err)
		} else {
			t.log.Infof("Gossip sub stopped.")
		}
	}()

	// Run ban manager
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.banManager.run(ctx)
	}()

	// Run broadcast rate limiter
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.broadcastRateLimiter.run(ctx)
	}()

	// Maintain mesh connectivity
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.connectionManager.run(ctx)
	}()

	// Wait for the initial connectivity pass to finish before reporting ready.
	t.connectionManager.waitInitial(ctx)

	// Query one random connected peer for their infraction snapshot
	// (all peers maintain equivalent sets via gossip)
	t.syncInfractionsFromRandomPeer(ctx)

	t.markReady(nil)

	// Run Oracle
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.oracle.Run(ctx)
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

// WaitReady blocks until Run has finished initialization or the context is done.
func (t *TatankaNode) WaitReady(ctx context.Context) error {
	select {
	case <-t.readyCh:
		if err, ok := t.readyErr.Load().(error); ok && err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fetchClientInfractionsSnapshot fetches a snapshot of active client infractions from a peer.
func (t *TatankaNode) fetchClientInfractionsSnapshot(ctx context.Context, peerID peer.ID) ([]*pb.ClientInfractionMsg, error) {
	s, err := t.node.NewStream(ctx, peerID, clientInfractionsSnapshotProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	request := &pb.ClientInfractionsSnapshotRequest{}
	if err := codec.WriteLengthPrefixedMessage(s, request); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	response := &pb.ClientInfractionsSnapshotResponse{}
	if err := codec.ReadLengthPrefixedMessage(s, response); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return response.Infractions, nil
}

// syncInfractionsFromRandomPeer queries a random connected peer for their infraction snapshot.
func (t *TatankaNode) syncInfractionsFromRandomPeer(ctx context.Context) {
	whitelist := t.getWhitelist()

	var connectedPeers []peer.ID
	for peerID := range whitelist.allPeerIDs() {
		if peerID == t.node.ID() {
			continue
		}
		if t.node.Network().Connectedness(peerID) == network.Connected {
			connectedPeers = append(connectedPeers, peerID)
		}
	}

	if len(connectedPeers) == 0 {
		t.log.Info("No connected peers to sync infractions from")
		return
	}

	rand.Shuffle(len(connectedPeers), func(i, j int) {
		connectedPeers[i], connectedPeers[j] = connectedPeers[j], connectedPeers[i]
	})

	for _, peerID := range connectedPeers {
		infractions, err := t.fetchClientInfractionsSnapshot(ctx, peerID)
		if err != nil {
			t.log.Warnf("Failed to fetch infractions from peer %s: %v", peerID.ShortString(), err)
			continue
		}

		// Deduplicate to prevent gossip re-delivery from creating duplicates
		seen := make(map[string]bool)
		var dedupedInfractions []*pb.ClientInfractionMsg
		for _, infraction := range infractions {
			infractionKey := InfractionDedupKey(infraction.Ip, infraction.Reporter, infraction.InfractionType, infraction.Expiry)
			if !seen[infractionKey] {
				seen[infractionKey] = true
				dedupedInfractions = append(dedupedInfractions, infraction)
			}
		}

		for _, infraction := range dedupedInfractions {
			if err := t.banManager.recordRemoteInfraction(infraction); err != nil {
				t.log.Errorf("Failed to record remote infraction from snapshot: %v", err)
			}
		}

		t.log.Infof("Synced %d infractions from peer %s", len(dedupedInfractions), peerID.ShortString())
		return
	}

	t.log.Warn("Failed to sync infractions from any connected peer")
}

// markReady signals that initialization is complete. If an error occurred,
// it is stored for WaitReady callers. Only the first call takes effect.
func (t *TatankaNode) markReady(err error) {
	t.readyOnce.Do(func() {
		if err != nil {
			t.readyErr.Store(err)
		}
		close(t.readyCh)
	})
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
	t.setStreamHandler(forwardRelayProtocol, t.handleForwardRelay, t.isWhitelistPeer)
	t.setStreamHandler(discoveryProtocol, t.handleDiscovery, t.isWhitelistPeer)
	t.setStreamHandler(whitelistProtocol, t.handleWhitelist, t.isWhitelistPeer)
	t.setStreamHandler(clientInfractionsSnapshotProtocol, t.handleClientInfractionsSnapshot, t.isWhitelistPeer)

	t.setStreamHandler(protocols.PostBondsProtocol, t.handlePostBonds,
		requireAll(t.requireNotBanned, requireNoPermission))
	t.setStreamHandler(protocols.ClientSubscribeProtocol, t.handleClientSubscribe,
		requireAll(t.requireNotBanned, t.requireBonds))
	t.setStreamHandler(protocols.ClientPublishProtocol, t.handleClientPublish,
		requireAll(t.requireNotBanned, t.requireBonds))
	t.setStreamHandler(protocols.ClientPushProtocol, t.handleClientPush,
		requireAll(t.requireNotBanned, t.requireBonds))
	t.setStreamHandler(protocols.ClientRelayMessageProtocol, t.handleClientRelayMessage,
		requireAll(t.requireNotBanned, t.requireBonds))
	t.setStreamHandler(protocols.AvailableMeshNodesProtocol, t.handleAvailableMeshNodes,
		requireAll(t.requireNotBanned, t.requireBonds))
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
