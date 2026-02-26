package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/bisoncraft/mesh/bond"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = 7565
)

var (
	// ErrRedundantSubscription indicates a redundant subscription has been made.
	ErrRedundantSubscription = errors.New("redundant subscription")
	// ErrRedundantUnsubscription indicates an unsubscription from a topic that is not subscribed.
	ErrRedundantUnsubscription = errors.New("redundant unsubscription")
	// errEmptyTopic indicates an empty topic name was provided.
	errEmptyTopic = errors.New("topic name cannot be empty")
	// errNoMeshConnection indicates that there is no mesh connection established.
	errNoMeshConnection = errors.New("no mesh connection established")
)

// Config represents a tatanka client configuration.
type Config struct {
	Port            int
	PrivateKey      crypto.PrivKey
	RemotePeerAddrs []string
	Logger          slog.Logger
}

// Client represents a tatanka client.
type Client struct {
	cfg           *Config
	host          host.Host
	topicRegistry *topicRegistry
	bondInfo      *bond.BondInfo
	log           slog.Logger
	connFactory   meshConnFactory
	connManager   *meshConnectionManager
}

// PeerID returns the peer ID of the client. If the client has not yet been
// initialized, an empty string is returned.
func (c *Client) PeerID() peer.ID {
	if c.host == nil {
		return ""
	}
	return c.host.ID()
}

// ConnectedTatankaNodePeerID returns the peer ID of the currently connected
// tatanka node. If no tatanka node connection is active, an empty string
// is returned.
func (c *Client) ConnectedTatankaNodePeerID() string {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return ""
	}
	return mc.remotePeerID().String()
}

// NewClient initializes a new tatanka client.
func NewClient(cfg *Config) (*Client, error) {
	c := &Client{
		cfg:           cfg,
		topicRegistry: newTopicRegistry(),
		bondInfo:      bond.NewBondInfo(),
		log:           cfg.Logger,
	}

	// Add a placeholder bond to validate client. Remove once bond pipeline is fully implemented.
	c.bondInfo.AddBonds([]*bond.BondParams{{
		ID:       "placeholder",
		Expiry:   time.Now().Add(time.Hour * 6),
		Strength: bond.MinRequiredBondStrength}},
		time.Now())

	if c.cfg.Port == 0 {
		c.cfg.Port = defaultPort
	}

	return c, nil
}

func (c *Client) primaryMeshConnection() (meshConn, error) {
	if c.connManager == nil {
		return nil, errNoMeshConnection
	}
	return c.connManager.primaryConnection()
}

// Broadcast publishes the provided message bytes on a mesh topic.
func (c *Client) Broadcast(ctx context.Context, topic string, data []byte) error {
	if topic == "" {
		return errEmptyTopic
	}

	if data == nil {
		return errors.New("data cannot be nil")
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return fmt.Errorf("failed to broadcast message: %w", err)
	}

	return mc.broadcast(ctx, topic, data)
}

// Topics returns the list of topics the client is currently subscribed to.
func (c *Client) Topics() []string {
	return c.topicRegistry.fetchTopics()
}

// Subscribe subscribes the client to the provided topic.
func (c *Client) Subscribe(ctx context.Context, topic string, handlerFunc TopicHandler) error {
	if topic == "" {
		return errEmptyTopic
	}

	if handlerFunc == nil {
		return errors.New("handler function cannot be nil")
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	if !c.topicRegistry.register(topic, handlerFunc) {
		return ErrRedundantSubscription
	}

	err = mc.subscribe(ctx, topic)
	if err != nil {
		// Unregister the topic if subscription fails.
		if !c.topicRegistry.unregister(topic) {
			c.log.Warnf("Failed to unregister topic %s", topic)
		}

		return err
	}

	return nil
}

// Unsubscribe unsubscribes the client from the provided topic.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	if topic == "" {
		return errEmptyTopic
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	// Fetch the handler for the topic before unregistering.
	topicHandler, err := c.topicRegistry.fetchHandler(topic)
	if err != nil {
		return ErrRedundantUnsubscription
	}

	if !c.topicRegistry.unregister(topic) {
		return ErrRedundantUnsubscription
	}

	err = mc.unsubscribe(ctx, topic)
	if err != nil {
		// Re-register the topic if unsubscription fails.
		if !c.topicRegistry.register(topic, topicHandler) {
			c.log.Warnf("Failed to re-register topic %s", topic)
		}

		return err
	}

	return nil
}

// handlePushMessage processes incoming pushed messages. The messages are processed based on their topics.
func (c *Client) handlePushMessage(msg *protocolsPb.PushMessage) {
	handlerFunc, err := c.topicRegistry.fetchHandler(msg.Topic)
	if err != nil {
		c.log.Warnf("Failed to fetch handler for topic %s: %v", msg.Topic, err)
		return
	}

	event := TopicEvent{
		Peer: peer.ID(msg.GetSender()),
		Type: TopicEventData,
	}

	switch msg.GetMessageType() {
	case protocolsPb.PushMessage_BROADCAST:
		event.Type = TopicEventData
		event.Data = msg.GetData()
	case protocolsPb.PushMessage_SUBSCRIBE:
		event.Type = TopicEventPeerSubscribed
	case protocolsPb.PushMessage_UNSUBSCRIBE:
		event.Type = TopicEventPeerUnsubscribed
	default:
		c.log.Warnf("Received push message with unknown type %v on topic %s", msg.GetMessageType(), msg.Topic)
		return
	}

	handlerFunc(event)
}

// PostBond posts the client's bond.
func (c *Client) PostBond(ctx context.Context) error {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}
	return mc.postBond(ctx)
}

// AddBond adds the provided bond parameters to the client.
func (c *Client) AddBond(params []*bond.BondParams) {
	c.bondInfo.AddBonds(params, time.Now())
}

// WaitForConnection blocks until the client has an active primary mesh connection
// or the context is done.
func (c *Client) WaitForConnection(ctx context.Context) error {
	if c.connManager == nil {
		return errNoMeshConnection
	}
	return c.connManager.waitForConnection(ctx)
}

// parseBootstrapAddrs parses a list of multiaddr strings into peer.AddrInfo.
// Multiple addresses for the same peer ID are combined into a single AddrInfo.
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

// Run starts the mesh client.
func (c *Client) Run(ctx context.Context, bonds []*bond.BondParams) error {
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", defaultHost, c.cfg.Port)

	if c.cfg.PrivateKey == nil {
		return fmt.Errorf("no private key provided for client")
	}

	var err error
	c.host, err = libp2p.New(libp2p.ListenAddrStrings(listenAddr), libp2p.Identity(c.cfg.PrivateKey))
	if err != nil {
		return fmt.Errorf("failed to create host: %w", err)
	}
	defer func() { _ = c.host.Close() }()

	bootstrapPeers, err := parseBootstrapAddrs(c.cfg.RemotePeerAddrs)
	if err != nil {
		return err
	}

	// Set default connection factory if not already set (tests may override).
	if c.connFactory == nil {
		c.connFactory = func(peerID peer.ID) meshConn {
			return newMeshConnection(c.host, peerID, c.log, c.bondInfo, c.topicRegistry.fetchTopics, c.handlePushMessage)
		}
	}

	// Create the connection manager.
	c.connManager = newMeshConnectionManager(&meshConnectionManagerConfig{
		host:           c.host,
		log:            c.log,
		connFactory:    c.connFactory,
		bootstrapPeers: bootstrapPeers,
	})

	c.bondInfo.AddBonds(bonds, time.Now())

	c.connManager.run(ctx)

	return nil
}
