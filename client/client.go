package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/bond"

	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	defaultHost        = "0.0.0.0"
	defaultPort        = 7565
	writeTimeout       = 5 * time.Second
	maxPostBondRetries = 3
)

var (
	// ErrRedundantSubscription indicates a redundant subscription has been made.
	ErrRedundantSubscription = errors.New("redundant subscription")

	errNoMeshConnection = errors.New("no mesh connection established")
)

// Config represents a tatanka client configuration.
type Config struct {
	Port           int
	PrivateKey     crypto.PrivKey
	RemotePeerAddr string
	Logger         slog.Logger
}

// Client represents a tatanka client.
type Client struct {
	cfg             *Config
	host            host.Host
	primaryMeshConn atomic.Pointer[meshConnection]
	topicRegistry   *topicRegistry
	bondInfo        *bond.BondInfo
	log             slog.Logger

	connectionsMtx sync.RWMutex
	connections    map[peer.ID]*meshConnection
}

// Ensure the client implements the network.Notifiee interface.
var _ network.Notifiee = (*Client)(nil)

func (c *Client) Listen(net network.Network, maddr ma.Multiaddr) {}

func (c *Client) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (c *Client) Connected(net network.Network, conn network.Conn) {}

func (c *Client) Disconnected(net network.Network, conn network.Conn) {
	c.connectionsMtx.RLock()
	meshConn, ok := c.connections[conn.RemotePeer()]
	if !ok {
		c.connectionsMtx.RUnlock()
		return
	}
	c.connectionsMtx.RUnlock()

	// Unset the primary mesh connection if it has been disconnected.
	pmc := c.primaryMeshConn.Load()
	if pmc != nil {
		if pmc.host.ID() == meshConn.host.ID() {
			c.primaryMeshConn.Store(nil)
		}
	}

	meshConn.kill()
}

// NewClient initializes a new tatanka client.
func NewClient(cfg *Config) (*Client, error) {
	c := &Client{
		cfg:           cfg,
		topicRegistry: newTopicRegistry(),
		connections:   make(map[peer.ID]*meshConnection),
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

func (c *Client) setPrimaryMeshConnection(mc *meshConnection) {
	c.primaryMeshConn.Store(mc)
}

// Broadcast publishes the provided message bytes on a mesh topic.
func (c *Client) Broadcast(ctx context.Context, publishMsgBytes []byte) error {
	meshConn := c.primaryMeshConn.Load()
	if meshConn == nil {
		return fmt.Errorf("failed to broadcast message: %w", errNoMeshConnection)
	}

	return meshConn.broadcast(ctx, publishMsgBytes)
}

// Subscribe subscribes the client to the provided topic.
func (c *Client) Subscribe(ctx context.Context, topic string, handlerFunc func(*protocolsPb.PushMessage)) error {
	// Ensure the topic has not already been subscribed to.
	registered := c.topicRegistry.isRegistered(topic)
	if registered {
		return ErrRedundantSubscription
	}

	meshConn := c.primaryMeshConn.Load()
	if meshConn == nil {
		return fmt.Errorf("failed to subscribe topic: %w", errNoMeshConnection)
	}

	err := meshConn.subscribe(ctx, topic)
	if err != nil {
		return err
	}

	// Register the subscribed topic with its handler.
	c.topicRegistry.register(topic, handlerFunc)

	return nil
}

// Unsubscribe unsubscribes the client from the provided topic.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	// Ensure the topic is already subscribed to.
	registered := c.topicRegistry.isRegistered(topic)
	if !registered {
		return nil
	}

	meshConn := c.primaryMeshConn.Load()
	if meshConn == nil {
		return fmt.Errorf("failed to unsubscribe topic: %w", errNoMeshConnection)
	}

	err := meshConn.unsubscribe(ctx, topic)
	if err != nil {
		return err
	}

	// Remove unsubscribed topic.
	c.topicRegistry.unregister(topic)

	return nil
}

// handlePushMessage processes incoming pushed messages. The messages are processed based on their topics.
func (c *Client) handlePushMessage(msg *protocolsPb.PushMessage) {
	hostID := c.host.ID()
	handlerFunc, err := c.topicRegistry.fetchHandler(msg.Topic)
	if err != nil {
		c.log.Errorf("%s: %v", hostID, err)
		return
	}

	handlerFunc(msg)
}

// postBond posts the client's bond.
func (c *Client) postBond(ctx context.Context) error {
	meshConn := c.primaryMeshConn.Load()
	if meshConn == nil {
		return fmt.Errorf("failed to post bond: %w", errNoMeshConnection)
	}

	return meshConn.postBond(ctx)
}

// maintainConnection maintains a mesh connection to the provided remote peer ID.
func (c *Client) maintainConnection(ctx context.Context, remotePeerID peer.ID) {
	reconnectTimer := time.NewTimer(0)
	defer reconnectTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconnectTimer.C:
			meshConn := newMeshConnection(c.host, remotePeerID, c.log, c.bondInfo, c.topicRegistry.fetchTopics, c.handlePushMessage, c.setPrimaryMeshConnection)

			c.connectionsMtx.Lock()
			c.connections[remotePeerID] = meshConn
			c.connectionsMtx.Unlock()

			err := meshConn.run(ctx)
			if err != nil {
				c.log.Error(err)
				reconnectTimer = time.NewTimer(time.Minute)
				continue
			}

			// Initially worked, but was disconnected. meshConn.kill was called.
			reconnectTimer = time.NewTimer(0)
		}
	}
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

	// Ensure the client receives connection state notifications.
	c.host.Network().Notify(c)

	// TODO: take multiple peer addresses from the mesh and establish connections to
	// them to failover to if the current peer becomes unreachable.
	peerAddr, err := ma.NewMultiaddr(c.cfg.RemotePeerAddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer address: %w", err)
	}

	peerInfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
	if err != nil {
		return fmt.Errorf("failed parsing peer info from address: %w", err)
	}

	c.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, time.Hour)
	c.bondInfo.AddBonds(bonds, time.Now())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.maintainConnection(ctx, peerInfo.ID)
		wg.Done()
	}()

	wg.Wait()

	return nil
}
