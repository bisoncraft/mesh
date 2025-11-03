package tatanka

import (
	"context"
	"errors"
	"fmt"

	"github.com/decred/slog"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	pb "github.com/martonp/tatanka-mesh/tatanka/pb"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const (
	// clientMessageTopicName is the name of the pubsub topic for client messages.
	clientMessageTopicName = "client_messages"

	// clientConnectionsTopicName is the name of the pubsub topic used to
	// propagate client connection and disconnection messages between tatanka
	// nodes.
	clientConnectionsTopicName = "client_connections"
)

type clientConnectionUpdate struct {
	clientID   peer.ID
	reporterID peer.ID
	addrs      []ma.Multiaddr
	timestamp  int64
	connected  bool
}

func newClientConnectionUpdateFromPb(msg *pb.ClientConnectionMsg) (*clientConnectionUpdate, error) {
	clientID, err := peer.IDFromBytes(msg.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to parse client ID: %w", err)
	}

	reporterID, err := peer.IDFromBytes(msg.ReporterId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reporter ID: %w", err)
	}

	var addrs []ma.Multiaddr
	if msg.Addrs != nil {
		addrs = make([]ma.Multiaddr, len(msg.Addrs))
		for i, addrBytes := range msg.Addrs {
			addr, err := ma.NewMultiaddrBytes(addrBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse multiaddr: %w", err)
			}
			addrs[i] = addr
		}
	}

	return &clientConnectionUpdate{
		clientID:   clientID,
		reporterID: reporterID,
		addrs:      addrs,
		timestamp:  msg.Timestamp,
		connected:  msg.Connected,
	}, nil
}

func (c *clientConnectionUpdate) toPb() *pb.ClientConnectionMsg {
	var addrBytes [][]byte
	if c.addrs != nil {
		addrBytes = make([][]byte, len(c.addrs))
		for i, addr := range c.addrs {
			addrBytes[i] = addr.Bytes()
		}
	}

	return &pb.ClientConnectionMsg{
		Id:         []byte(c.clientID),
		ReporterId: []byte(c.reporterID),
		Addrs:      addrBytes,
		Timestamp:  c.timestamp,
		Connected:  c.connected,
	}
}

type gossipSubCfg struct {
	node                          host.Host
	log                           slog.Logger
	getManifestPeers              func() map[peer.ID]struct{}
	handleBroadcastMessage        func(msg *protocolsPb.ClientPushMessage)
	handleClientConnectionMessage func(update *clientConnectionUpdate)
}

// gossipSub manages the nodes connection to a gossip sub network between tatanka
// nodes. This network is used to gossip all client broadcast messages and
// client connections.
type gossipSub struct {
	log                    slog.Logger
	ps                     *pubsub.PubSub
	cfg                    *gossipSubCfg
	clientMessageTopic     *pubsub.Topic
	clientConnectionsTopic *pubsub.Topic
}

// gossipSubParams returns gossip sub parameters to create a fully connected mesh
// of the given size. It is expected that the mesh size is greater than 0.
func gossipSubParams(meshSize int) pubsub.GossipSubParams {
	gossipParams := pubsub.DefaultGossipSubParams()
	fullMeshDegree := meshSize - 1
	gossipParams.D = fullMeshDegree
	gossipParams.Dlo = fullMeshDegree
	gossipParams.Dhi = fullMeshDegree
	gossipParams.Dlazy = 0
	gossipParams.Dscore = min(4, fullMeshDegree)
	gossipParams.Dout = 0
	return gossipParams
}

func newGossipSub(ctx context.Context, cfg *gossipSubCfg) (*gossipSub, error) {
	meshSize := len(cfg.getManifestPeers())
	if meshSize == 0 {
		return nil, errors.New("number of manifest peers must be greater than 0 for gossipsub")
	}

	// If the mesh size changes, gossipParams would need to be updated.
	gossipParams := gossipSubParams(meshSize)

	peerFilter := func(pid peer.ID, topic string) bool {
		_, ok := cfg.getManifestPeers()[pid]
		return ok
	}

	ps, err := pubsub.NewGossipSub(ctx, cfg.node,
		pubsub.WithGossipSubParams(gossipParams),
		pubsub.WithPeerFilter(peerFilter),
		// FloodPublish optimizes for quick delivery at the cost of duplicates.
		pubsub.WithFloodPublish(true),
	)
	if err != nil {
		return nil, err
	}

	// Currently using a single topic for all client messages, but
	// we could use separate topics for each client topic to only
	// send messages to tatanka nodes that have clients subscribed to
	// a certain topic.
	clientMessageTopic, err := ps.Join(clientMessageTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join client message topic: %w", err)
	}

	clientConnectionsTopic, err := ps.Join(clientConnectionsTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join client connections topic: %w", err)
	}

	return &gossipSub{
		log:                    cfg.log,
		ps:                     ps,
		cfg:                    cfg,
		clientMessageTopic:     clientMessageTopic,
		clientConnectionsTopic: clientConnectionsTopic,
	}, nil
}

// listenForClientMessages subscribes to the pubsub client messages topic, and
// distributes messages to subscribed clients as they come in.
func (gs *gossipSub) listenForClientMessages(ctx context.Context) error {
	// TODO: configure buffer size if needed
	sub, err := gs.clientMessageTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to client message topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil {
			pushMessage := &protocolsPb.ClientPushMessage{}
			if err := proto.Unmarshal(msg.Data, pushMessage); err != nil {
				gs.log.Errorf("Failed to unmarshal push message: %v", err)
				continue
			}

			gs.cfg.handleBroadcastMessage(pushMessage)
		}
	}
}

func (gs *gossipSub) listenForClientConnections(ctx context.Context) error {
	sub, err := gs.clientConnectionsTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to client connections topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil {
			connMsg := &pb.ClientConnectionMsg{}
			if err := proto.Unmarshal(msg.Data, connMsg); err != nil {
				gs.log.Errorf("Failed to unmarshal client connection message: %v", err)
				continue
			}

			update, err := newClientConnectionUpdateFromPb(connMsg)
			if err != nil {
				gs.log.Errorf("Failed to create client connection update from message: %v", err)
				continue
			}

			gs.cfg.handleClientConnectionMessage(update)
		}
	}
}

func (gs *gossipSub) publishClientMessage(ctx context.Context, msg *protocolsPb.ClientPushMessage) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal client push message: %w", err)
	}
	return gs.clientMessageTopic.Publish(ctx, data)
}

func (gs *gossipSub) publishClientConnectionMessage(ctx context.Context, msg *clientConnectionUpdate) error {
	data, err := proto.Marshal(msg.toPb())
	if err != nil {
		return fmt.Errorf("failed to marshal client connection message: %w", err)
	}

	return gs.clientConnectionsTopic.Publish(ctx, data)
}

func (gs *gossipSub) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := gs.listenForClientMessages(ctx)
		gs.log.Debug("Client broadcast messages listener stopped.")
		return err
	})

	g.Go(func() error {
		err := gs.listenForClientConnections(ctx)
		gs.log.Debug("Client connection listener stopped.")
		return err
	})

	return g.Wait()
}
