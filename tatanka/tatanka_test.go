package tatanka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

func newTestNode(t *testing.T, ctx context.Context, h host.Host, dataDir string, manifest *manifest) *TatankaNode {
	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger(h.ID().ShortString())
	log.SetLevel(slog.LevelDebug)

	// Write the manifest to the data directory.
	manifestPath := filepath.Join(dataDir, "manifest.json")
	manifestData, err := json.Marshal(manifest.toManifestFile())
	if err != nil {
		t.Fatalf("Failed to marshal manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, manifestData, 0644); err != nil {
		t.Fatalf("Failed to write manifest: %v", err)
	}

	n, err := NewTatankaNode(&Config{
		Logger:       log,
		DataDir:      dataDir,
		ManifestPath: filepath.Join(dataDir, "manifest.json"),
	}, WithHost(h))
	if err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}

	go func() {
		if err := n.Run(ctx); err != nil {
			t.Errorf("Failed to run test node: %v", err)
		}
	}()

	return n
}

// testClient simulates a client that connects to a TatankaNode.
type testClient struct {
	t          *testing.T
	host       host.Host
	nodeID     peer.ID
	pushStream network.Stream
	channels   map[string]chan *protocolsPb.ClientPushMessage
	mtx        sync.RWMutex
}

// newTestClient creates a new test client connected to a mesh node.
// It establishes the long-running push stream and starts listening for
// messages.
func newTestClient(t *testing.T, ctx context.Context, h host.Host, nodeID peer.ID) (*testClient, error) {
	stream, err := h.NewStream(ctx, nodeID, protocols.ClientPushProtocol)
	if err != nil {
		return nil, err
	}

	tc := &testClient{
		t:          t,
		host:       h,
		nodeID:     nodeID,
		pushStream: stream,
		channels:   make(map[string]chan *protocolsPb.ClientPushMessage),
	}

	// Start goroutine to read incoming push messages
	go tc.readPushMessages()

	return tc, nil
}

// readPushMessages reads and decodes messages from the push stream.
// Messages are length-prefixed: 4 bytes (big-endian) length, then protobuf data.
func (tc *testClient) readPushMessages() {
	lengthBuf := make([]byte, 4)
	for {
		// Read 4-byte length prefix
		if _, err := io.ReadFull(tc.pushStream, lengthBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return
			}
			tc.t.Logf("Error reading message length from client %s: %v", tc.nodeID.ShortString(), err)
			return
		}

		// Decode length (big-endian)
		msgLen := uint32(lengthBuf[0])<<24 | uint32(lengthBuf[1])<<16 | uint32(lengthBuf[2])<<8 | uint32(lengthBuf[3])
		if msgLen == 0 || msgLen > 10*1024*1024 { // Sanity check: max 10MB
			tc.t.Logf("Invalid message length %d", msgLen)
			return
		}

		// Read the protobuf message
		data := make([]byte, msgLen)
		if _, err := io.ReadFull(tc.pushStream, data); err != nil {
			tc.t.Logf("Error reading message data from client %s: %v", tc.nodeID.ShortString(), err)
			return
		}

		msg := &protocolsPb.ClientPushMessage{}
		if err := proto.Unmarshal(data, msg); err != nil {
			tc.t.Logf("Error unmarshaling push message from client %s: %v", tc.nodeID.ShortString(), err)
			continue
		}

		tc.mtx.Lock()
		ch, exists := tc.channels[msg.Topic]
		if !exists {
			ch = make(chan *protocolsPb.ClientPushMessage, 100)
			tc.channels[msg.Topic] = ch
		}
		tc.mtx.Unlock()

		// Send message to channel (non-blocking with buffer)
		select {
		case ch <- msg:
		default:
			tc.t.Logf("Warning: message buffer full for topic %s, dropping message", msg.Topic)
		}
	}
}

// Subscribe subscribes the client to a topic.
func (tc *testClient) Subscribe(ctx context.Context, topic string) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return err
	}
	defer stream.Close()

	subMsg := &protocolsPb.ClientSubscribeMessage{Subscribe: true, Topic: topic}
	return codec.WriteLengthPrefixedMessage(stream, subMsg)
}

// Publish publishes a message to a topic.
func (tc *testClient) Publish(ctx context.Context, topic string, data []byte) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientPublishProtocol)
	if err != nil {
		return err
	}
	defer stream.Close()

	pubMsg := &protocolsPb.ClientPublishMessage{Topic: topic, Data: data}
	return codec.WriteLengthPrefixedMessage(stream, pubMsg)
}

// Next blocks until a message is received for the given topic and returns it.
// Returns an error if the context is cancelled before a message arrives.
func (tc *testClient) Next(ctx context.Context, topic string) (*protocolsPb.ClientPushMessage, error) {
	tc.mtx.Lock()
	ch, exists := tc.channels[topic]
	if !exists {
		ch = make(chan *protocolsPb.ClientPushMessage, 100)
		tc.channels[topic] = ch
	}
	tc.mtx.Unlock()

	select {
	case msg := <-ch:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (tc *testClient) GetPeerAddr(ctx context.Context, id peer.ID) ([]ma.Multiaddr, error) {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientAddrProtocol)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	addrMsg := &protocolsPb.ClientAddrRequestMessage{Id: []byte(id)}
	if err := codec.WriteLengthPrefixedMessage(stream, addrMsg); err != nil {
		return nil, err
	}
	stream.CloseWrite()

	responseMessage := &protocolsPb.ClientAddrResponseMessage{}
	if err := codec.ReadLengthPrefixedMessage(stream, responseMessage); err != nil {
		return nil, err
	}

	// Switch on the result type
	switch result := responseMessage.Result.(type) {
	case *protocolsPb.ClientAddrResponseMessage_Success_:
		// Convert bytes back to multiaddrs
		addrs := make([]ma.Multiaddr, len(result.Success.Addrs))
		for i, addrBytes := range result.Success.Addrs {
			addr, err := ma.NewMultiaddrBytes(addrBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse multiaddr: %w", err)
			}
			addrs[i] = addr
		}
		return addrs, nil

	case *protocolsPb.ClientAddrResponseMessage_Error:
		return nil, fmt.Errorf("server error: %s", result.Error)

	default:
		return nil, fmt.Errorf("no success or error in response")
	}
}

func fullyConnectedMeshWithClients(ctx context.Context, t *testing.T, numMeshNodes, numClients int, clientToNode func(int) int) (
	net mocknet.Mocknet, meshNodes []*TatankaNode, clients []*testClient) {
	mnet, err := mocknet.WithNPeers(numMeshNodes + numClients)
	if err != nil {
		t.Fatal(err)
	}

	allPeers := mnet.Peers()
	meshHosts := make([]host.Host, numMeshNodes)
	for i := range meshHosts {
		meshHosts[i] = mnet.Host(allPeers[i])
	}

	clientHosts := make([]host.Host, numClients)
	for i := range clientHosts {
		clientHosts[i] = mnet.Host(allPeers[numMeshNodes+i])
	}

	// Just make all mesh nodes bootstrap nodes.
	bootstrapPeers := make([]*peer.AddrInfo, numMeshNodes)
	for i, h := range meshHosts {
		bootstrapPeers[i] = &peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
	}
	mockManifest := &manifest{
		bootstrapPeers:    bootstrapPeers,
		nonBootstrapPeers: []peer.ID{},
	}

	runningNodes := make([]*TatankaNode, 0, numMeshNodes)
	for i, h := range meshHosts {
		dir := t.TempDir()
		node := newTestNode(t, ctx, h, dir, mockManifest)
		if err := linkNodeWithMesh(mnet, node, runningNodes, true); err != nil {
			t.Fatalf("Failed to link node %d: %v", i, err)
		}
		runningNodes = append(runningNodes, node)
		runMaintenance(ctx, node)
	}

	// Make sure the mesh is fully connected
	checkFullyConnected(t, runningNodes)

	time.Sleep(time.Second)

	clients = make([]*testClient, numClients)
	for i, clientHost := range clientHosts {
		nodeIdx := clientToNode(i)
		if _, err := mnet.LinkPeers(clientHosts[i].ID(), meshHosts[nodeIdx].ID()); err != nil {
			t.Fatalf("Failed to link client %d to node %d: %v", i, nodeIdx, err)
		}
		if _, err := mnet.ConnectPeers(clientHosts[i].ID(), meshHosts[nodeIdx].ID()); err != nil {
			t.Fatalf("Failed to connect client %d to node %d: %v", i, nodeIdx, err)
		}
		clients[i], err = newTestClient(t, ctx, clientHost, meshHosts[nodeIdx].ID())
		if err != nil {
			t.Fatalf("Failed to create client %d: %v", i, err)
		}
	}

	return mnet, runningNodes, clients
}

// runMeshMaintenance simulates a single cycle of the node's maintenance loop.
func runMaintenance(ctx context.Context, node *TatankaNode) {
	node.refreshPeersFromBootstrap(ctx)
	node.connectToPeers(ctx)
}

// checkFullyConnected verifies that all provided nodes are connected to each other.
func checkFullyConnected(t *testing.T, nodes []*TatankaNode) {
	t.Helper()
	if len(nodes) < 2 {
		return
	}

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			n1 := nodes[i]
			n2 := nodes[j]

			// Check n1 -> n2
			connStatus1 := n1.node.Network().Connectedness(n2.node.ID())
			if connStatus1 != network.Connected {
				t.Errorf("Node %s is not connected to %s (status: %s)",
					n1.node.ID().ShortString(), n2.node.ID().ShortString(), connStatus1)
			}

			// Check n2 -> n1
			connStatus2 := n2.node.Network().Connectedness(n1.node.ID())
			if connStatus2 != network.Connected {
				t.Errorf("Node %s is not connected to %s (status: %s)",
					n2.node.ID().ShortString(), n1.node.ID().ShortString(), connStatus2)
			}
		}
	}
}

// linkNodeWithMesh links a node to the other running nodes. Linking mocks
// the ability for a node to be reached from another node over the network.
func linkNodeWithMesh(mesh mocknet.Mocknet, node *TatankaNode, runningNodes []*TatankaNode, link bool) error {
	if len(runningNodes) == 0 {
		return nil
	}

	for _, otherNode := range runningNodes {
		if link {
			if _, err := mesh.LinkPeers(node.node.ID(), otherNode.node.ID()); err != nil {
				return err
			}
		} else {
			if err := mesh.DisconnectPeers(node.node.ID(), otherNode.node.ID()); err != nil {
				return err
			}
			if err := mesh.UnlinkPeers(node.node.ID(), otherNode.node.ID()); err != nil {
				return err
			}
		}
	}

	return nil
}

// TestProgressiveMeshStartup simulates the staggered startup of a 5-node mesh.
// Makes sure that each node fully connects to the mesh as it comes online.
func TestProgressiveMeshStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a mock network with 5 nodes
	const numPeers = 5
	mesh, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}

	// Define nodes and manifest
	peerIDs := mesh.Peers()
	h1 := mesh.Host(peerIDs[0])
	h2 := mesh.Host(peerIDs[1])
	h3 := mesh.Host(peerIDs[2])
	h4 := mesh.Host(peerIDs[3])
	h5 := mesh.Host(peerIDs[4])
	mockManifest := &manifest{
		nonBootstrapPeers: []peer.ID{
			h2.ID(),
			h4.ID(),
			h5.ID(),
		},
		bootstrapPeers: []*peer.AddrInfo{
			{ID: h1.ID(), Addrs: h1.Addrs()},
			{ID: h3.ID(), Addrs: h3.Addrs()},
		},
	}

	// runningNodes will hold all running nodes that have been connected
	// to the mesh.
	runningNodes := make([]*TatankaNode, 0, numPeers)

	// startNode links a node to the rest of the mesh, runs the connection logic,
	// then makes sure that the mesh is still fully connected.
	startNode := func(nodeNum int, host host.Host, nodeType string) {
		t.Helper()

		dir := t.TempDir()
		t.Logf("--- Starting Node %d (%s) ---", nodeNum, nodeType)
		node := newTestNode(t, ctx, host, dir, mockManifest)
		err := linkNodeWithMesh(mesh, node, runningNodes, true)
		if err != nil {
			t.Fatal(err)
		}
		runningNodes = append(runningNodes, node)
		runMaintenance(ctx, node)

		// First node starts alone, others should be fully connected
		if len(runningNodes) == 1 {
			if len(node.node.Network().Peers()) != 0 {
				t.Errorf("node %d should have 0 peers, but has %d", nodeNum, len(node.node.Network().Peers()))
			}
			t.Logf("Node %d is up. Connected to 0 peers.", nodeNum)
		} else {
			checkFullyConnected(t, runningNodes)
			t.Logf("Node %d is up. Mesh size: %d. Fully connected.", nodeNum, len(runningNodes))
		}
	}

	// Bring up nodes one by one
	startNode(1, h1, "Bootstrap")
	startNode(2, h2, "Peer")
	startNode(3, h3, "Bootstrap")
	startNode(4, h4, "Peer")
	startNode(5, h5, "Peer")
}

// TestDiscoveryProtocol tests the peer discovery mechanism where a node
// shares its connected peers with a requesting node.
func TestDiscoveryProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a fully connected mesh of 4 nodes, but only three are listed in
	// the manifest.
	mesh, err := mocknet.FullMeshConnected(4)
	if err != nil {
		t.Fatal(err)
	}

	peerIDs := mesh.Peers()
	h1 := mesh.Host(peerIDs[0])
	h2 := mesh.Host(peerIDs[1])
	h3 := mesh.Host(peerIDs[2])
	h4 := mesh.Host(peerIDs[3])

	mockManifest := &manifest{
		nonBootstrapPeers: []peer.ID{
			h1.ID(),
			h2.ID(),
			h3.ID(),
		},
	}

	n1 := newTestNode(t, ctx, h1, t.TempDir(), mockManifest)
	newTestNode(t, ctx, h2, t.TempDir(), mockManifest)
	n3 := newTestNode(t, ctx, h3, t.TempDir(), mockManifest)
	newTestNode(t, ctx, h4, t.TempDir(), mockManifest)

	time.Sleep(time.Second)

	t.Logf("Setup complete. n1 is connected to %d peers", len(n1.node.Network().Peers()))

	// Node 3 calls discover peers on node 1. Only the whitelisted peers
	// in the manifest should be returned.
	discoveredPeers, err := n3.discoverPeers(ctx, &peer.AddrInfo{ID: h1.ID()})
	if err != nil {
		t.Fatalf("Discovery failed: %v", err)
	}
	if len(discoveredPeers) != 2 {
		t.Fatalf("Expected 2 discovered peers, but got %d", len(discoveredPeers))
	}
	discoveredIDs := make(map[peer.ID]bool)
	for _, p := range discoveredPeers {
		discoveredIDs[p.ID] = true
		if len(p.Addrs) == 0 {
			t.Errorf("Discovered peer %s has no addresses", p.ID.ShortString())
		}
	}
	if !discoveredIDs[h2.ID()] {
		t.Errorf("Expected to discover h2 (%s)", h2.ID().ShortString())
	}
	if !discoveredIDs[h3.ID()] {
		t.Errorf("Expected to discover h3 (%s)", h3.ID().ShortString())
	}
}

func TestClientSubscriptionAndBroadcast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numMeshNodes = 3
	const numClients = 6

	_, _, clients := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, numClients, func(i int) int {
		return i / 2
	})

	time.Sleep(time.Second)

	topic1 := "topic_1"
	topic2 := "topic_2"

	// Subscribe clients to topics:
	// Clients 0, 1 -> topic1
	// Clients 2, 3 -> topic2
	// Clients 4, 5 -> both topics
	topic1Subscribers := []*testClient{clients[0], clients[1], clients[4], clients[5]}
	topic2Subscribers := []*testClient{clients[2], clients[3], clients[4], clients[5]}

	for _, client := range topic1Subscribers {
		if err := client.Subscribe(ctx, topic1); err != nil {
			t.Fatalf("Failed to subscribe client %s to topic %s: %v", client.host.ID().ShortString(), topic1, err)
		}
	}

	for _, client := range topic2Subscribers {
		if err := client.Subscribe(ctx, topic2); err != nil {
			t.Fatalf("Failed to subscribe client %s to topic %s: %v", client.host.ID().ShortString(), topic2, err)
		}
	}

	time.Sleep(time.Second)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Run 100 iterations of random publish/receive
	for iteration := 0; iteration < 100; iteration++ {
		// Randomly select a topic
		var topic string
		var subscribers []*testClient
		if rng.Intn(2) == 0 {
			topic = topic1
			subscribers = topic1Subscribers
		} else {
			topic = topic2
			subscribers = topic2Subscribers
		}

		// Randomly select a publisher from the subscribers
		publisherIdx := rng.Intn(len(subscribers))
		publisher := subscribers[publisherIdx]

		// Create a unique message
		msgData := []byte(fmt.Sprintf("message_%d_from_%s_on_%s", iteration, publisher.host.ID().ShortString(), topic))

		// Publish the message
		if err := publisher.Publish(ctx, topic, msgData); err != nil {
			t.Fatalf("Iteration %d: Failed to publish message to topic %s: %v", iteration, topic, err)
		}

		// All subscribers (except the publisher) should receive the message
		for _, subscriber := range subscribers {
			// Skip the publisher - they should not receive their own message
			if subscriber == publisher {
				continue
			}

			msg, err := subscriber.Next(ctx, topic)
			if err != nil {
				t.Fatalf("Iteration %d: Client %s failed to receive message on topic %s: %v",
					iteration, subscriber.host.ID().ShortString(), topic, err)
			}

			if msg.Topic != topic {
				t.Fatalf("Iteration %d: Client %s received message with wrong topic. Expected %s, got %s",
					iteration, subscriber.host.ID().ShortString(), topic, msg.Topic)
			}

			if string(msg.Data) != string(msgData) {
				t.Fatalf("Iteration %d: Client %s received message with wrong data. Expected %s, got %s",
					iteration, subscriber.host.ID().ShortString(), string(msgData), string(msg.Data))
			}

			t.Logf("Iteration %d: Client %s successfully received message on topic %s",
				iteration, subscriber.host.ID().ShortString(), topic)
		}

		// Small delay between iterations
		time.Sleep(50 * time.Millisecond)
	}
}

func TestClientAddrSharing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMeshNodes = 3
	const numClients = 3
	_, _, clients := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, numClients, func(i int) int {
		return i
	})

	time.Sleep(time.Second)

	multiAddrsEqual := func(addrs1, addrs2 []ma.Multiaddr) bool {
		if len(addrs1) != len(addrs2) {
			return false
		}
		for i := range addrs1 {
			if !addrs1[i].Equal(addrs2[i]) {
				return false
			}
		}
		return true
	}

	addr, err := clients[0].GetPeerAddr(ctx, clients[1].host.ID())
	if err != nil {
		t.Fatalf("Failed to get client 1 addr: %v", err)
	}
	if !multiAddrsEqual(addr, clients[1].host.Addrs()) {
		t.Fatalf("Client 1 addr does not match expected addr. Got %v, expected %v", addr, clients[1].host.Addrs())
	}

	addr, err = clients[0].GetPeerAddr(ctx, clients[2].host.ID())
	if err != nil {
		t.Fatalf("Failed to get client 2 addr: %v", err)
	}
	if !multiAddrsEqual(addr, clients[2].host.Addrs()) {
		t.Fatalf("Client 2 addr does not match expected addr. Got %v, expected %v", addr, clients[2].host.Addrs())
	}
}
