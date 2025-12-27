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
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/martonp/tatanka-mesh/bond"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	"google.golang.org/protobuf/proto"
)

var (
	errRelayRejected = errors.New("relay rejected")
	errRelayNotFound = errors.New("relay counterparty not found")
	errRelayOther    = errors.New("relay error")
)

type testBondStorage struct {
	score uint32
}

var _ bondStorage = (*testBondStorage)(nil)

func (tbs *testBondStorage) addBonds(peerID peer.ID, bonds []*bond.BondParams) uint32 {
	return tbs.score
}

func (tbs *testBondStorage) bondStrength(peerID peer.ID) uint32 {
	return tbs.score
}

func newTestNode(t *testing.T, ctx context.Context, h host.Host, dataDir string, whitelist *whitelist) *TatankaNode {
	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger(h.ID().ShortString())
	log.SetLevel(slog.LevelDebug)

	// Write the whitelist to the data directory.
	whitelistPath := filepath.Join(dataDir, "whitelist.json")
	whitelistData, err := json.Marshal(whitelist.toFile())
	if err != nil {
		t.Fatalf("Failed to marshal whitelist: %v", err)
	}
	if err := os.WriteFile(whitelistPath, whitelistData, 0644); err != nil {
		t.Fatalf("Failed to write whitelist: %v", err)
	}

	n, err := NewTatankaNode(&Config{
		Logger:        log,
		DataDir:       dataDir,
		WhitelistPath: filepath.Join(dataDir, "whitelist.json"),
	}, WithHost(h))
	if err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}

	n.bondStorage = &testBondStorage{score: 1}

	go func() {
		if err := n.Run(ctx); err != nil {
			t.Errorf("Failed to run test node: %v", err)
		}
	}()

	if err := n.WaitReady(ctx); err != nil {
		t.Fatalf("Failed to start test node: %v", err)
	}

	return n
}

// testClient simulates a client that connects to a TatankaNode.
type testClient struct {
	log        slog.Logger
	host       host.Host
	nodeID     peer.ID
	pushStream network.Stream
	channels   map[string]chan *protocolsPb.PushMessage
	relays     chan relayRequest
	mtx        sync.RWMutex
}

type relayRequest struct {
	stream network.Stream
	req    *protocolsPb.TatankaRelayMessageRequest
}

// newTestClient creates a new test client connected to a mesh node.
// It establishes the long-running push stream and starts listening for
// messages.
func newTestClient(ctx context.Context, h host.Host, nodeID peer.ID) (*testClient, error) {
	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger(h.ID().ShortString())
	log.SetLevel(slog.LevelDebug)

	stream, err := h.NewStream(ctx, nodeID, protocols.ClientPushProtocol)
	if err != nil {
		return nil, err
	}

	// Send initial subscriptions (empty list).
	initialSubs := &protocolsPb.InitialSubscriptions{Topics: nil}
	if err := codec.WriteLengthPrefixedMessage(stream, initialSubs); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("failed to send initial subscriptions: %w", err)
	}

	// Read the success response.
	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("failed to read push stream response: %w", err)
	}
	if _, ok := resp.Response.(*protocolsPb.Response_Success); !ok {
		_ = stream.Close()
		return nil, fmt.Errorf("unexpected push stream response: %T", resp.Response)
	}

	tc := &testClient{
		log:        log,
		host:       h,
		nodeID:     nodeID,
		pushStream: stream,
		channels:   make(map[string]chan *protocolsPb.PushMessage),
		relays:     make(chan relayRequest, 2),
	}

	h.SetStreamHandler(protocols.TatankaRelayMessageProtocol, tc.handleIncomingRelay)

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
			if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
				tc.log.Errorf("Error reading message length from client %s: %v", tc.nodeID.ShortString(), err)
			}

			return
		}

		// Decode length (big-endian)
		msgLen := uint32(lengthBuf[0])<<24 | uint32(lengthBuf[1])<<16 | uint32(lengthBuf[2])<<8 | uint32(lengthBuf[3])
		if msgLen == 0 || msgLen > 10*1024*1024 { // Sanity check: max 10MB
			tc.log.Errorf("Invalid message length %d", msgLen)
			return
		}

		// Read the protobuf message
		data := make([]byte, msgLen)
		if _, err := io.ReadFull(tc.pushStream, data); err != nil {
			tc.log.Errorf("Error reading message data from client %s: %v", tc.nodeID.ShortString(), err)
			return
		}

		msg := &protocolsPb.PushMessage{}
		if err := proto.Unmarshal(data, msg); err != nil {
			tc.log.Errorf("Error unmarshaling push message from client %s: %v", tc.nodeID.ShortString(), err)
			continue
		}

		tc.mtx.Lock()
		ch, exists := tc.channels[msg.Topic]
		if !exists {
			ch = make(chan *protocolsPb.PushMessage, 100)
			tc.channels[msg.Topic] = ch
		}
		tc.mtx.Unlock()

		// Send message to channel (non-blocking with buffer)
		select {
		case ch <- msg:
		default:
			tc.log.Errorf("Warning: message buffer full for topic %s, dropping message", msg.Topic)
		}
	}
}

// Subscribe subscribes the client to a topic.
func (tc *testClient) Subscribe(ctx context.Context, topic string) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	subMsg := &protocolsPb.SubscribeRequest{Subscribe: true, Topic: topic}
	if err := codec.WriteLengthPrefixedMessage(stream, subMsg); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	return nil
}

// Unsubscribe unsubscribes the client from a topic.
func (tc *testClient) Unsubscribe(ctx context.Context, topic string) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	subMsg := &protocolsPb.SubscribeRequest{Subscribe: false, Topic: topic}
	return codec.WriteLengthPrefixedMessage(stream, subMsg)
}

// Publish publishes a message to a topic.
func (tc *testClient) Publish(ctx context.Context, topic string, data []byte) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientPublishProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	pubMsg := &protocolsPb.PublishRequest{Topic: topic, Data: data}
	if err := codec.WriteLengthPrefixedMessage(stream, pubMsg); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	return nil
}

// Close terminates the client.
func (tc *testClient) Close() {
	conns := tc.host.Network().Conns()
	for _, conn := range conns {
		streams := conn.GetStreams()
		for _, stream := range streams {
			_ = stream.Close()
		}

		_ = conn.Close()
	}

	_ = tc.host.Close()
}

// Next blocks until a message is received for the given topic and returns it.
// Returns an error if the context is cancelled before a message arrives.
func (tc *testClient) Next(ctx context.Context, topic string) (*protocolsPb.PushMessage, error) {
	tc.mtx.Lock()
	ch, exists := tc.channels[topic]
	if !exists {
		ch = make(chan *protocolsPb.PushMessage, 100)
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

// NextData blocks until a DATA message (not a subscription event) is received
// for the given topic and returns it.
func (tc *testClient) NextData(ctx context.Context, topic string) (*protocolsPb.PushMessage, error) {
	for {
		msg, err := tc.Next(ctx, topic)
		if err != nil {
			return nil, err
		}
		if msg.MessageType == protocolsPb.PushMessage_BROADCAST {
			return msg, nil
		}
	}
}

// relayMessage asks the connected tatanka node to relay a message to the
// given counterparty client and returns the response payload.
func (tc *testClient) relayMessage(ctx context.Context, counterparty peer.ID, message []byte) ([]byte, error) {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientRelayMessageProtocol)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()

	req := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte(counterparty),
		Message: message,
	}
	if err := codec.WriteLengthPrefixedMessage(stream, req); err != nil {
		return nil, err
	}

	resp := &protocolsPb.ClientRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		return nil, err
	}

	if resp.GetError() != nil {
		errObj := resp.GetError()
		switch {
		case errObj.GetCpNotFoundError() != nil:
			return nil, errRelayNotFound
		case errObj.GetCpRejectedError() != nil:
			return nil, errRelayRejected
		default:
			return nil, fmt.Errorf("%w: %v", errRelayOther, errObj)
		}
	}

	return resp.GetMessage(), nil
}

// acceptRelay waits for an incoming TatankaRelayMessageRequest and responds
// with the provided response payload.
func (tc *testClient) acceptRelay(ctx context.Context, response []byte) ([]byte, error) {
	select {
	case rr := <-tc.relays:
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Message{
				Message: response,
			},
		}
		if err := codec.WriteLengthPrefixedMessage(rr.stream, resp); err != nil {
			_ = rr.stream.Close()
			return nil, err
		}
		_ = rr.stream.Close()
		return rr.req.Message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// rejectRelay responds to a TatankaRelayMessageRequest with a rejection.
func (tc *testClient) rejectRelay(ctx context.Context) error {
	select {
	case rr := <-tc.relays:
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_CpRejectedError{
						CpRejectedError: &protocolsPb.CounterpartyRejectedError{},
					},
				},
			},
		}
		if err := codec.WriteLengthPrefixedMessage(rr.stream, resp); err != nil {
			_ = rr.stream.Close()
			return err
		}
		_ = rr.stream.Close()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tc *testClient) handleIncomingRelay(s network.Stream) {
	req := &protocolsPb.TatankaRelayMessageRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
		_ = s.Close()
		return
	}

	select {
	case tc.relays <- relayRequest{stream: s, req: req}:
	default:
		tc.log.Infof("relay channel full, closing stream")
		_ = s.Close()
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

	whitelistPeers := make([]*peer.AddrInfo, numMeshNodes)
	for i, h := range meshHosts {
		whitelistPeers[i] = &peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
	}
	mockWhitelist := &whitelist{
		peers: whitelistPeers,
	}

	runningNodes := make([]*TatankaNode, 0, numMeshNodes)
	for i, h := range meshHosts {
		dir := t.TempDir()
		if err := linkNodeWithMesh(mnet, h, runningNodes, true); err != nil {
			t.Fatalf("Failed to link node %d: %v", i, err)
		}
		node := newTestNode(t, ctx, h, dir, mockWhitelist)
		runningNodes = append(runningNodes, node)
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
		clients[i], err = newTestClient(ctx, clientHost, meshHosts[nodeIdx].ID())
		if err != nil {
			t.Fatalf("Failed to create client %d: %v", i, err)
		}
	}

	time.Sleep(time.Second)

	return mnet, runningNodes, clients
}

// checkFullyConnected verifies that all provided nodes are connected to each other.
// Returns true if fully connected, false otherwise.
func checkFullyConnected(t *testing.T, nodes []*TatankaNode) bool {
	t.Helper()
	if len(nodes) < 2 {
		return true
	}

	fullyConnected := true
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			n1 := nodes[i]
			n2 := nodes[j]

			// Check n1 -> n2
			connStatus1 := n1.node.Network().Connectedness(n2.node.ID())
			if connStatus1 != network.Connected {
				t.Logf("Node %s is not connected to %s (status: %s)",
					n1.node.ID().ShortString(), n2.node.ID().ShortString(), connStatus1)
				fullyConnected = false
			}

			// Check n2 -> n1
			connStatus2 := n2.node.Network().Connectedness(n1.node.ID())
			if connStatus2 != network.Connected {
				t.Logf("Node %s is not connected to %s (status: %s)",
					n2.node.ID().ShortString(), n1.node.ID().ShortString(), connStatus2)
				fullyConnected = false
			}
		}
	}

	return fullyConnected
}

// linkNodeWithMesh links a node to the other running nodes. Linking mocks
// the ability for a node to be reached from another node over the network.
func linkNodeWithMesh(mesh mocknet.Mocknet, host host.Host, runningNodes []*TatankaNode, link bool) error {
	if len(runningNodes) == 0 {
		return nil
	}

	for _, otherNode := range runningNodes {
		if link {
			if _, err := mesh.LinkPeers(host.ID(), otherNode.node.ID()); err != nil {
				return err
			}
		} else {
			if err := mesh.DisconnectPeers(host.ID(), otherNode.node.ID()); err != nil {
				return err
			}
			if err := mesh.UnlinkPeers(host.ID(), otherNode.node.ID()); err != nil {
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

	// Define nodes and whitelist
	peerIDs := mesh.Peers()
	h1 := mesh.Host(peerIDs[0])
	h2 := mesh.Host(peerIDs[1])
	h3 := mesh.Host(peerIDs[2])
	h4 := mesh.Host(peerIDs[3])
	h5 := mesh.Host(peerIDs[4])
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: h1.ID(), Addrs: h1.Addrs()},
			{ID: h2.ID()},
			{ID: h3.ID(), Addrs: h3.Addrs()},
			{ID: h4.ID()},
			{ID: h5.ID()},
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
		err := linkNodeWithMesh(mesh, host, runningNodes, true)
		if err != nil {
			t.Fatal(err)
		}
		node := newTestNode(t, ctx, host, dir, mockWhitelist)
		runningNodes = append(runningNodes, node)

		// First node starts alone, others should be fully connected
		if len(runningNodes) == 1 {
			if len(node.node.Network().Peers()) != 0 {
				t.Errorf("node %d should have 0 peers, but has %d", nodeNum, len(node.node.Network().Peers()))
			} else {
				t.Logf("Node %d is up. Connected to 0 peers.", nodeNum)
			}
		} else {
			if checkFullyConnected(t, runningNodes) {
				t.Logf("Node %d is up. Mesh size: %d. Fully connected.", nodeNum, len(runningNodes))
			} else {
				t.Errorf("Node %d is up. Mesh size: %d. Not fully connected.", nodeNum, len(runningNodes))
			}
		}
	}

	// Bring up nodes one by one
	startNode(1, h1, "Bootstrap")
	startNode(2, h2, "Peer")
	startNode(3, h3, "Bootstrap")
	startNode(4, h4, "Peer")
	startNode(5, h5, "Peer")
}

// requireEventually asserts that the given condition function returns true within
// the specified timeout. It polls the condition at the given tick interval.
func requireEventually(t *testing.T, condition func() bool, timeout, tick time.Duration, msg string, args ...any) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(tick)
	}

	if condition() {
		return
	}

	t.Fatalf("Condition failed after %v: %s", timeout, fmt.Sprintf(msg, args...))
}

func TestMeshRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Setup a standard 3-node mesh
	const numPeers = 3
	mesh, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}

	// Fully connected whitelist (no discovery required)
	hosts := mesh.Hosts()
	var whitelistPeers []*peer.AddrInfo
	for _, h := range hosts {
		whitelistPeers = append(whitelistPeers, &peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()})
	}
	mockWhitelist := &whitelist{peers: whitelistPeers}

	// Start the nodes
	var nodes []*TatankaNode
	for _, h := range hosts {
		err := linkNodeWithMesh(mesh, h, nodes, true)
		if err != nil {
			t.Fatal(err)
		}
		node := newTestNode(t, ctx, h, t.TempDir(), mockWhitelist)
		nodes = append(nodes, node)
	}

	// 2. Verify the mesh is fully connected
	if !checkFullyConnected(t, nodes) {
		t.Fatal("Initial mesh failed to connect")
	}

	// 3. Crash node 1
	victim := nodes[1]
	t.Logf("--- Simulating crash of Node 1 (%s) ---", victim.node.ID())
	mesh.UnlinkPeers(victim.node.ID(), nodes[0].node.ID())
	mesh.UnlinkPeers(victim.node.ID(), nodes[2].node.ID())
	victim.node.Network().ClosePeer(nodes[0].node.ID())
	victim.node.Network().ClosePeer(nodes[2].node.ID())

	// 4. Verify the mesh is broken
	requireEventually(t, func() bool {
		return nodes[0].node.Network().Connectedness(victim.node.ID()) == network.NotConnected
	}, 2*time.Second, 100*time.Millisecond, "Node 0 failed to detect Node 1 disconnect")

	// 5. "Restart" Node 1 (Restore the links)
	t.Log("--- Recovering Node 1 ---")
	mesh.LinkPeers(victim.node.ID(), nodes[0].node.ID())
	mesh.LinkPeers(victim.node.ID(), nodes[2].node.ID())

	// 6. Verify Self-Healing
	t.Log("Waiting for mesh self-healing...")
	requireEventually(t, func() bool {
		return checkFullyConnected(t, nodes)
	}, 10*time.Second, 100*time.Millisecond, "Mesh failed to auto-heal after node recovery")
}

func TestWhitelistMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start a 3 node mesh
	const numPeers = 3
	mesh, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mesh.Hosts()
	h1, h2, h3 := hosts[0], hosts[1], hosts[2]

	goodWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: h1.ID(), Addrs: h1.Addrs()},
			{ID: h2.ID(), Addrs: h2.Addrs()},
			{ID: h3.ID(), Addrs: h3.Addrs()},
		},
	}

	badWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: h1.ID(), Addrs: h1.Addrs()},
			{ID: h2.ID(), Addrs: h2.Addrs()},
			{ID: h3.ID(), Addrs: h3.Addrs()},
			{ID: randomPeerID(), Addrs: nil},
		},
	}

	// Start Nodes
	// Node 1 & 2 get the Good Whitelist
	// Node 3 gets the Bad Whitelist
	var nodes []*TatankaNode
	startNode := func(h host.Host, whitelist *whitelist) (*TatankaNode, context.CancelFunc) {
		err = linkNodeWithMesh(mesh, h, nodes, true)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(ctx)
		node := newTestNode(t, ctx, h, t.TempDir(), whitelist)
		nodes = append(nodes, node)
		return node, cancel
	}
	n1, _ := startNode(h1, goodWhitelist)
	n2, _ := startNode(h2, goodWhitelist)
	_, cancel3 := startNode(h3, badWhitelist)

	// Check that node 1 and node2 are connected, but node 3 is not.
	checkConnected := func(h1, h2 host.Host, expected network.Connectedness) bool {
		return h1.Network().Connectedness(h2.ID()) == expected &&
			h2.Network().Connectedness(h1.ID()) == expected
	}
	checkConnected(h1, h2, network.Connected)
	checkConnected(h1, h3, network.NotConnected)
	checkConnected(h2, h3, network.NotConnected)

	// Shut down node 3, restart with the correct whitelist.
	cancel3()
	n3, _ := startNode(h3, goodWhitelist)

	// Check that the mesh is fully connected.
	if !checkFullyConnected(t, []*TatankaNode{n1, n2, n3}) {
		t.Fatal("Mesh failed to connect after node 3 restart")
	}
}

func randomPeerID() peer.ID {
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		panic(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		panic(err)
	}
	return id
}

func TestClientRelay(t *testing.T) {
	t.Run("across_nodes", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 2, 2, func(i int) int { return i })
		checkRelayHappyPath(ctx, t, clients[0], clients[1])
	})

	t.Run("same_node", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 1, 2, func(i int) int { return 0 })
		checkRelayHappyPath(ctx, t, clients[0], clients[1])
	})

	t.Run("counterparty_not_found", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 1, 1, func(i int) int { return 0 })
		initiator := clients[0]

		_, err := initiator.relayMessage(ctx, randomPeerID(), []byte("hi"))
		if !errors.Is(err, errRelayNotFound) {
			t.Fatalf("expected counterparty not found error, got %v", err)
		}
	})

	t.Run("direct_reject", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 1, 2, func(i int) int { return 0 })
		initiator := clients[0]
		counterparty := clients[1]

		// Make counterparty reject.
		go func() {
			_ = counterparty.rejectRelay(ctx)
		}()

		if _, err := initiator.relayMessage(ctx, counterparty.host.ID(), []byte("hi")); !errors.Is(err, errRelayRejected) {
			t.Fatalf("expected rejection error, got %v", err)
		}
	})

	t.Run("forward_reject", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 2, 2, func(i int) int { return i })
		initiator := clients[0]
		counterparty := clients[1]

		// Make counterparty reject.
		go func() {
			_ = counterparty.rejectRelay(ctx)
		}()

		if _, err := initiator.relayMessage(ctx, counterparty.host.ID(), []byte("hi")); !errors.Is(err, errRelayRejected) {
			t.Fatalf("expected rejection error, got %v", err)
		}
	})
}

// checkRelayHappyPath checks that sending a message between two clients works.
func checkRelayHappyPath(ctx context.Context, t *testing.T, initiator, counterparty *testClient) {
	t.Helper()

	// Allow gossip to propagate.
	time.Sleep(time.Second)

	errCh := make(chan error, 1)
	respCh := make(chan []byte, 1)
	go func() {
		reqMsg, err := counterparty.acceptRelay(ctx, []byte("pong from counterparty"))
		if err != nil {
			errCh <- err
			return
		}
		if string(reqMsg) != "ping from initiator" {
			errCh <- fmt.Errorf("unexpected request message: %q", string(reqMsg))
			return
		}
		respCh <- []byte("pong from counterparty")
	}()

	resp, err := initiator.relayMessage(ctx, counterparty.host.ID(), []byte("ping from initiator"))
	if err != nil {
		t.Fatalf("initiator failed to relay message: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("counterparty relay error: %v", err)
	case want := <-respCh:
		if string(resp) != string(want) {
			t.Fatalf("unexpected response: got %q want %q", string(resp), string(want))
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for relay: %v", ctx.Err())
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

			msg, err := subscriber.NextData(ctx, topic)
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

	// Terminate clients.
	for idx := range clients {
		clients[idx].Close()
	}
}

func TestClientSubscriptionEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const numMeshNodes = 2
	const numClients = 4

	_, _, clients := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, numClients, func(i int) int {
		return i / 2
	})

	topic := "test_topic"

	// Helper to verify subscription/unsubscription events
	verifyEvents := func(subscribers []*testClient, expectedSender *testClient, isSubscribe bool) {
		t.Helper()
		expectedType := protocolsPb.PushMessage_UNSUBSCRIBE
		eventName := "UNSUBSCRIBE"
		if isSubscribe {
			expectedType = protocolsPb.PushMessage_SUBSCRIBE
			eventName = "SUBSCRIBE"
		}

		for _, client := range subscribers {
			msg, err := client.Next(ctx, topic)
			if err != nil {
				t.Fatalf("Client %s failed to receive message: %v", client.host.ID().ShortString(), err)
			}
			if msg.MessageType != expectedType {
				t.Fatalf("Expected %s event, got %v", eventName, msg.MessageType)
			}
			if string(msg.Sender) != string(expectedSender.host.ID()) {
				t.Fatalf("Wrong sender in %s event. Expected %s, got %s",
					eventName, expectedSender.host.ID().ShortString(), peer.ID(msg.Sender).ShortString())
			}
		}
	}

	t.Log("Client 0 subscribes (first subscriber)")
	if err := clients[0].Subscribe(ctx, topic); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	t.Log("Client 1 subscribes - Client 0 should receive event")
	if err := clients[1].Subscribe(ctx, topic); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	verifyEvents([]*testClient{clients[0]}, clients[1], true)

	time.Sleep(200 * time.Millisecond)

	t.Log("Client 2 subscribes - Clients 0,1 should receive event")
	if err := clients[2].Subscribe(ctx, topic); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	verifyEvents([]*testClient{clients[0], clients[1]}, clients[2], true)

	t.Log("Client 1 unsubscribes - Clients 0,2 should receive event")
	if err := clients[1].Unsubscribe(ctx, topic); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}
	verifyEvents([]*testClient{clients[0], clients[2]}, clients[1], false)

	t.Log("Client 3 subscribes - Clients 0,2 should receive event")
	if err := clients[3].Subscribe(ctx, topic); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	verifyEvents([]*testClient{clients[0], clients[2]}, clients[3], true)

	// Terminate clients.
	for idx := range clients {
		clients[idx].Close()
	}
}
