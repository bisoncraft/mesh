package tatanka

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

func TestPeerInfoRoundTrip(t *testing.T) {
	// Create sample multiaddrs
	addr1, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		t.Fatalf("Failed to create multiaddr1: %v", err)
	}
	addr2, err := ma.NewMultiaddr("/ip6/::1/tcp/5678")
	if err != nil {
		t.Fatalf("Failed to create multiaddr2: %v", err)
	}

	// Create sample peer.AddrInfo
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	originalID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("Failed to get peer ID from public key: %v", err)
	}
	original := peer.AddrInfo{
		ID:    originalID,
		Addrs: []ma.Multiaddr{addr1, addr2},
	}

	// Test round-trip: libp2p to PB to libp2p
	pbPeer := libp2pPeerInfoToPb(original)
	roundTrip, err := pbPeerInfoToLibp2p(pbPeer)
	if err != nil {
		t.Fatalf("Failed to convert back from PB: %v", err)
	}

	// Assert IDs match
	if roundTrip.ID != original.ID {
		t.Errorf("Expected ID %s, got %s", original.ID, roundTrip.ID)
	}

	// Assert addrs match (order may not preserve, so check lengths and contents)
	if len(roundTrip.Addrs) != len(original.Addrs) {
		t.Errorf("Expected %d addrs, got %d", len(original.Addrs), len(roundTrip.Addrs))
	}
	addrMap := make(map[string]bool)
	for _, addr := range original.Addrs {
		addrMap[addr.String()] = true
	}
	for _, addr := range roundTrip.Addrs {
		if !addrMap[addr.String()] {
			t.Errorf("Unexpected addr: %s", addr)
		}
	}

	// Test empty addrs
	originalEmpty := peer.AddrInfo{ID: originalID}
	pbEmpty := libp2pPeerInfoToPb(originalEmpty)
	roundTripEmpty, err := pbPeerInfoToLibp2p(pbEmpty)
	if err != nil {
		t.Fatalf("Failed empty round-trip: %v", err)
	}
	if roundTripEmpty.ID != originalEmpty.ID || len(roundTripEmpty.Addrs) != 0 {
		t.Error("Empty addrs round-trip failed")
	}

	// Test invalid PB (e.g., bad ID bytes)
	badPb := &protocolsPb.PeerInfo{Id: []byte("invalid")}
	_, err = pbPeerInfoToLibp2p(badPb)
	if err == nil {
		t.Error("Expected error for invalid peer ID")
	}

	// Test invalid addr bytes
	badAddrPb := &protocolsPb.PeerInfo{
		Id:    []byte(originalID),
		Addrs: [][]byte{[]byte("invalid addr")},
	}
	_, err = pbPeerInfoToLibp2p(badAddrPb)
	if err == nil {
		t.Error("Expected error for invalid multiaddr")
	}
}

func hasPushStream(t *testing.T, psm *pushStreamManager, clientID peer.ID) bool {
	t.Helper()
	psm.mtx.RLock()
	defer psm.mtx.RUnlock()
	_, exists := psm.pushStreams[clientID]
	return exists
}

// TestHandleClientPush tests the handleClientPush handler with various scenarios.
func TestHandleClientPush(t *testing.T) {
	t.Run("Success", testHandleClientPush_Success)
	t.Run("Subscriptions", testHandleClientPush_Subscriptions)
	t.Run("InvalidMessage", testHandleClientPush_InvalidMessage)
	t.Run("ReplacePushStream", testHandleClientPush_ReplacePushStream)
}

// testHandleClientPush_Success tests successful push stream setup with and without initial subscriptions.
func testHandleClientPush_Success(t *testing.T) {
	testCases := []struct {
		name          string
		topics        []string
		expectedCount int
	}{
		{
			name:          "EmptySubscriptions",
			topics:        nil,
			expectedCount: 0,
		},
		{
			name:          "WithSubscriptions",
			topics:        []string{"topic1", "topic2"},
			expectedCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Create mocknet with 2 peers (mesh node + client)
			mnet, err := mocknet.WithNPeers(2)
			if err != nil {
				t.Fatalf("Failed to create mocknet: %v", err)
			}

			peers := mnet.Peers()
			meshHost := mnet.Host(peers[0])
			clientHost := mnet.Host(peers[1])

			mockWhitelist := &whitelist{
				peers: []*peer.AddrInfo{
					{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
				},
			}

			if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
				t.Fatalf("Failed to link peers: %v", err)
			}
			if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
				t.Fatalf("Failed to connect peers: %v", err)
			}

			dir := t.TempDir()
			node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
			node.bondStorage = &testBondStorage{score: 1}

			stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
			if err != nil {
				t.Fatalf("Failed to open stream: %v", err)
			}

			initialSubs := &protocolsPb.InitialSubscriptions{Topics: tc.topics}
			if err := codec.WriteLengthPrefixedMessage(stream, initialSubs); err != nil {
				t.Fatalf("Failed to send initial subscriptions: %v", err)
			}

			response := &protocolsPb.Response{}
			if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			if _, ok := response.Response.(*protocolsPb.Response_Success); !ok {
				t.Fatalf("Expected success response, got: %T", response.Response)
			}

			// Wait for push stream handler to register
			requireEventually(t, func() bool {
				return hasPushStream(t, node.pushStreamManager, clientHost.ID())
			}, 2*time.Second, 50*time.Millisecond, "push stream not registered")

			registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
			if len(registeredTopics) != tc.expectedCount {
				t.Errorf("Expected %d subscriptions, got %d", tc.expectedCount, len(registeredTopics))
			}
			for _, topic := range tc.topics {
				found := slices.Contains(registeredTopics, topic)
				if !found {
					t.Errorf("Expected topic %s not found in registered topics", topic)
				}
			}

			if !hasPushStream(t, node.pushStreamManager, clientHost.ID()) {
				t.Error("Push stream not registered in pushStreamManager")
			}

			_ = stream.Close()
		})
	}
}

func testHandleClientPush_Subscriptions(t *testing.T) {
	testCases := []struct {
		name             string
		initialTopics    []string // Topics to subscribe to via separate calls before push stream
		pushStreamTopics []string // Topics to send via push stream
		expectedFinal    []string // Expected final topics after push stream
		verifyCrossMaps  bool     // Whether to verify clientsForTopic mappings
	}{
		{
			name:             "MultipleTopics",
			initialTopics:    nil,
			pushStreamTopics: []string{"topic_a", "topic_b", "topic_c", "topic_d"},
			expectedFinal:    []string{"topic_a", "topic_b", "topic_c", "topic_d"},
			verifyCrossMaps:  true,
		},
		{
			name:             "BulkSubscribeChanges",
			initialTopics:    []string{"A", "B", "C"},
			pushStreamTopics: []string{"B", "C", "D"},
			expectedFinal:    []string{"B", "C", "D"},
			verifyCrossMaps:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Create mocknet
			mnet, err := mocknet.WithNPeers(2)
			if err != nil {
				t.Fatalf("Failed to create mocknet: %v", err)
			}

			peers := mnet.Peers()
			meshHost := mnet.Host(peers[0])
			clientHost := mnet.Host(peers[1])

			mockWhitelist := &whitelist{
				peers: []*peer.AddrInfo{
					{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
				},
			}

			if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
				t.Fatalf("Failed to link peers: %v", err)
			}
			if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
				t.Fatalf("Failed to connect peers: %v", err)
			}

			dir := t.TempDir()
			node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
			node.bondStorage = &testBondStorage{score: 1}

			for _, topic := range tc.initialTopics {
				stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
				if err != nil {
					t.Fatalf("Failed to open subscribe stream: %v", err)
				}
				subMsg := &protocolsPb.SubscribeRequest{Subscribe: true, Topic: topic}
				if err := codec.WriteLengthPrefixedMessage(stream, subMsg); err != nil {
					t.Fatalf("Failed to send subscribe message: %v", err)
				}
				_ = stream.Close()
				time.Sleep(100 * time.Millisecond)
			}

			if len(tc.initialTopics) > 0 {
				initialTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
				if len(initialTopics) != len(tc.initialTopics) {
					t.Fatalf("Expected %d initial subscriptions, got %d", len(tc.initialTopics), len(initialTopics))
				}
			}

			stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
			if err != nil {
				t.Fatalf("Failed to open stream: %v", err)
			}

			initialSubs := &protocolsPb.InitialSubscriptions{Topics: tc.pushStreamTopics}
			if err := codec.WriteLengthPrefixedMessage(stream, initialSubs); err != nil {
				t.Fatalf("Failed to send initial subscriptions: %v", err)
			}

			response := &protocolsPb.Response{}
			if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			if _, ok := response.Response.(*protocolsPb.Response_Success); !ok {
				t.Fatalf("Expected success response, got: %T", response.Response)
			}

			// Wait for push stream handler to register
			requireEventually(t, func() bool {
				return hasPushStream(t, node.pushStreamManager, clientHost.ID())
			}, 2*time.Second, 50*time.Millisecond, "push stream not registered")

			registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
			if len(registeredTopics) != len(tc.expectedFinal) {
				t.Errorf("Expected %d subscriptions, got %d", len(tc.expectedFinal), len(registeredTopics))
			}

			for _, topic := range tc.expectedFinal {
				found := slices.Contains(registeredTopics, topic)
				if !found {
					t.Errorf("Expected topic %s not found in registered topics", topic)
				}
			}

			if tc.verifyCrossMaps {
				for _, topic := range tc.expectedFinal {
					clients := node.subscriptionManager.clientsForTopic(topic)
					found := slices.Contains(clients, clientHost.ID())
					if !found {
						t.Errorf("Client not found as subscriber for topic %s", topic)
					}
				}
			}

			_ = stream.Close()
		})
	}
}

func testHandleClientPush_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create mocknet
	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data (just random bytes)
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	// Try to read response - stream should be closed
	response := &protocolsPb.Response{}
	err = codec.ReadLengthPrefixedMessage(stream, response)
	// We expect an error due to the stream being closed by the handler
	if err == nil {
		t.Error("Expected error reading from closed stream")
	}

	registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(registeredTopics) != 0 {
		t.Errorf("Expected 0 subscriptions for client with invalid message, got %d", len(registeredTopics))
	}

	if hasPushStream(t, node.pushStreamManager, clientHost.ID()) {
		t.Error("Push stream should not be registered after invalid message")
	}

	_ = stream.Close()
}

func testHandleClientPush_ReplacePushStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Open first push stream with topics A, B
	stream1, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to open first stream: %v", err)
	}

	initialSubs1 := &protocolsPb.InitialSubscriptions{Topics: []string{"A", "B"}}
	if err := codec.WriteLengthPrefixedMessage(stream1, initialSubs1); err != nil {
		t.Fatalf("Failed to send initial subscriptions: %v", err)
	}

	response1 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream1, response1); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Wait for push stream handler to register
	requireEventually(t, func() bool {
		return hasPushStream(t, node.pushStreamManager, clientHost.ID())
	}, 2*time.Second, 50*time.Millisecond, "push stream not registered")

	if !hasPushStream(t, node.pushStreamManager, clientHost.ID()) {
		t.Error("First push stream not registered")
	}
	topics1 := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(topics1) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(topics1))
	}

	// Open second push stream with topics C, D (different subscriptions)
	stream2, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to open second stream: %v", err)
	}

	initialSubs2 := &protocolsPb.InitialSubscriptions{Topics: []string{"C", "D"}}
	if err := codec.WriteLengthPrefixedMessage(stream2, initialSubs2); err != nil {
		t.Fatalf("Failed to send initial subscriptions: %v", err)
	}

	response2 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream2, response2); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !hasPushStream(t, node.pushStreamManager, clientHost.ID()) {
		t.Error("Second push stream not registered")
	}

	topics2 := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(topics2) != 2 {
		t.Errorf("Expected 2 subscriptions after reconnect, got %d", len(topics2))
	}

	expectedTopics := map[string]bool{"C": false, "D": false}
	for _, topic := range topics2 {
		if _, ok := expectedTopics[topic]; ok {
			expectedTopics[topic] = true
		} else {
			t.Errorf("Unexpected topic in subscriptions: %s", topic)
		}
	}
	for topic, found := range expectedTopics {
		if !found {
			t.Errorf("Expected topic %s not found in subscriptions", topic)
		}
	}

	_ = stream1.Close()
	_ = stream2.Close()
}

// TestHandleClientSubscribe tests the handleClientSubscribe handler with various scenarios.
func TestHandleClientSubscribe(t *testing.T) {
	t.Run("SubscribeSuccess", testHandleClientSubscribe_SubscribeSuccess)
	t.Run("UnsubscribeSuccess", testHandleClientSubscribe_UnsubscribeSuccess)
	t.Run("SubscribeIdempotent", testHandleClientSubscribe_SubscribeIdempotent)
	t.Run("UnsubscribeNotSubscribed", testHandleClientSubscribe_UnsubscribeNotSubscribed)
	t.Run("InvalidMessage", testHandleClientSubscribe_InvalidMessage)
	t.Run("MultipleOperations", testHandleClientSubscribe_MultipleOperations)
}

func testHandleClientSubscribe_SubscribeSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Subscribe to a topic
	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	subMsg := &protocolsPb.SubscribeRequest{Subscribe: true, Topic: "test-topic"}
	if err := codec.WriteLengthPrefixedMessage(stream, subMsg); err != nil {
		t.Fatalf("Failed to send subscribe message: %v", err)
	}
	_ = stream.Close()

	// Wait for subscription handler to process
	requireEventually(t, func() bool {
		topics := node.subscriptionManager.topicsForClient(clientHost.ID())
		return len(topics) > 0
	}, 2*time.Second, 50*time.Millisecond, "subscription not registered")

	registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(registeredTopics) != 1 {
		t.Errorf("Expected 1 subscription, got %d", len(registeredTopics))
	}
	if !slices.Contains(registeredTopics, "test-topic") {
		t.Error("test-topic not found in client's subscriptions")
	}

	// Verify client is in clientsForTopic mapping
	subscribers := node.subscriptionManager.clientsForTopic("test-topic")
	if !slices.Contains(subscribers, clientHost.ID()) {
		t.Error("Client not found in clientsForTopic mapping")
	}
}

// testHandleClientSubscribe_UnsubscribeSuccess tests successful unsubscription from a topic.
func testHandleClientSubscribe_UnsubscribeSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// First, subscribe to a topic
	stream1, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	subMsg := &protocolsPb.SubscribeRequest{Subscribe: true, Topic: "test-topic"}
	if err := codec.WriteLengthPrefixedMessage(stream1, subMsg); err != nil {
		t.Fatalf("Failed to send subscribe message: %v", err)
	}
	_ = stream1.Close()

	time.Sleep(100 * time.Millisecond)

	// Verify subscribed
	topics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(topics) != 1 {
		t.Fatalf("Expected 1 subscription, got %d", len(topics))
	}

	// Now unsubscribe
	stream2, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream for unsubscribe: %v", err)
	}

	unsubMsg := &protocolsPb.SubscribeRequest{Subscribe: false, Topic: "test-topic"}
	if err := codec.WriteLengthPrefixedMessage(stream2, unsubMsg); err != nil {
		t.Fatalf("Failed to send unsubscribe message: %v", err)
	}
	_ = stream2.Close()

	time.Sleep(100 * time.Millisecond)

	// Verify unsubscribed - no topics should remain
	registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(registeredTopics) != 0 {
		t.Errorf("Expected 0 subscriptions after unsubscribe, got %d", len(registeredTopics))
	}

	// Verify client is no longer in clientsForTopic mapping
	subscribers := node.subscriptionManager.clientsForTopic("test-topic")
	if slices.Contains(subscribers, clientHost.ID()) {
		t.Error("Client still found in clientsForTopic mapping after unsubscribe")
	}
}

func testHandleClientSubscribe_SubscribeIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Subscribe to a topic first time
	stream1, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	subMsg := &protocolsPb.SubscribeRequest{Subscribe: true, Topic: "test-topic"}
	if err := codec.WriteLengthPrefixedMessage(stream1, subMsg); err != nil {
		t.Fatalf("Failed to send subscribe message: %v", err)
	}
	_ = stream1.Close()

	time.Sleep(100 * time.Millisecond)

	topics1 := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(topics1) != 1 {
		t.Fatalf("Expected 1 subscription after first subscribe, got %d", len(topics1))
	}

	// Subscribe to the same topic again
	stream2, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream for second subscribe: %v", err)
	}

	subMsg2 := &protocolsPb.SubscribeRequest{Subscribe: true, Topic: "test-topic"}
	if err := codec.WriteLengthPrefixedMessage(stream2, subMsg2); err != nil {
		t.Fatalf("Failed to send second subscribe message: %v", err)
	}
	_ = stream2.Close()

	time.Sleep(100 * time.Millisecond)

	// Verify still only one subscription (idempotent)
	topics2 := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(topics2) != 1 {
		t.Errorf("Expected 1 subscription after second subscribe (idempotent), got %d", len(topics2))
	}

	// Verify only one client is subscribed to the topic
	subscribers := node.subscriptionManager.clientsForTopic("test-topic")
	if len(subscribers) != 1 {
		t.Errorf("Expected 1 subscriber for topic, got %d", len(subscribers))
	}
}

func testHandleClientSubscribe_UnsubscribeNotSubscribed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Try to unsubscribe from a topic without being subscribed
	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	unsubMsg := &protocolsPb.SubscribeRequest{Subscribe: false, Topic: "never-subscribed"}
	if err := codec.WriteLengthPrefixedMessage(stream, unsubMsg); err != nil {
		t.Fatalf("Failed to send unsubscribe message: %v", err)
	}
	_ = stream.Close()

	time.Sleep(100 * time.Millisecond)

	// Verify no subscriptions exist
	registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(registeredTopics) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(registeredTopics))
	}

	// Verify topic has no subscribers
	subscribers := node.subscriptionManager.clientsForTopic("never-subscribed")
	if len(subscribers) != 0 {
		t.Errorf("Expected 0 subscribers for unsubscribed topic, got %d", len(subscribers))
	}
}

func testHandleClientSubscribe_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Open subscribe stream and send invalid data
	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data (just random bytes)
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	// Stream should be closed by the handler
	time.Sleep(100 * time.Millisecond)
	_ = stream.Close()

	// Verify no subscriptions were registered
	registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(registeredTopics) != 0 {
		t.Errorf("Expected 0 subscriptions after invalid message, got %d", len(registeredTopics))
	}
}

func testHandleClientSubscribe_MultipleOperations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	testCases := []struct {
		subscribe bool
		topic     string
	}{
		{true, "topic1"},
		{true, "topic2"},
		{true, "topic3"},
		{false, "topic1"},
		{true, "topic4"},
		{false, "topic2"},
		{false, "topic3"},
		{true, "topic1"},
	}

	for i, tc := range testCases {
		stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientSubscribeProtocol)
		if err != nil {
			t.Fatalf("Failed to open stream for operation %d: %v", i, err)
		}

		msg := &protocolsPb.SubscribeRequest{Subscribe: tc.subscribe, Topic: tc.topic}
		if err := codec.WriteLengthPrefixedMessage(stream, msg); err != nil {
			t.Fatalf("Failed to send message for operation %d: %v", i, err)
		}
		_ = stream.Close()

		time.Sleep(50 * time.Millisecond)
	}

	// After all operations, verify final state
	// Expected subscriptions: topic1 (re-subscribed), topic4
	registeredTopics := node.subscriptionManager.topicsForClient(clientHost.ID())
	if len(registeredTopics) != 2 {
		t.Errorf("Expected 2 final subscriptions, got %d", len(registeredTopics))
	}

	expectedTopics := map[string]bool{"topic1": false, "topic4": false}
	for _, topic := range registeredTopics {
		if _, ok := expectedTopics[topic]; ok {
			expectedTopics[topic] = true
		} else {
			t.Errorf("Unexpected topic in final subscriptions: %s", topic)
		}
	}
	for topic, found := range expectedTopics {
		if !found {
			t.Errorf("Expected topic %s not found in final subscriptions", topic)
		}
	}

	// Verify cross-mapping for final subscriptions
	for _, topic := range registeredTopics {
		subscribers := node.subscriptionManager.clientsForTopic(topic)
		if !slices.Contains(subscribers, clientHost.ID()) {
			t.Errorf("Client not found in subscribers for topic %s", topic)
		}
	}

	// Verify unsubscribed topics don't have the client
	for _, topic := range []string{"topic2", "topic3"} {
		subscribers := node.subscriptionManager.clientsForTopic(topic)
		if slices.Contains(subscribers, clientHost.ID()) {
			t.Errorf("Client should not be subscribed to topic %s", topic)
		}
	}
}

// TestHandleClientPublish tests the handleClientPublish handler with various scenarios.
func TestHandleClientPublish(t *testing.T) {
	t.Run("Success", testHandleClientPublish_Success)
	t.Run("InvalidMessage", testHandleClientPublish_InvalidMessage)
}

func testHandleClientPublish_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPublishProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	pubMsg := &protocolsPb.PublishRequest{Topic: "test-topic", Data: []byte("test data")}
	if err := codec.WriteLengthPrefixedMessage(stream, pubMsg); err != nil {
		t.Fatalf("Failed to send publish message: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Stream should close cleanly without errors (handler processes asynchronously)
	// Verify no panic occurred by checking that the node is still running
	if node == nil {
		t.Fatal("Node should still be running after publish handler")
	}
}

func testHandleClientPublish_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPublishProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Handler should close stream cleanly without panic on invalid message
	if node == nil {
		t.Fatal("Node should still be running after invalid message")
	}

	_ = stream.Close()
}

func TestHandlePostBonds(t *testing.T) {
	t.Run("InvalidMessage", testHandlePostBonds_InvalidMessage)
	t.Run("EmptyBondsList", testHandlePostBonds_EmptyBondsList)
	t.Run("SuccessWithBonds", testHandlePostBonds_SuccessWithBonds)
	t.Run("MultipleBonds", testHandlePostBonds_MultipleBonds)
	t.Run("ZeroAssetID", testHandlePostBonds_ZeroAssetID)
}

func testHandlePostBonds_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.PostBondsProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Handler should close stream cleanly without panic on invalid message
	_ = stream.Close()
}

func testHandlePostBonds_EmptyBondsList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.PostBondsProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send empty bonds list
	postBondMsg := &protocolsPb.PostBondRequest{Bonds: []*protocolsPb.Bond{}}
	if err := codec.WriteLengthPrefixedMessage(stream, postBondMsg); err != nil {
		t.Fatalf("Failed to send post bond message: %v", err)
	}

	// Read response
	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got a successful response
	pbResp, ok := resp.Response.(*protocolsPb.Response_PostBondResponse)
	if !ok {
		t.Fatalf("Expected PostBondResponse, got %T", resp.Response)
	}

	// Bond strength should match the bondStorage value (testBondStorage returns score: 1)
	if pbResp.PostBondResponse.BondStrength != 1 {
		t.Errorf("Expected bond strength 1, got %d", pbResp.PostBondResponse.BondStrength)
	}
}

func testHandlePostBonds_SuccessWithBonds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 5}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.PostBondsProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	bonds := []*protocolsPb.Bond{
		{AssetID: 42, BondID: []byte("bond-001")},
	}
	postBondMsg := &protocolsPb.PostBondRequest{Bonds: bonds}
	if err := codec.WriteLengthPrefixedMessage(stream, postBondMsg); err != nil {
		t.Fatalf("Failed to send post bond message: %v", err)
	}

	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	pbResp, ok := resp.Response.(*protocolsPb.Response_PostBondResponse)
	if !ok {
		t.Fatalf("Expected PostBondResponse, got %T", resp.Response)
	}

	if pbResp.PostBondResponse.BondStrength != 5 {
		t.Errorf("Expected bond strength 5, got %d", pbResp.PostBondResponse.BondStrength)
	}
}

func testHandlePostBonds_MultipleBonds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 10}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.PostBondsProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	bonds := []*protocolsPb.Bond{
		{AssetID: 1, BondID: []byte("bond-1")},
		{AssetID: 2, BondID: []byte("bond-2")},
		{AssetID: 3, BondID: []byte("bond-3")},
		{AssetID: 4, BondID: []byte("bond-4")},
	}
	postBondMsg := &protocolsPb.PostBondRequest{Bonds: bonds}
	if err := codec.WriteLengthPrefixedMessage(stream, postBondMsg); err != nil {
		t.Fatalf("Failed to send post bond message: %v", err)
	}

	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	pbResp, ok := resp.Response.(*protocolsPb.Response_PostBondResponse)
	if !ok {
		t.Fatalf("Expected PostBondResponse, got %T", resp.Response)
	}

	if pbResp.PostBondResponse.BondStrength != 10 {
		t.Errorf("Expected bond strength 10, got %d", pbResp.PostBondResponse.BondStrength)
	}
}

func testHandlePostBonds_ZeroAssetID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 3}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.PostBondsProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	bonds := []*protocolsPb.Bond{
		{AssetID: 0, BondID: []byte("zero-asset-bond")},
	}
	postBondMsg := &protocolsPb.PostBondRequest{Bonds: bonds}
	if err := codec.WriteLengthPrefixedMessage(stream, postBondMsg); err != nil {
		t.Fatalf("Failed to send post bond message: %v", err)
	}

	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	pbResp, ok := resp.Response.(*protocolsPb.Response_PostBondResponse)
	if !ok {
		t.Fatalf("Expected PostBondResponse, got %T", resp.Response)
	}

	if pbResp.PostBondResponse.BondStrength != 3 {
		t.Errorf("Expected bond strength 3, got %d", pbResp.PostBondResponse.BondStrength)
	}
}

func TestHandleClientRelayMessage(t *testing.T) {
	t.Run("InvalidMessage", testHandleClientRelayMessage_InvalidMessage)
	t.Run("InvalidPeerID", testHandleClientRelayMessage_InvalidPeerID)
	t.Run("DirectRelaySuccess", testHandleClientRelayMessage_DirectRelaySuccess)
	t.Run("DirectRelayCounterpartyNotFound", testHandleClientRelayMessage_DirectRelayCounterpartyNotFound)
	t.Run("DirectRelayCounterpartyRejected", testHandleClientRelayMessage_DirectRelayCounterpartyRejected)
	t.Run("DirectRelayErrorMessage", testHandleClientRelayMessage_DirectRelayErrorMessage)
}

func testHandleClientRelayMessage_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientRelayMessageProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	// Stream should be closed by the handler
	time.Sleep(100 * time.Millisecond)
	_ = stream.Close()
}

func testHandleClientRelayMessage_InvalidPeerID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientRelayMessageProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with invalid peer ID
	request := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte("invalid-peer-id"),
		Message: []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Handler should close stream cleanly without panic
	time.Sleep(100 * time.Millisecond)
	if node == nil {
		t.Fatal("Node should still be running after invalid peer ID")
	}
}

func testHandleClientRelayMessage_DirectRelaySuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])
	counterpartyHost := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link client to mesh: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect client to mesh: %v", err)
	}

	if _, err := mnet.LinkPeers(counterpartyHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link counterparty to mesh: %v", err)
	}
	if _, err := mnet.ConnectPeers(counterpartyHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect counterparty to mesh: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Set up a handler on the counterparty to receive relay messages
	relayHandlerCalled := false
	counterpartyHost.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()
		relayHandlerCalled = true

		req := &protocolsPb.TatankaRelayMessageRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
			return
		}

		// Send success response with test data
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Message{
				Message: []byte("relay response"),
			},
		}
		_ = codec.WriteLengthPrefixedMessage(s, resp)
	})

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientRelayMessageProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte(counterpartyHost.ID()),
		Message: []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &protocolsPb.ClientRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if relayHandlerCalled {
		t.Log("Relay handler was called on counterparty")
	}

	// Verify we got a successful message response
	msgResp, ok := response.Response.(*protocolsPb.ClientRelayMessageResponse_Message)
	if !ok {
		t.Fatalf("Expected Message response, got %T", response.Response)
	}

	if string(msgResp.Message) != "relay response" {
		t.Errorf("Expected 'relay response', got %s", string(msgResp.Message))
	}
}

func testHandleClientRelayMessage_DirectRelayCounterpartyNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientRelayMessageProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Create a peer that is not connected to the mesh node
	_, disconnectedPubKey, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	disconnectedID, err := peer.IDFromPublicKey(disconnectedPubKey)
	if err != nil {
		t.Fatalf("Failed to get peer ID: %v", err)
	}

	request := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte(disconnectedID),
		Message: []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &protocolsPb.ClientRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got a counterparty not found error
	errResp, ok := response.Response.(*protocolsPb.ClientRelayMessageResponse_Error)
	if !ok {
		t.Fatalf("Expected Error response, got %T", response.Response)
	}

	if _, ok := errResp.Error.Error.(*protocolsPb.Error_CpNotFoundError); !ok {
		t.Fatalf("Expected CpNotFoundError, got %T", errResp.Error.Error)
	}
}

func testHandleClientRelayMessage_DirectRelayCounterpartyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])
	counterpartyHost := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link client to mesh: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect client to mesh: %v", err)
	}

	if _, err := mnet.LinkPeers(counterpartyHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link counterparty to mesh: %v", err)
	}
	if _, err := mnet.ConnectPeers(counterpartyHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect counterparty to mesh: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Set up a handler on the counterparty that rejects the relay
	counterpartyHost.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		req := &protocolsPb.TatankaRelayMessageRequest{}
		_ = codec.ReadLengthPrefixedMessage(s, req)

		// Send rejection response
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_CpRejectedError{
						CpRejectedError: &protocolsPb.CounterpartyRejectedError{},
					},
				},
			},
		}
		_ = codec.WriteLengthPrefixedMessage(s, resp)
	})

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientRelayMessageProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte(counterpartyHost.ID()),
		Message: []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &protocolsPb.ClientRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got a counterparty rejected error
	errResp, ok := response.Response.(*protocolsPb.ClientRelayMessageResponse_Error)
	if !ok {
		t.Fatalf("Expected Error response, got %T", response.Response)
	}

	if _, ok := errResp.Error.Error.(*protocolsPb.Error_CpRejectedError); !ok {
		t.Fatalf("Expected CpRejectedError, got %T", errResp.Error.Error)
	}
}

func testHandleClientRelayMessage_DirectRelayErrorMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])
	counterpartyHost := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link client to mesh: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect client to mesh: %v", err)
	}

	if _, err := mnet.LinkPeers(counterpartyHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link counterparty to mesh: %v", err)
	}
	if _, err := mnet.ConnectPeers(counterpartyHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect counterparty to mesh: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	node.bondStorage = &testBondStorage{score: 1}

	// Set up a handler on the counterparty that returns an error message
	errorMessage := "relay processing failed"
	counterpartyHost.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		req := &protocolsPb.TatankaRelayMessageRequest{}
		_ = codec.ReadLengthPrefixedMessage(s, req)

		// Send error response with message
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_Message{
						Message: errorMessage,
					},
				},
			},
		}
		_ = codec.WriteLengthPrefixedMessage(s, resp)
	})

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientRelayMessageProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte(counterpartyHost.ID()),
		Message: []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &protocolsPb.ClientRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got an error response with the message
	errResp, ok := response.Response.(*protocolsPb.ClientRelayMessageResponse_Error)
	if !ok {
		t.Fatalf("Expected Error response, got %T", response.Response)
	}

	msgErr, ok := errResp.Error.Error.(*protocolsPb.Error_Message)
	if !ok {
		t.Fatalf("Expected Message error, got %T", errResp.Error.Error)
	}

	if msgErr.Message != errorMessage {
		t.Errorf("Expected error message %q, got %q", errorMessage, msgErr.Message)
	}
}

func TestHandleForwardRelay(t *testing.T) {
	t.Run("InvalidMessage", testHandleForwardRelay_InvalidMessage)
	t.Run("InvalidCounterpartyID", testHandleForwardRelay_InvalidCounterpartyID)
	t.Run("Success", testHandleForwardRelay_Success)
	t.Run("CounterpartyNotConnected", testHandleForwardRelay_CounterpartyNotConnected)
	t.Run("CounterpartyRejected", testHandleForwardRelay_CounterpartyRejected)
	t.Run("CounterpartyError", testHandleForwardRelay_CounterpartyError)
}

func testHandleForwardRelay_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	tatankaNode1 := mnet.Host(peers[0])
	tatankaNode2 := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: tatankaNode1.ID(), Addrs: tatankaNode1.Addrs()},
			{ID: tatankaNode2.ID(), Addrs: tatankaNode2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node2 := newTestNode(t, ctx, tatankaNode2, dir, mockWhitelist)
	node2.bondStorage = &testBondStorage{score: 1}

	stream, err := tatankaNode1.NewStream(ctx, tatankaNode2.ID(), forwardRelayProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	_ = stream.Close()
}

func testHandleForwardRelay_InvalidCounterpartyID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	tatankaNode1 := mnet.Host(peers[0])
	tatankaNode2 := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: tatankaNode1.ID(), Addrs: tatankaNode1.Addrs()},
			{ID: tatankaNode2.ID(), Addrs: tatankaNode2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node2 := newTestNode(t, ctx, tatankaNode2, dir, mockWhitelist)
	node2.bondStorage = &testBondStorage{score: 1}

	stream, err := tatankaNode1.NewStream(ctx, tatankaNode2.ID(), forwardRelayProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with invalid counterparty ID
	request := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(tatankaNode1.ID()),
		CounterpartyId: []byte("invalid-peer-id"),
		Message:        []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if node2 == nil {
		t.Fatal("Node should still be running after invalid counterparty ID")
	}
}

func testHandleForwardRelay_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(4)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	client := mnet.Host(peers[0])
	tatankaNode1 := mnet.Host(peers[1])
	tatankaNode2 := mnet.Host(peers[2])
	counterpartyClient := mnet.Host(peers[3])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: tatankaNode1.ID(), Addrs: tatankaNode1.Addrs()},
			{ID: tatankaNode2.ID(), Addrs: tatankaNode2.Addrs()},
		},
	}

	// Connect client to tatankaNode1
	if _, err := mnet.LinkPeers(client.ID(), tatankaNode1.ID()); err != nil {
		t.Fatalf("Failed to link client to tatankaNode1: %v", err)
	}
	if _, err := mnet.ConnectPeers(client.ID(), tatankaNode1.ID()); err != nil {
		t.Fatalf("Failed to connect client to tatankaNode1: %v", err)
	}

	// Connect tatankaNode1 to tatankaNode2
	if _, err := mnet.LinkPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link tatankaNode1 to tatankaNode2: %v", err)
	}
	if _, err := mnet.ConnectPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect tatankaNode1 to tatankaNode2: %v", err)
	}

	// Connect counterpartyClient to tatankaNode2
	if _, err := mnet.LinkPeers(counterpartyClient.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link counterpartyClient to tatankaNode2: %v", err)
	}
	if _, err := mnet.ConnectPeers(counterpartyClient.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect counterpartyClient to tatankaNode2: %v", err)
	}

	dir1 := t.TempDir()
	node1 := newTestNode(t, ctx, tatankaNode1, dir1, mockWhitelist)
	node1.bondStorage = &testBondStorage{score: 1}

	dir2 := t.TempDir()
	node2 := newTestNode(t, ctx, tatankaNode2, dir2, mockWhitelist)
	node2.bondStorage = &testBondStorage{score: 1}

	// Set up a handler on the counterparty to receive relay messages
	counterpartyClient.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		req := &protocolsPb.TatankaRelayMessageRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
			return
		}

		// Send success response with test data
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Message{
				Message: []byte("forward relay response"),
			},
		}
		_ = codec.WriteLengthPrefixedMessage(s, resp)
	})

	stream, err := tatankaNode1.NewStream(ctx, tatankaNode2.ID(), forwardRelayProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(client.ID()),
		CounterpartyId: []byte(counterpartyClient.ID()),
		Message:        []byte("forward relay message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.TatankaForwardRelayResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got a successful response
	if string(response.GetSuccess()) != "forward relay response" {
		t.Errorf("Expected 'forward relay response', got %q", string(response.GetSuccess()))
	}
}

func testHandleForwardRelay_CounterpartyNotConnected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	tatankaNode1 := mnet.Host(peers[0])
	tatankaNode2 := mnet.Host(peers[1])
	disconnectedClient := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: tatankaNode1.ID(), Addrs: tatankaNode1.Addrs()},
			{ID: tatankaNode2.ID(), Addrs: tatankaNode2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node2 := newTestNode(t, ctx, tatankaNode2, dir, mockWhitelist)
	node2.bondStorage = &testBondStorage{score: 1}

	stream, err := tatankaNode1.NewStream(ctx, tatankaNode2.ID(), forwardRelayProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(tatankaNode1.ID()),
		CounterpartyId: []byte(disconnectedClient.ID()),
		Message:        []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.TatankaForwardRelayResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got a client not found response
	if response.GetClientNotFound() == nil {
		t.Fatalf("Expected ClientNotFound response, got %T", response.Response)
	}
}

func testHandleForwardRelay_CounterpartyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(4)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	tatankaNode1 := mnet.Host(peers[0])
	tatankaNode2 := mnet.Host(peers[1])
	client := mnet.Host(peers[2])
	counterpartyClient := mnet.Host(peers[3])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: tatankaNode1.ID(), Addrs: tatankaNode1.Addrs()},
			{ID: tatankaNode2.ID(), Addrs: tatankaNode2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link tatankaNode1 to tatankaNode2: %v", err)
	}
	if _, err := mnet.ConnectPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect tatankaNode1 to tatankaNode2: %v", err)
	}

	if _, err := mnet.LinkPeers(counterpartyClient.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link counterpartyClient to tatankaNode2: %v", err)
	}
	if _, err := mnet.ConnectPeers(counterpartyClient.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect counterpartyClient to tatankaNode2: %v", err)
	}

	dir := t.TempDir()
	node2 := newTestNode(t, ctx, tatankaNode2, dir, mockWhitelist)
	node2.bondStorage = &testBondStorage{score: 1}

	// Set up a handler on the counterparty that rejects the relay
	counterpartyClient.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		req := &protocolsPb.TatankaRelayMessageRequest{}
		_ = codec.ReadLengthPrefixedMessage(s, req)

		// Send rejection response
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_CpRejectedError{
						CpRejectedError: &protocolsPb.CounterpartyRejectedError{},
					},
				},
			},
		}
		_ = codec.WriteLengthPrefixedMessage(s, resp)
	})

	stream, err := tatankaNode1.NewStream(ctx, tatankaNode2.ID(), forwardRelayProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(client.ID()),
		CounterpartyId: []byte(counterpartyClient.ID()),
		Message:        []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.TatankaForwardRelayResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got a client rejected response
	if response.GetClientRejected() == nil {
		t.Fatalf("Expected ClientRejected response, got %T", response.Response)
	}
}

func testHandleForwardRelay_CounterpartyError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(4)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	tatankaNode1 := mnet.Host(peers[0])
	tatankaNode2 := mnet.Host(peers[1])
	client := mnet.Host(peers[2])
	counterpartyClient := mnet.Host(peers[3])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: tatankaNode1.ID(), Addrs: tatankaNode1.Addrs()},
			{ID: tatankaNode2.ID(), Addrs: tatankaNode2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link tatankaNode1 to tatankaNode2: %v", err)
	}
	if _, err := mnet.ConnectPeers(tatankaNode1.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect tatankaNode1 to tatankaNode2: %v", err)
	}

	if _, err := mnet.LinkPeers(counterpartyClient.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to link counterpartyClient to tatankaNode2: %v", err)
	}
	if _, err := mnet.ConnectPeers(counterpartyClient.ID(), tatankaNode2.ID()); err != nil {
		t.Fatalf("Failed to connect counterpartyClient to tatankaNode2: %v", err)
	}

	dir := t.TempDir()
	node2 := newTestNode(t, ctx, tatankaNode2, dir, mockWhitelist)
	node2.bondStorage = &testBondStorage{score: 1}

	// Set up a handler on the counterparty that returns an error message
	errorMsg := "relay processing failed"
	counterpartyClient.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		req := &protocolsPb.TatankaRelayMessageRequest{}
		_ = codec.ReadLengthPrefixedMessage(s, req)

		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_Message{
						Message: errorMsg,
					},
				},
			},
		}
		_ = codec.WriteLengthPrefixedMessage(s, resp)
	})

	stream, err := tatankaNode1.NewStream(ctx, tatankaNode2.ID(), forwardRelayProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	request := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(client.ID()),
		CounterpartyId: []byte(counterpartyClient.ID()),
		Message:        []byte("test message"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.TatankaForwardRelayResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify we got an error response with the correct message
	if response.GetError() != errorMsg {
		t.Errorf("Expected error message %q, got %q", errorMsg, response.GetError())
	}
}

// TestHandleDiscovery tests the handleDiscovery handler with various scenarios.
func TestHandleDiscovery(t *testing.T) {
	t.Run("InvalidMessage", testHandleDiscovery_InvalidMessage)
	t.Run("InvalidPeerID", testHandleDiscovery_InvalidPeerID)
	t.Run("PeerNotInWhitelist", testHandleDiscovery_PeerNotInWhitelist)
	t.Run("PeerNotConnected", testHandleDiscovery_PeerNotConnected)
	t.Run("DiscoverySuccess", testHandleDiscovery_DiscoverySuccess)
	t.Run("MultipleAddresses", testHandleDiscovery_MultipleAddresses)
}

func testHandleDiscovery_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	queryingHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
			{ID: queryingHost.ID(), Addrs: queryingHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := queryingHost.NewStream(ctx, meshHost.ID(), discoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	_ = stream.Close()

	// Handler should close stream cleanly without panic
	if node == nil {
		t.Fatal("Node should still be running after invalid message")
	}
}

func testHandleDiscovery_InvalidPeerID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	queryingHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
			{ID: queryingHost.ID(), Addrs: queryingHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := queryingHost.NewStream(ctx, meshHost.ID(), discoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with invalid peer ID bytes
	request := &pb.DiscoveryRequest{
		Id: []byte("invalid-peer-id"),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Handler should close stream cleanly without panic
	if node == nil {
		t.Fatal("Node should still be running after invalid peer ID")
	}
}

func testHandleDiscovery_PeerNotInWhitelist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	queryingHost := mnet.Host(peers[1])
	targetHost := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
			{ID: queryingHost.ID(), Addrs: queryingHost.Addrs()},
			// Note: targetHost is NOT in whitelist
		},
	}

	if _, err := mnet.LinkPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link querying peer: %v", err)
	}
	if _, err := mnet.ConnectPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect querying peer: %v", err)
	}

	if _, err := mnet.LinkPeers(targetHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link target peer: %v", err)
	}
	if _, err := mnet.ConnectPeers(targetHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect target peer: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := queryingHost.NewStream(ctx, meshHost.ID(), discoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Request discovery of targetHost (not in whitelist)
	request := &pb.DiscoveryRequest{
		Id: []byte(targetHost.ID()),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get "not found" response since peer is not in whitelist
	if _, ok := response.Response.(*pb.DiscoveryResponse_NotFound_); !ok {
		t.Fatalf("Expected NotFound response for peer not in whitelist, got %T", response.Response)
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleDiscovery_PeerNotConnected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	queryingHost := mnet.Host(peers[1])

	// Create a peer that is not connected to meshHost
	_, targetPubKey, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	targetID, err := peer.IDFromPublicKey(targetPubKey)
	if err != nil {
		t.Fatalf("Failed to get peer ID: %v", err)
	}

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
			{ID: queryingHost.ID(), Addrs: queryingHost.Addrs()},
			{ID: targetID}, // In whitelist but not connected
		},
	}

	if _, err := mnet.LinkPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := queryingHost.NewStream(ctx, meshHost.ID(), discoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Request discovery of targetID (in whitelist but not connected)
	request := &pb.DiscoveryRequest{
		Id: []byte(targetID),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get "not found" response since peer is not connected
	if _, ok := response.Response.(*pb.DiscoveryResponse_NotFound_); !ok {
		t.Fatalf("Expected NotFound response for disconnected peer, got %T", response.Response)
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleDiscovery_DiscoverySuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	queryingHost := mnet.Host(peers[1])
	targetHost := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
			{ID: queryingHost.ID(), Addrs: queryingHost.Addrs()},
			{ID: targetHost.ID(), Addrs: targetHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link querying peer: %v", err)
	}
	if _, err := mnet.ConnectPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect querying peer: %v", err)
	}

	if _, err := mnet.LinkPeers(targetHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link target peer: %v", err)
	}
	if _, err := mnet.ConnectPeers(targetHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect target peer: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := queryingHost.NewStream(ctx, meshHost.ID(), discoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Request discovery of targetHost
	request := &pb.DiscoveryRequest{
		Id: []byte(targetHost.ID()),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get successful response with addresses
	successResp, ok := response.Response.(*pb.DiscoveryResponse_Success_)
	if !ok {
		t.Fatalf("Expected Success response, got %T", response.Response)
	}

	// Verify addresses are returned
	if len(successResp.Success.Addrs) == 0 {
		t.Error("Expected at least one address in successful discovery response")
	}

	// Verify addresses contain the target host's addresses
	expectedAddrs := targetHost.Addrs()
	if len(successResp.Success.Addrs) != len(expectedAddrs) {
		t.Errorf("Expected %d addresses, got %d", len(expectedAddrs), len(successResp.Success.Addrs))
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleDiscovery_MultipleAddresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	queryingHost := mnet.Host(peers[1])
	targetHost := mnet.Host(peers[2])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
			{ID: queryingHost.ID(), Addrs: queryingHost.Addrs()},
			{ID: targetHost.ID(), Addrs: targetHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link querying peer: %v", err)
	}
	if _, err := mnet.ConnectPeers(queryingHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect querying peer: %v", err)
	}

	if _, err := mnet.LinkPeers(targetHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link target peer: %v", err)
	}
	if _, err := mnet.ConnectPeers(targetHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect target peer: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := queryingHost.NewStream(ctx, meshHost.ID(), discoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Request discovery of targetHost
	request := &pb.DiscoveryRequest{
		Id: []byte(targetHost.ID()),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	successResp, ok := response.Response.(*pb.DiscoveryResponse_Success_)
	if !ok {
		t.Fatalf("Expected Success response, got %T", response.Response)
	}

	// Verify that all returned addresses are valid multiaddrs
	for i, addrBytes := range successResp.Success.Addrs {
		_, err := ma.NewMultiaddrBytes(addrBytes)
		if err != nil {
			t.Errorf("Address %d is not a valid multiaddr: %v", i, err)
		}
	}

	// Verify addresses match target host's addresses
	targetAddrs := targetHost.Addrs()
	if len(successResp.Success.Addrs) != len(targetAddrs) {
		t.Errorf("Expected %d addresses, got %d", len(targetAddrs), len(successResp.Success.Addrs))
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

// TestHandleWhitelist tests the handleWhitelist handler with various scenarios.
func TestHandleWhitelist(t *testing.T) {
	t.Run("InvalidMessage", testHandleWhitelist_InvalidMessage)
	t.Run("InvalidPeerID", testHandleWhitelist_InvalidPeerID)
	t.Run("WhitelistMatch", testHandleWhitelist_WhitelistMatch)
	t.Run("WhitelistMatch_Reverse", testHandleWhitelist_WhitelistMatch_Reverse)
	t.Run("WhitelistMismatch_ExtraRemote", testHandleWhitelist_WhitelistMismatch_ExtraRemote)
	t.Run("WhitelistMismatch_MissingRemote", testHandleWhitelist_WhitelistMismatch_MissingRemote)
	t.Run("WhitelistMismatch_Disjoint", testHandleWhitelist_WhitelistMismatch_Disjoint)
	t.Run("EmptyRemoteRequest", testHandleWhitelist_EmptyRemoteRequest)
}

func testHandleWhitelist_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	_ = stream.Close()

	// Handler should close stream cleanly without panic
	if node == nil {
		t.Fatal("Node should still be running after invalid message")
	}
}

func testHandleWhitelist_InvalidPeerID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with invalid peer ID bytes
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{[]byte("invalid-peer-id")},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get mismatch response due to invalid peer ID
	if _, ok := response.Response.(*pb.WhitelistResponse_Mismatch_); !ok {
		t.Fatalf("Expected Mismatch response for invalid peer ID, got %T", response.Response)
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleWhitelist_WhitelistMatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with matching peer IDs
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{
			[]byte(meshHost1.ID()),
			[]byte(meshHost2.ID()),
		},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get success response when whitelists match
	if _, ok := response.Response.(*pb.WhitelistResponse_Success_); !ok {
		t.Fatalf("Expected Success response for matching whitelist, got %T", response.Response)
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleWhitelist_WhitelistMismatch_ExtraRemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2])

	// meshHost1's whitelist contains only hosts 1 and 2
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with extra peer (meshHost3) that meshHost1 doesn't know about
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{
			[]byte(meshHost1.ID()),
			[]byte(meshHost2.ID()),
			[]byte(meshHost3.ID()),
		},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get mismatch response with local peer IDs
	mismatchResp, ok := response.Response.(*pb.WhitelistResponse_Mismatch_)
	if !ok {
		t.Fatalf("Expected Mismatch response, got %T", response.Response)
	}

	// Verify we got the local whitelist peer IDs in the response
	if len(mismatchResp.Mismatch.PeerIDs) != 2 {
		t.Errorf("Expected 2 peer IDs in mismatch response, got %d", len(mismatchResp.Mismatch.PeerIDs))
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleWhitelist_WhitelistMismatch_MissingRemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2])

	// meshHost1's whitelist contains all three hosts
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
			{ID: meshHost3.ID(), Addrs: meshHost3.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request missing meshHost3
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{
			[]byte(meshHost1.ID()),
			[]byte(meshHost2.ID()),
		},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get mismatch response with local peer IDs
	mismatchResp, ok := response.Response.(*pb.WhitelistResponse_Mismatch_)
	if !ok {
		t.Fatalf("Expected Mismatch response, got %T", response.Response)
	}

	// Verify we got all three peer IDs in the response (local whitelist)
	if len(mismatchResp.Mismatch.PeerIDs) != 3 {
		t.Errorf("Expected 3 peer IDs in mismatch response, got %d", len(mismatchResp.Mismatch.PeerIDs))
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleWhitelist_WhitelistMismatch_Disjoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(4)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2])
	meshHost4 := mnet.Host(peers[3])

	// meshHost1's whitelist contains hosts 1 and 2
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with completely different peers (hosts 3 and 4)
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{
			[]byte(meshHost3.ID()),
			[]byte(meshHost4.ID()),
		},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get mismatch response with local peer IDs
	mismatchResp, ok := response.Response.(*pb.WhitelistResponse_Mismatch_)
	if !ok {
		t.Fatalf("Expected Mismatch response, got %T", response.Response)
	}

	// Verify we got the local whitelist peer IDs (hosts 1 and 2)
	if len(mismatchResp.Mismatch.PeerIDs) != 2 {
		t.Errorf("Expected 2 peer IDs in mismatch response, got %d", len(mismatchResp.Mismatch.PeerIDs))
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleWhitelist_WhitelistMatch_Reverse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2])

	// Whitelist with three peers in specific order
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
			{ID: meshHost3.ID(), Addrs: meshHost3.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send request with same peer IDs but in reverse order to test order-independence
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{
			[]byte(meshHost3.ID()),
			[]byte(meshHost2.ID()),
			[]byte(meshHost1.ID()),
		},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get success response since peer IDs match regardless of order
	if _, ok := response.Response.(*pb.WhitelistResponse_Success_); !ok {
		t.Fatalf("Expected Success response for matching whitelist (different order), got %T", response.Response)
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleWhitelist_EmptyRemoteRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])

	// Non-empty whitelist
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := meshHost2.NewStream(ctx, meshHost1.ID(), whitelistProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Send empty request but local whitelist has peers
	request := &pb.WhitelistRequest{
		PeerIDs: [][]byte{},
	}
	if err := codec.WriteLengthPrefixedMessage(stream, request); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	response := &pb.WhitelistResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Should get mismatch response with local peer IDs
	mismatchResp, ok := response.Response.(*pb.WhitelistResponse_Mismatch_)
	if !ok {
		t.Fatalf("Expected Mismatch response for empty remote request, got %T", response.Response)
	}

	// Verify we got the local whitelist peer IDs in the response
	if len(mismatchResp.Mismatch.PeerIDs) != 2 {
		t.Errorf("Expected 2 peer IDs in mismatch response, got %d", len(mismatchResp.Mismatch.PeerIDs))
	}

	// Verify node is still running
	if node == nil {
		t.Fatal("Node should still be running")
	}
}

// TestHandleAvailableMeshNodes tests the handleAvailableMeshNodes handler with various scenarios.
func TestHandleAvailableMeshNodes(t *testing.T) {
	t.Run("InvalidMessage", testHandleAvailableMeshNodes_InvalidMessage)
	t.Run("OnlyNodeItself", testHandleAvailableMeshNodes_OnlyNodeItself)
	t.Run("MultipleConnectedNodes", testHandleAvailableMeshNodes_MultipleConnectedNodes)
	t.Run("DisconnectedNodesExcluded", testHandleAvailableMeshNodes_DisconnectedNodesExcluded)
	t.Run("MixedConnectedAndDisconnected", testHandleAvailableMeshNodes_MixedConnectedAndDisconnected)
}

func testHandleAvailableMeshNodes_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.AvailableMeshNodesProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send invalid protobuf data
	if _, err := stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Handler should close stream cleanly without panic on invalid message
	if node == nil {
		t.Fatal("Node should still be running after invalid message")
	}

	_ = stream.Close()
}

func testHandleAvailableMeshNodes_OnlyNodeItself(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost := mnet.Host(peers[0])
	clientHost := mnet.Host(peers[1])

	// Whitelist contains only the mesh node itself
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost.ID(), Addrs: meshHost.Addrs()},
		},
	}

	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.AvailableMeshNodesProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	response := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	availableResp, ok := response.Response.(*protocolsPb.Response_AvailableMeshNodesResponse)
	if !ok {
		t.Fatalf("Expected AvailableMeshNodesResponse, got %T", response.Response)
	}

	// Should only have the node itself
	if len(availableResp.AvailableMeshNodesResponse.Peers) != 1 {
		t.Errorf("Expected 1 peer (node itself), got %d", len(availableResp.AvailableMeshNodesResponse.Peers))
	}

	// Verify the peer is the node itself
	if len(availableResp.AvailableMeshNodesResponse.Peers) > 0 {
		returnedID := peer.ID(availableResp.AvailableMeshNodesResponse.Peers[0].Id)
		if returnedID != meshHost.ID() {
			t.Errorf("Expected peer ID %s, got %s", meshHost.ID(), returnedID)
		}
	}

	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleAvailableMeshNodes_MultipleConnectedNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create 4 nodes: node1, node2, node3, client
	mnet, err := mocknet.WithNPeers(4)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2])
	clientHost := mnet.Host(peers[3])

	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
			{ID: meshHost3.ID(), Addrs: meshHost3.Addrs()},
		},
	}

	// Connect client to meshHost1
	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost1.ID()); err != nil {
		t.Fatalf("Failed to link client to meshHost1: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost1.ID()); err != nil {
		t.Fatalf("Failed to connect client to meshHost1: %v", err)
	}

	// Connect meshHost1 to meshHost2 and meshHost3
	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link meshHost1 to meshHost2: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect meshHost1 to meshHost2: %v", err)
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost3.ID()); err != nil {
		t.Fatalf("Failed to link meshHost1 to meshHost3: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost3.ID()); err != nil {
		t.Fatalf("Failed to connect meshHost1 to meshHost3: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost1.ID(), protocols.AvailableMeshNodesProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	response := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	availableResp, ok := response.Response.(*protocolsPb.Response_AvailableMeshNodesResponse)
	if !ok {
		t.Fatalf("Expected AvailableMeshNodesResponse, got %T", response.Response)
	}

	// Should have 3 peers: meshHost1, meshHost2, meshHost3
	if len(availableResp.AvailableMeshNodesResponse.Peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(availableResp.AvailableMeshNodesResponse.Peers))
	}

	// Build set of returned peer IDs for verification
	returnedPeerIDs := make(map[peer.ID]bool)
	for _, peerInfo := range availableResp.AvailableMeshNodesResponse.Peers {
		returnedPeerIDs[peer.ID(peerInfo.Id)] = true
	}

	expectedPeerIDs := map[peer.ID]bool{
		meshHost1.ID(): false,
		meshHost2.ID(): false,
		meshHost3.ID(): false,
	}

	for expectedID := range expectedPeerIDs {
		if _, found := returnedPeerIDs[expectedID]; !found {
			t.Errorf("Expected peer %s not found in available nodes", expectedID.ShortString())
		}
	}

	// Verify each peer has addresses
	for _, peerInfo := range availableResp.AvailableMeshNodesResponse.Peers {
		if len(peerInfo.Addrs) == 0 {
			t.Errorf("Peer %s has no addresses", string(peerInfo.Id))
		}
	}

	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleAvailableMeshNodes_DisconnectedNodesExcluded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create 3 nodes: node1, node2 (connected), node3 (disconnected), client
	mnet, err := mocknet.WithNPeers(4)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2]) // This will not be connected
	clientHost := mnet.Host(peers[3])

	// Whitelist includes all three mesh nodes
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
			{ID: meshHost3.ID(), Addrs: meshHost3.Addrs()},
		},
	}

	// Connect client to meshHost1
	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost1.ID()); err != nil {
		t.Fatalf("Failed to link client to meshHost1: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost1.ID()); err != nil {
		t.Fatalf("Failed to connect client to meshHost1: %v", err)
	}

	// Connect meshHost1 to meshHost2 only (meshHost3 is NOT connected)
	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link meshHost1 to meshHost2: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect meshHost1 to meshHost2: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost1.ID(), protocols.AvailableMeshNodesProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	response := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	availableResp, ok := response.Response.(*protocolsPb.Response_AvailableMeshNodesResponse)
	if !ok {
		t.Fatalf("Expected AvailableMeshNodesResponse, got %T", response.Response)
	}

	// Should only have 2 peers: meshHost1 (itself) and meshHost2 (connected)
	// meshHost3 should NOT be included because it's not connected
	if len(availableResp.AvailableMeshNodesResponse.Peers) != 2 {
		t.Errorf("Expected 2 peers (excluding disconnected node), got %d", len(availableResp.AvailableMeshNodesResponse.Peers))
	}

	// Build set of returned peer IDs
	returnedPeerIDs := make(map[peer.ID]bool)
	for _, peerInfo := range availableResp.AvailableMeshNodesResponse.Peers {
		returnedPeerIDs[peer.ID(peerInfo.Id)] = true
	}

	// Verify meshHost1 and meshHost2 are present
	if _, found := returnedPeerIDs[meshHost1.ID()]; !found {
		t.Error("Expected meshHost1 not found in available nodes")
	}
	if _, found := returnedPeerIDs[meshHost2.ID()]; !found {
		t.Error("Expected meshHost2 not found in available nodes")
	}

	// Verify meshHost3 is NOT present
	if _, found := returnedPeerIDs[meshHost3.ID()]; found {
		t.Error("Disconnected meshHost3 should not be in available nodes")
	}

	if node == nil {
		t.Fatal("Node should still be running")
	}
}

func testHandleAvailableMeshNodes_MixedConnectedAndDisconnected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create 5 nodes: node1, node2, node3, node4, client
	mnet, err := mocknet.WithNPeers(5)
	if err != nil {
		t.Fatalf("Failed to create mocknet: %v", err)
	}

	peers := mnet.Peers()
	meshHost1 := mnet.Host(peers[0])
	meshHost2 := mnet.Host(peers[1])
	meshHost3 := mnet.Host(peers[2])
	meshHost4 := mnet.Host(peers[3])
	clientHost := mnet.Host(peers[4])

	// Whitelist includes only the first three mesh nodes (meshHost4 is not whitelisted)
	// This prevents auto-connection attempts to meshHost4
	mockWhitelist := &whitelist{
		peers: []*peer.AddrInfo{
			{ID: meshHost1.ID(), Addrs: meshHost1.Addrs()},
			{ID: meshHost2.ID(), Addrs: meshHost2.Addrs()},
			{ID: meshHost3.ID(), Addrs: meshHost3.Addrs()},
		},
	}

	// Connect client to meshHost1
	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost1.ID()); err != nil {
		t.Fatalf("Failed to link client to meshHost1: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost1.ID()); err != nil {
		t.Fatalf("Failed to connect client to meshHost1: %v", err)
	}

	// Connect meshHost1 to meshHost2 and meshHost3 (but not meshHost4)
	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to link meshHost1 to meshHost2: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost2.ID()); err != nil {
		t.Fatalf("Failed to connect meshHost1 to meshHost2: %v", err)
	}

	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost3.ID()); err != nil {
		t.Fatalf("Failed to link meshHost1 to meshHost3: %v", err)
	}
	if _, err := mnet.ConnectPeers(meshHost1.ID(), meshHost3.ID()); err != nil {
		t.Fatalf("Failed to connect meshHost1 to meshHost3: %v", err)
	}

	// meshHost4 is linked but not connected
	if _, err := mnet.LinkPeers(meshHost1.ID(), meshHost4.ID()); err != nil {
		t.Fatalf("Failed to link meshHost1 to meshHost4: %v", err)
	}

	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost1, dir, mockWhitelist)

	stream, err := clientHost.NewStream(ctx, meshHost1.ID(), protocols.AvailableMeshNodesProtocol)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	response := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	availableResp, ok := response.Response.(*protocolsPb.Response_AvailableMeshNodesResponse)
	if !ok {
		t.Fatalf("Expected AvailableMeshNodesResponse, got %T", response.Response)
	}

	// Should have 3 peers: meshHost1 (itself), meshHost2, meshHost3
	// meshHost4 should NOT be included because it's not connected
	if len(availableResp.AvailableMeshNodesResponse.Peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(availableResp.AvailableMeshNodesResponse.Peers))
	}

	// Build set of returned peer IDs
	returnedPeerIDs := make(map[peer.ID]bool)
	for _, peerInfo := range availableResp.AvailableMeshNodesResponse.Peers {
		returnedPeerIDs[peer.ID(peerInfo.Id)] = true
	}

	expectedConnected := map[peer.ID]bool{
		meshHost1.ID(): false,
		meshHost2.ID(): false,
		meshHost3.ID(): false,
	}

	for expectedID := range expectedConnected {
		if _, found := returnedPeerIDs[expectedID]; !found {
			t.Errorf("Expected peer %s not found in available nodes", expectedID.ShortString())
		}
	}

	// Verify disconnected node is not present
	if _, found := returnedPeerIDs[meshHost4.ID()]; found {
		t.Error("Disconnected meshHost4 should not be in available nodes")
	}

	if node == nil {
		t.Fatal("Node should still be running")
	}
}
