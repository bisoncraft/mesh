package tatanka

import (
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
)

// TestPushPermissions tests the permissions for the push protocol.
func TestPushPermissions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a mock network with 2 peers (1 mesh node, 1 client)
	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatal(err)
	}

	allPeers := mnet.Peers()
	meshHost := mnet.Host(allPeers[0])
	clientHost := mnet.Host(allPeers[1])

	// Create a manifest with just the mesh node
	mockManifest := &manifest{
		bootstrapPeers:    []*peer.AddrInfo{{ID: meshHost.ID(), Addrs: meshHost.Addrs()}},
		nonBootstrapPeers: []peer.ID{},
	}

	// Create the test node with bondStorage initially set to score 0
	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockManifest)
	testBondStorage := &testBondStorage{score: 0}
	node.bondStorage = testBondStorage

	// Link and connect the client to the mesh node
	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link client to node: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect client to node: %v", err)
	}

	time.Sleep(time.Second)

	// With bond score 0, push protocol should return unauthorized error
	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Read the response
	if err := codec.SetReadDeadline(codec.ReadTimeout, stream); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
	buf := bufio.NewReader(stream)
	response1 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(buf, response1); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify it's an unauthorized error
	if errResp, ok := response1.Response.(*protocolsPb.Response_Error); ok {
		if _, isUnauthorized := errResp.Error.Error.(*protocolsPb.Error_Unauthorized); !isUnauthorized {
			t.Fatalf("Expected unauthorized error, got different error type")
		}
	} else {
		t.Fatalf("Expected error response, got: %T", response1.Response)
	}

	_ = stream.Close()

	// Update bond score to 1, push protocol should now succeed
	testBondStorage.score = 1

	stream2, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to create second stream: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	// Read the response
	if err := codec.SetReadDeadline(codec.ReadTimeout, stream2); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
	buf2 := bufio.NewReader(stream2)
	response2 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(buf2, response2); err != nil {
		t.Fatalf("Failed to read second response: %v", err)
	}

	// Verify it's a success response
	if _, ok := response2.Response.(*protocolsPb.Response_Success); !ok {
		t.Fatalf("Expected success response, got: %T", response2.Response)
	}
}

// TestDiscoveryPermissions tests the permissions for the discovery protocol.
// Both peers on the manifest and clients with bonds should be able to call
// the discovery protocol.
func TestDiscoveryPermissions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatal(err)
	}

	allPeers := mnet.Peers()
	meshHost := mnet.Host(allPeers[0])
	manifestHost := mnet.Host(allPeers[1])
	nonManifestClientHost := mnet.Host(allPeers[2]) // Client NOT on manifest

	// Create a manifest with the mesh node and one client
	mockManifest := &manifest{
		bootstrapPeers:    []*peer.AddrInfo{{ID: meshHost.ID(), Addrs: meshHost.Addrs()}},
		nonBootstrapPeers: []peer.ID{manifestHost.ID()},
	}

	// Create the test node with bondStorage initially set to score 0
	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockManifest)
	testBondStorage := &testBondStorage{score: 0}
	node.bondStorage = testBondStorage

	// Link and connect both clients to the mesh node
	if _, err := mnet.LinkPeers(manifestHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link manifest client to node: %v", err)
	}
	if _, err := mnet.ConnectPeers(manifestHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect manifest client to node: %v", err)
	}

	if _, err := mnet.LinkPeers(nonManifestClientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link non-manifest client to node: %v", err)
	}
	if _, err := mnet.ConnectPeers(nonManifestClientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect non-manifest client to node: %v", err)
	}

	time.Sleep(time.Second)

	// With bond score 0, manifest client should succeed
	stream1, err := manifestHost.NewStream(ctx, meshHost.ID(), protocols.DiscoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to create stream from manifest client: %v", err)
	}
	defer func() { _ = stream1.Close() }()

	// Read the response
	if err := codec.SetReadDeadline(codec.ReadTimeout, stream1); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
	buf1 := bufio.NewReader(stream1)
	response1 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(buf1, response1); err != nil {
		t.Fatalf("Failed to read response from manifest client: %v", err)
	}

	// Verify it's a discovery response (success)
	if _, ok := response1.Response.(*protocolsPb.Response_DiscoveryResponse); !ok {
		t.Fatalf("Expected discovery response from manifest client, got: %T", response1.Response)
	}

	// With bond score 0, non-manifest client should fail
	stream2, err := nonManifestClientHost.NewStream(ctx, meshHost.ID(), protocols.DiscoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to create stream from non-manifest client: %v", err)
	}

	// Read the response
	if err := codec.SetReadDeadline(codec.ReadTimeout, stream2); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
	buf2 := bufio.NewReader(stream2)
	response2 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(buf2, response2); err != nil {
		t.Fatalf("Failed to read response from non-manifest client: %v", err)
	}

	// Verify it's an unauthorized error
	if errResp, ok := response2.Response.(*protocolsPb.Response_Error); ok {
		if _, isUnauthorized := errResp.Error.Error.(*protocolsPb.Error_Unauthorized); !isUnauthorized {
			t.Fatalf("Expected unauthorized error, got different error type")
		}
	} else {
		t.Fatalf("Expected error response from non-manifest client, got: %T", response2.Response)
	}
	_ = stream2.Close()

	// Update bond score to 1, non-manifest client should now succeed
	testBondStorage.score = 1

	stream3, err := nonManifestClientHost.NewStream(ctx, meshHost.ID(), protocols.DiscoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to create second stream from non-manifest client: %v", err)
	}
	defer func() { _ = stream3.Close() }()

	// Read the response
	if err := codec.SetReadDeadline(codec.ReadTimeout, stream3); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
	buf3 := bufio.NewReader(stream3)
	response3 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(buf3, response3); err != nil {
		t.Fatalf("Failed to read second response from non-manifest client: %v", err)
	}

	// Verify it's a discovery response (success)
	if _, ok := response3.Response.(*protocolsPb.Response_DiscoveryResponse); !ok {
		t.Fatalf("Expected discovery response from non-manifest client, got: %T", response3.Response)
	}

	// Manifest client should still succeed with bond score 1
	stream4, err := manifestHost.NewStream(ctx, meshHost.ID(), protocols.DiscoveryProtocol)
	if err != nil {
		t.Fatalf("Failed to create second stream from manifest client: %v", err)
	}
	defer func() { _ = stream4.Close() }()

	// Read the response
	if err := codec.SetReadDeadline(codec.ReadTimeout, stream4); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
	buf4 := bufio.NewReader(stream4)
	response4 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(buf4, response4); err != nil {
		t.Fatalf("Failed to read second response from manifest client: %v", err)
	}

	// Verify it's a discovery response (success)
	if _, ok := response4.Response.(*protocolsPb.Response_DiscoveryResponse); !ok {
		t.Fatalf("Expected discovery response from manifest client, got: %T", response4.Response)
	}
}
