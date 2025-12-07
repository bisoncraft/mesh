package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/bond"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

func TestMeshConnection(t *testing.T) {
	// Create two hosts, A and B.
	hostAPort := 4455
	hostA, _ := createHost(t, hostAPort)

	hostBPort := 4456
	hostB, hostBAddr := createHost(t, hostBPort)

	hostBReceivedMsgs := make(chan proto.Message, 10)
	hostBPublishHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		pubMsg := &protocolsPb.PublishRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, pubMsg); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}

			return
		}

		hostBReceivedMsgs <- pubMsg
	}

	hostBSubscribeHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.SubscribeRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}

			return
		}

		hostBReceivedMsgs <- msg
	}

	var attempts atomic.Int32
	hostBFailureBondHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.PostBondRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}

			return
		}

		hostBReceivedMsgs <- msg

		var bondResp proto.Message
		switch attempts.Load() {
		case 0:
			// Fail on the first post bond attempt with an error message.
			bondResp = &protocolsPb.Response{
				Response: &protocolsPb.Response_Error{
					Error: &protocolsPb.Error{
						Error: &protocolsPb.Error_Message{
							Message: "failed to post bonds",
						},
					},
				},
			}

		case 1:
			// Fail on the second post bond attempt with an invalid bond index error.
			bondResp = &protocolsPb.Response{
				Response: &protocolsPb.Response_Error{
					Error: &protocolsPb.Error{
						Error: &protocolsPb.Error_PostBondError{
							PostBondError: &protocolsPb.PostBondError{
								InvalidBondIndex: 0,
							},
						},
					},
				},
			}

		case 2:
			// Succeed on the third post bond attempt.
			bondResp = &protocolsPb.Response{
				Response: &protocolsPb.Response_PostBondResponse{
					PostBondResponse: &protocolsPb.PostBondResponse{
						BondStrength: bond.MinRequiredBondStrength,
					},
				},
			}
		}

		err := codec.WriteLengthPrefixedMessage(s, bondResp)
		if err != nil {
			t.Fatalf("Failed to write message: %v", err)
		}

		attempts.Add(1)
	}

	hostBSuccessBondHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.PostBondRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}

			return
		}

		bondResp := &protocolsPb.Response{
			Response: &protocolsPb.Response_PostBondResponse{
				PostBondResponse: &protocolsPb.PostBondResponse{
					BondStrength: bond.MinRequiredBondStrength,
				},
			},
		}

		err := codec.WriteLengthPrefixedMessage(s, bondResp)
		if err != nil {
			t.Fatalf("Failed to write message: %v", err)
		}
	}

	var hostBPushStream atomic.Pointer[network.Stream]
	getHostBPushStream := func() network.Stream {
		ps := hostBPushStream.Load()
		if ps != nil {
			return *ps
		}

		return nil
	}

	hostA.SetStreamHandler(protocols.ClientPushProtocol, func(s network.Stream) {})
	hostB.SetStreamHandler(protocols.ClientPushProtocol, func(s network.Stream) {
		resp := &protocolsPb.Response{
			Response: &protocolsPb.Response_Success{
				Success: &protocolsPb.Success{},
			},
		}

		err := codec.WriteLengthPrefixedMessage(s, resp)
		if err != nil {
			t.Fatalf("Failed to write message: %v", err)
		}

		hostBPushStream.Store(&s)
	})
	hostB.SetStreamHandler(protocols.ClientPublishProtocol, hostBPublishHandler)
	hostB.SetStreamHandler(protocols.ClientSubscribeProtocol, hostBSubscribeHandler)
	hostB.SetStreamHandler(protocols.PostBondsProtocol, hostBSuccessBondHandler)

	// Add host Bs details to A.
	peerAddr, err := ma.NewMultiaddr(hostBAddr)
	if err != nil {
		t.Fatalf("Failed to parse remote peer address: %v", err)
	}

	peerInfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
	if err != nil {
		t.Fatalf("Failed to parse peer info from address: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hostA.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, time.Minute*10)

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("meshconn")
	logger.SetLevel(slog.LevelDebug)

	pushedMessages := make(chan *protocolsPb.PushMessage, 10)
	handleMessage := func(msg *protocolsPb.PushMessage) {
		pushedMessages <- msg
	}

	bondInfo := bond.NewBondInfo()

	fetchTopics := func() []string {
		return []string{}
	}

	readyCh := make(chan struct{}, 1)
	setMeshConnection := func(*meshConnection) {
		readyCh <- struct{}{}
	}

	// Setup the mesh connection.
	meshConn := newMeshConnection(hostA, hostB.ID(), logger, bondInfo, fetchTopics, handleMessage, setMeshConnection)

	meshConn.bondInfo.AddBonds([]*bond.BondParams{{
		ID:       "placeholder",
		Expiry:   time.Now().Add(time.Hour * 6),
		Strength: bond.MinRequiredBondStrength}},
		time.Now())

	go func() {
		err := meshConn.run(ctx)
		if err != nil {
			t.Errorf("mesh connection run error: %v", err)
		}
	}()

	// Wait until mesh connection is ready.
	<-readyCh

	pushStreamFromB := hostB.Network().ConnsToPeer(hostA.ID())[0].GetStreams()[0]
	if pushStreamFromB == nil {
		t.Fatalf("Expected an initialized push stream from B")
	}

	// Ensure the mesh connection can process pushed messages.
	pushMsg1 := &protocolsPb.PushMessage{
		Topic: "test1",
		Data:  []byte("test1"),
	}

	err = codec.WriteLengthPrefixedMessage(pushStreamFromB, pushMsg1)
	if err != nil {
		t.Fatalf("Failed to write push message: %v", err)
	}

	// Ensure the handler func receives and processes pushed messages.
	select {
	case receivedMsg := <-pushedMessages:
		if receivedMsg.Topic != pushMsg1.Topic {
			t.Fatalf("Expected topic %s, got %s", pushMsg1.Topic, receivedMsg.Topic)
		}
		if string(receivedMsg.Data) != string(pushMsg1.Data) {
			t.Fatalf("Expected data %s, got %s", string(pushMsg1.Data), string(receivedMsg.Data))
		}
	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for pushed message")
	}

	// Ensure the mesh connection can subscribe to a topic.
	testTopic := "test3"
	err = meshConn.subscribe(ctx, testTopic)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic: %v", err)
	}

	select {
	case msg := <-hostBReceivedMsgs:
		subMsg, ok := msg.(*protocolsPb.SubscribeRequest)
		if !ok {
			t.Fatal("Received message is not a ClientSubscribeMessage")
		}

		if subMsg.Topic != testTopic {
			t.Fatalf("Expected %s, got %s", testTopic, subMsg.Topic)
		}

		if !subMsg.Subscribe {
			t.Fatal("Expected a true flag for an subscribe message")
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for a subscribe message")
	}

	// Ensure the mesh connection can unsubscribe from a topic.
	err = meshConn.unsubscribe(ctx, testTopic)
	if err != nil {
		t.Fatalf("Failed to unsubscribe from topic: %v", err)
	}

	select {
	case msg := <-hostBReceivedMsgs:
		unsubMsg, ok := msg.(*protocolsPb.SubscribeRequest)
		if !ok {
			t.Fatal("Received message is not a ClientSubscribeMessage")
		}

		if unsubMsg.Topic != testTopic {
			t.Fatalf("Expected %s, got %s", testTopic, unsubMsg.Topic)
		}

		if unsubMsg.Subscribe {
			t.Fatal("Expected a false flag for an subscribe message")
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for an unsubscribe message")
	}

	// Ensure the mesh connection can publish messages.
	data := []byte("testing")
	err = meshConn.broadcast(ctx, testTopic, data)
	if err != nil {
		t.Fatalf("Failed to broadcast message: %v", err)
	}

	select {
	case msg := <-hostBReceivedMsgs:
		pubMsg, ok := msg.(*protocolsPb.PublishRequest)
		if !ok {
			t.Fatal("Received message is not a PublishRequest")
		}

		if pubMsg.Topic != testTopic {
			t.Fatalf("Expected %s, got %s", testTopic, pubMsg.Topic)
		}

		if !bytes.Equal(pubMsg.Data, data) {
			t.Fatalf("Expected a %s as published message data, got %s", string(pubMsg.Data), string(data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for a published message")
	}

	// Ensure the mesh connection can post bonds.
	hostB.SetStreamHandler(protocols.PostBondsProtocol, hostBFailureBondHandler)

	bondReq, err := bond.PostBondReqFromBondInfo(meshConn.bondInfo)
	if err != nil {
		t.Fatalf("Unexpected error creating post bond request: %v", err)
	}

	// We expect the first two post bond attempts to fail.
	err = meshConn.postBondInternal(ctx, bondReq)
	if err == nil {
		t.Fatal("Expected a post bond error")
	}

	select {
	case msg := <-hostBReceivedMsgs:
		bondMsg, ok := msg.(*protocolsPb.PostBondRequest)
		if !ok {
			t.Fatal("Received message is not a PostBondRequest")
		}

		if len(bondMsg.Bonds) != 1 {
			t.Fatalf("Expected %d bond, got %d", 1, len(bondMsg.Bonds))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for a bond message")
	}

	err = meshConn.postBondInternal(ctx, bondReq)
	if err == nil {
		t.Fatal("Expected a post bond error")
	}

	if !errors.Is(err, errInvalidBondIndex) {
		t.Fatalf("Expected an invalid bond index error, got %v", err)
	}

	select {
	case <-hostBReceivedMsgs:
	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for a bond message")
	}

	// The third attempt to post bond should succeed.
	err = meshConn.postBondInternal(ctx, bondReq)
	if err != nil {
		t.Fatal("Expected a successful post bond attempt")
	}

	select {
	case <-hostBReceivedMsgs:
	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for a bond message")
	}

	hostB.SetStreamHandler(protocols.PostBondsProtocol, hostBSuccessBondHandler)

	// Force a reconnection by closing the mesh connection push stream.
	pushStreamFromA := hostA.Network().ConnsToPeer(hostB.ID())[0].GetStreams()[0]
	if pushStreamFromA == nil {
		t.Fatalf("Expected an initialized push stream from A")
	}

	err = pushStreamFromA.Close()
	if err != nil {
		t.Fatalf("Unexpected error closing push stream: %v", err)
	}

	// Wait until mesh connection is ready.
	<-readyCh

	pushMsg2 := &protocolsPb.PushMessage{
		Topic: "test2",
		Data:  []byte("test2"),
	}

	pushStreamFromB = getHostBPushStream()
	if pushStreamFromB == nil {
		t.Fatalf("Expected an initialized push stream from B")
	}

	err = codec.WriteLengthPrefixedMessage(pushStreamFromB, pushMsg2)
	if err != nil {
		t.Fatalf("Failed to write push message: %v", err)
	}

	// Ensure the handler func receives and processes pushed messages.
	select {
	case receivedMsg := <-pushedMessages:
		if receivedMsg.Topic != pushMsg2.Topic {
			t.Fatalf("Expected topic %s, got %s", pushMsg2.Topic, receivedMsg.Topic)
		}
		if string(receivedMsg.Data) != string(pushMsg2.Data) {
			t.Fatalf("Expected data %s, got %s", string(pushMsg2.Data), string(receivedMsg.Data))
		}
	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for pushed message")
	}

	meshConn.kill()
}
