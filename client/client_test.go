package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/bond"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	"google.golang.org/protobuf/proto"
)

func createHost(t *testing.T, port int) (host.Host, string) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	host, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
	)
	if err != nil {
		t.Fatal(err)
	}

	return host, fmt.Sprintf("%s/p2p/%s", addr, host.ID().String())
}

func TestClient(t *testing.T) {
	// Setup the remote peer.
	remoteHostPort := 4567
	remoteHost, remoteHostAddr := createHost(t, remoteHostPort)
	defer func() { _ = remoteHost.Close() }()

	var remoteHostPushStream atomic.Pointer[network.Stream]
	setPushStream := func(ps network.Stream) {
		remoteHostPushStream.Store(&ps)
	}
	getPushStream := func() network.Stream {
		ps := remoteHostPushStream.Load()
		if ps != nil {
			return *ps
		}

		return nil
	}

	remoteHostReceivedMsgs := make(chan proto.Message, 10)
	remoteHostSubscribeHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		clientID := s.Conn().RemotePeer()
		if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
			t.Fatalf("Failed to set read deadline for client %s: %v", clientID, err)
			return
		}

		buf := bufio.NewReader(s)

		subscribeMessage := &protocolsPb.SubscribeRequest{}
		if err := codec.ReadLengthPrefixedMessage(buf, subscribeMessage); err != nil {
			t.Fatalf("Failed to read/unmarshal subscribe message from client %s: %v", clientID, err)

			return
		}

		remoteHostReceivedMsgs <- subscribeMessage
	}

	remoteHostPushHandler := func(s network.Stream) {
		resp := &protocolsPb.Response{
			Response: &protocolsPb.Response_Success{
				Success: &protocolsPb.Success{},
			},
		}

		err := codec.WriteLengthPrefixedMessage(s, resp)
		if err != nil {
			t.Fatalf("Failed to write message: %v", err)
		}

		setPushStream(s)
	}

	remoteHostPublishHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		clientID := s.Conn().RemotePeer()
		if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to set read deadline for client %s: %v.", clientID, err)
			}

			return
		}

		buf := bufio.NewReader(s)

		publishMessage := &protocolsPb.PublishRequest{}
		if err := codec.ReadLengthPrefixedMessage(buf, publishMessage); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read push message from client %s: %v.", clientID, err)
			}

			return
		}

		remoteHostReceivedMsgs <- publishMessage
	}

	remoteHostPostBondHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to set read deadline: %v.", err)
			}

			return
		}

		buf := bufio.NewReader(s)

		msg := &protocolsPb.PostBondRequest{}
		if err := codec.ReadLengthPrefixedMessage(buf, msg); err != nil {
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

	remoteHost.SetStreamHandler(protocols.ClientSubscribeProtocol, remoteHostSubscribeHandler)
	remoteHost.SetStreamHandler(protocols.ClientPushProtocol, remoteHostPushHandler)
	remoteHost.SetStreamHandler(protocols.ClientPublishProtocol, remoteHostPublishHandler)
	remoteHost.SetStreamHandler(protocols.PostBondsProtocol, remoteHostPostBondHandler)

	remoteHostBroadcastMsg := func(topic string, data []byte, sender peer.ID) {
		msg := &protocolsPb.PushMessage{
			MessageType: protocolsPb.PushMessage_BROADCAST,
			Topic:       topic,
			Data:        []byte(data),
			Sender:      []byte(sender),
		}

		pushStream := getPushStream()
		if pushStream == nil {
			t.Fatalf("Push stream is not initialized")
		}

		if err := codec.WriteLengthPrefixedMessage(pushStream, msg); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("Failed to write push message: %v", err)
			}
		}
	}

	// Setup the client.
	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client")
	logger.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	clientPort := 5678
	cfg := &Config{
		Port:           clientPort,
		RemotePeerAddr: remoteHostAddr,
		PrivateKey:     priv,
		Logger:         logger,
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		_ = client.Run(ctx, []*bond.BondParams{})
	}()

	// Wait briefly for initialization.
	time.Sleep(time.Second)

	pingTopic := "ping"

	clientReceivedMsgs := make(chan TopicEvent, 10)
	clientPingHandlerFunc := func(evt TopicEvent) {
		clientReceivedMsgs <- evt
	}

	// Ensure the client can subscribe to a topic.
	err = client.Subscribe(ctx, pingTopic, clientPingHandlerFunc)
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic %s, %v", pingTopic, err)
	}

	select {
	case msg := <-remoteHostReceivedMsgs:
		// Ensure the message received matches the sent topic.
		subMsg, ok := msg.(*protocolsPb.SubscribeRequest)
		if !ok {
			t.Fatal("Received message is not a ClientSubscribeMessage")
		}

		if subMsg.Topic != pingTopic {
			t.Fatalf("Expected %s, got %s", pingTopic, subMsg.Topic)
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive client subscribe message")
	}

	// Ensure the client can receive pushed messages.
	remoteHostBroadcastMsg(pingTopic, []byte(pingTopic), remoteHost.ID())
	select {
	case pushedMsg := <-clientReceivedMsgs:
		if pushedMsg.Type != TopicEventData {
			t.Fatalf("Expected TopicEventData type, got %v", pushedMsg.Type)
		}

		if string(pushedMsg.Data) != pingTopic {
			t.Fatalf("Expected %s for data, got %s", "ping", string(pushedMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive pushed message")
	}

	echoTopic := "echo"

	// Ensure the client can unsubscribe from a topic.
	err = client.Subscribe(ctx, echoTopic, func(TopicEvent) {})
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic %s, %v", echoTopic, err)
	}

	select {
	case msg := <-remoteHostReceivedMsgs:
		// Ensure the message received matches the sent topic.
		subMsg, ok := msg.(*protocolsPb.SubscribeRequest)
		if !ok {
			t.Fatal("Received message is not a ClientSubscribeMessage")
		}

		if subMsg.Topic != echoTopic {
			t.Fatalf("Expected %s, got %s", pingTopic, subMsg.Topic)
		}

	case <-time.After(time.Second * 3):
		t.Fatalf("Timed out waiting for unsubscription message from client")
	}

	topics := client.topicRegistry.fetchTopics()
	if len(topics) != 2 {
		t.Fatalf("Expected 2 subscribed topics, got %d", len(topics))
	}

	err = client.Unsubscribe(ctx, echoTopic)
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic %s, %v", echoTopic, err)
	}

	select {
	case msg := <-remoteHostReceivedMsgs:
		// Ensure the message received matches the sent topic.
		unsubMsg, ok := msg.(*protocolsPb.SubscribeRequest)
		if !ok {
			t.Fatal("Received message is not a ClientSubscribeMessage")
		}

		if unsubMsg.Topic != echoTopic {
			t.Fatalf("Expected %s, got %s", echoTopic, unsubMsg.Topic)
		}

		if unsubMsg.Subscribe {
			t.Fatal("Expected a false flag for an unsubscribe message")
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for client unsubscribe message")
	}

	topics = client.topicRegistry.fetchTopics()
	if len(topics) != 1 {
		t.Fatalf("Expected a subscribed topic, got %d", len(topics))
	}

	// Ensure the client can broadcast a message.
	broadcastMessage := &protocolsPb.PublishRequest{
		Topic: pingTopic,
		Data:  []byte("pong"),
	}

	broadcastMessageBytes, err := proto.Marshal(broadcastMessage)
	if err != nil {
		t.Fatalf("Unexpected error marshalling broadcast message: %v", err)
	}

	err = client.Broadcast(ctx, broadcastMessageBytes)
	if err != nil {
		t.Fatalf("Unexpected error broadcasting message: %v", err)
	}

	select {
	case msg := <-remoteHostReceivedMsgs:
		// Ensure the message received matches the sent topic.
		pubMsg, ok := msg.(*protocolsPb.PublishRequest)
		if !ok {
			t.Fatal("Received message is not a ClientPublishMessage")
		}

		if pubMsg.Topic != pingTopic {
			t.Fatalf("Expected %s, got %s", pingTopic, pubMsg.Topic)
		}

		if string(pubMsg.Data) != "pong" {
			t.Fatalf("Expected %s for message data, got %s", "pong", string(pubMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for broadcasted client message")
	}

	// Sever the client connection to trigger a reconnection.
	conns := client.host.Network().Conns()
	for _, conn := range conns {
		_ = conn.Close()
	}

	// Wait briefly for reconnection.
	time.Sleep(time.Second * 2)

	// Ensure the client reconnected successfully and resubscribed to its topics.
	select {
	case msg := <-remoteHostReceivedMsgs:
		subMsg, ok := msg.(*protocolsPb.SubscribeRequest)
		if !ok {
			t.Fatal("Received message is not a ClientSubscribeMessage")
		}

		if subMsg.Topic != pingTopic {
			t.Fatalf("Expected %s, got %s", pingTopic, subMsg.Topic)
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for broadcasted client message")
	}

	// Ensure the client can receive pushed messages after reconnecting.
	remoteHostBroadcastMsg(pingTopic, []byte(pingTopic), remoteHost.ID())

	// Ensure the client can receive pushed messages.
	select {
	case pushedMsg := <-clientReceivedMsgs:
		if string(pushedMsg.Data) != pingTopic {
			t.Fatalf("Expected %s for data, got %s", pingTopic, string(pushedMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive client pushed message after reconnect")
	}

	cancel()
}
