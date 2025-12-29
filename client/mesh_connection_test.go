package client

import (
	"bytes"
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
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

// Test helpers

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

func receiveWithTimeout(t *testing.T, ch <-chan proto.Message) proto.Message {
	select {
	case evt := <-ch:
		return evt
	case <-time.After(time.Second * 2):
		t.Fatal("Timed out waiting for message")
		return nil
	}
}

func receiveAttemptSignalWithTimeout(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		return
	case <-time.After(time.Second * 2):
		t.Fatal("Timed out waiting for attempt signal")
		return
	}
}

func receiveReadySignalWithTimeout(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		return
	case <-time.After(time.Second * 2):
		t.Fatal("Timed out waiting for ready signal")
		return
	}
}

type meshConnHarness struct {
	hostA             host.Host
	hostB             host.Host
	hostBAddr         string
	ctx               context.Context
	cancel            context.CancelFunc
	logger            slog.Logger
	bondInfo          *bond.BondInfo
	pushedMessages    chan proto.Message
	hostBReceivedMsgs chan proto.Message
	readyCh           chan struct{}
	hostBPushStream   atomic.Pointer[network.Stream]
	meshConn          *meshConnection
}

func newMeshConnHarness(t *testing.T) *meshConnHarness {
	hostAPort := 4455
	hostA, _ := createHost(t, hostAPort)

	hostBPort := 4456
	hostB, hostBAddr := createHost(t, hostBPort)

	peerAddr, err := ma.NewMultiaddr(hostBAddr)
	if err != nil {
		t.Fatalf("Failed to parse remote peer address: %v", err)
	}

	peerInfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
	if err != nil {
		t.Fatalf("Failed to parse peer info from address: %v", err)
	}

	hostA.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, time.Minute*10)

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("meshconn")
	logger.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())

	pushedMessages := make(chan proto.Message, 10)
	hostBReceivedMsgs := make(chan proto.Message, 10)
	readyCh := make(chan struct{}, 1)

	bondInfo := bond.NewBondInfo()
	bondInfo.AddBonds([]*bond.BondParams{{
		ID:       "placeholder",
		Expiry:   time.Now().Add(time.Hour * 6),
		Strength: bond.MinRequiredBondStrength}},
		time.Now())

	th := &meshConnHarness{
		hostA:             hostA,
		hostB:             hostB,
		hostBAddr:         hostBAddr,
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
		bondInfo:          bondInfo,
		pushedMessages:    pushedMessages,
		hostBReceivedMsgs: hostBReceivedMsgs,
		readyCh:           readyCh,
	}

	// Set up default handlers
	th.setupDefaultHandlers(t)

	return th
}

func (th *meshConnHarness) setupDefaultHandlers(t *testing.T) {
	th.hostA.SetStreamHandler(protocols.ClientPushProtocol, func(s network.Stream) {})

	th.hostB.SetStreamHandler(protocols.ClientPushProtocol, func(s network.Stream) {
		resp := &protocolsPb.Response{
			Response: &protocolsPb.Response_Success{
				Success: &protocolsPb.Success{},
			},
		}

		err := codec.WriteLengthPrefixedMessage(s, resp)
		if err != nil {
			t.Fatalf("Failed to write message: %v", err)
		}

		th.hostBPushStream.Store(&s)
	})

	th.hostB.SetStreamHandler(protocols.ClientPublishProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		pubMsg := &protocolsPb.PublishRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, pubMsg); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}
			return
		}

		th.hostBReceivedMsgs <- pubMsg
	})

	th.hostB.SetStreamHandler(protocols.ClientSubscribeProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.SubscribeRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}
			return
		}

		th.hostBReceivedMsgs <- msg
	})

	th.hostB.SetStreamHandler(protocols.PostBondsProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.PostBondRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if !errors.Is(err, io.EOF) {
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
	})
}

func (th *meshConnHarness) createMeshConnection() {
	handleMessage := func(msg *protocolsPb.PushMessage) {
		th.pushedMessages <- msg
	}

	fetchTopics := func() []string {
		return []string{}
	}

	setMeshConnection := func(meshConn meshConn) {
		th.readyCh <- struct{}{}
	}

	th.meshConn = newMeshConnection(
		th.hostA,
		th.hostB.ID(),
		th.logger,
		th.bondInfo,
		fetchTopics,
		handleMessage,
		setMeshConnection,
	)
}

func (th *meshConnHarness) runMeshConnection(t *testing.T) {
	go func() {
		err := th.meshConn.run(th.ctx)
		if err != nil {
			t.Logf("mesh connection run error: %v", err)
		}
	}()

	receiveReadySignalWithTimeout(t, th.readyCh)
}

func (th *meshConnHarness) getHostBPushStream() network.Stream {
	ps := th.hostBPushStream.Load()
	if ps != nil {
		return *ps
	}
	return nil
}

func (th *meshConnHarness) cleanup() {
	th.cancel()
	_ = th.hostA.Close()
	_ = th.hostB.Close()
}

// Tests

func TestMeshConnectionPushMessages(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	th.createMeshConnection()
	th.runMeshConnection(t)

	pushStreamFromB := th.hostB.Network().ConnsToPeer(th.hostA.ID())[0].GetStreams()[0]
	if pushStreamFromB == nil {
		t.Fatalf("Expected an initialized push stream from B")
	}

	pushMsg := &protocolsPb.PushMessage{
		Topic: "test1",
		Data:  []byte("test1"),
	}

	err := codec.WriteLengthPrefixedMessage(pushStreamFromB, pushMsg)
	if err != nil {
		t.Fatalf("Failed to write push message: %v", err)
	}

	msg := receiveWithTimeout(t, th.pushedMessages)

	pushedMsg, ok := msg.(*protocolsPb.PushMessage)
	if !ok {
		t.Fatal("Received message is not a PushMessage")
	}

	if pushedMsg.Topic != pushMsg.Topic {
		t.Fatalf("Expected topic %s, got %s", pushMsg.Topic, pushedMsg.Topic)
	}
	if string(pushedMsg.Data) != string(pushMsg.Data) {
		t.Fatalf("Expected data %s, got %s", string(pushMsg.Data), string(pushedMsg.Data))
	}
}

func TestMeshConnectionSubscribeTopicsOnConnection(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	topics := []string{"topic1", "topic2", "topic3"}
	var subscribeCount atomic.Int32

	fetchTopics := func() []string {
		return topics
	}

	handleMessage := func(msg *protocolsPb.PushMessage) {
		th.pushedMessages <- msg
	}

	setMeshConnection := func(meshConn meshConn) {
		th.readyCh <- struct{}{}
	}

	// Override the subscribe handler to count subscriptions
	th.hostB.SetStreamHandler(protocols.ClientSubscribeProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.SubscribeRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Logf("Failed to read message: %v", err)
			}
			return
		}

		if msg.Subscribe {
			subscribeCount.Add(1)
		}
	})

	th.meshConn = newMeshConnection(
		th.hostA,
		th.hostB.ID(),
		th.logger,
		th.bondInfo,
		fetchTopics,
		handleMessage,
		setMeshConnection,
	)

	th.runMeshConnection(t)

	// Give it time to process initial subscriptions
	time.Sleep(100 * time.Millisecond)

	count := subscribeCount.Load()
	if count != int32(len(topics)) {
		t.Fatalf("Expected %d subscriptions, got %d", len(topics), count)
	}

	// Close the push stream to force reconnection
	pushStreamFromA := th.hostA.Network().ConnsToPeer(th.hostB.ID())[0].GetStreams()[0]
	if pushStreamFromA == nil {
		t.Fatalf("Expected an initialized push stream from A")
	}

	err := pushStreamFromA.Close()
	if err != nil {
		t.Fatalf("Unexpected error closing push stream: %v", err)
	}

	// Wait for reconnection
	receiveReadySignalWithTimeout(t, th.readyCh)

	// Give it time after reconnection
	time.Sleep(100 * time.Millisecond)

	// Verify subscriptions remain the same (subscribeTopics is only called once during run)
	finalCount := subscribeCount.Load()
	if finalCount != int32(len(topics)) {
		t.Fatalf("Expected subscriptions to remain %d after reconnection, got %d", len(topics), finalCount)
	}
}

func TestMeshConnectionSubscribe(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	th.createMeshConnection()
	th.runMeshConnection(t)

	testTopic := "test-topic"
	err := th.meshConn.subscribe(th.ctx, testTopic)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic: %v", err)
	}

	msg := receiveWithTimeout(t, th.hostBReceivedMsgs)

	subMsg, ok := msg.(*protocolsPb.SubscribeRequest)
	if !ok {
		t.Fatal("Received message is not a SubscribeRequest")
	}

	if subMsg.Topic != testTopic {
		t.Fatalf("Expected %s, got %s", testTopic, subMsg.Topic)
	}

	if !subMsg.Subscribe {
		t.Fatal("Expected Subscribe flag to be true")
	}
}

func TestMeshConnectionUnsubscribe(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	th.createMeshConnection()
	th.runMeshConnection(t)

	testTopic := "test-topic"
	err := th.meshConn.unsubscribe(th.ctx, testTopic)
	if err != nil {
		t.Fatalf("Failed to unsubscribe from topic: %v", err)
	}

	msg := receiveWithTimeout(t, th.hostBReceivedMsgs)

	unsubMsg, ok := msg.(*protocolsPb.SubscribeRequest)
	if !ok {
		t.Fatal("Received message is not a SubscribeRequest")
	}

	if unsubMsg.Topic != testTopic {
		t.Fatalf("Expected %s, got %s", testTopic, unsubMsg.Topic)
	}

	if unsubMsg.Subscribe {
		t.Fatal("Expected Subscribe flag to be false")
	}
}

func TestMeshConnectionBroadcast(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	th.createMeshConnection()
	th.runMeshConnection(t)

	testTopic := "test-topic"
	data := []byte("testing")

	err := th.meshConn.broadcast(th.ctx, testTopic, data)
	if err != nil {
		t.Fatalf("Failed to broadcast message: %v", err)
	}

	msg := receiveWithTimeout(t, th.hostBReceivedMsgs)

	pubMsg, ok := msg.(*protocolsPb.PublishRequest)
	if !ok {
		t.Fatal("Received message is not a PublishRequest")
	}

	if pubMsg.Topic != testTopic {
		t.Fatalf("Expected %s, got %s", testTopic, pubMsg.Topic)
	}

	if !bytes.Equal(pubMsg.Data, data) {
		t.Fatalf("Expected data %s, got %s", string(data), string(pubMsg.Data))
	}
}

func TestMeshConnectionPostBondWithRetries(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	var attempts atomic.Int32
	attemptsCh := make(chan struct{}, 10)

	th.hostB.SetStreamHandler(protocols.PostBondsProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		msg := &protocolsPb.PostBondRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, msg); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read message: %v", err)
			}
			return
		}

		th.hostBReceivedMsgs <- msg

		var bondResp proto.Message
		switch attempts.Load() {
		case 0:
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
		attemptsCh <- struct{}{}
	})

	th.createMeshConnection()

	bondReq, err := bond.PostBondReqFromBondInfo(th.meshConn.bondInfo)
	if err != nil {
		t.Fatalf("Unexpected error creating post bond request: %v", err)
	}

	// First attempt should fail with error message
	err = th.meshConn.postBondInternal(th.ctx, bondReq)
	if err == nil {
		t.Fatal("Expected a post bond error")
	}

	msg := receiveWithTimeout(t, th.hostBReceivedMsgs)
	bondMsg, ok := msg.(*protocolsPb.PostBondRequest)
	if !ok {
		t.Fatal("Received message is not a PostBondRequest")
	}

	if len(bondMsg.Bonds) != 1 {
		t.Fatalf("Expected %d bond, got %d", 1, len(bondMsg.Bonds))
	}

	receiveAttemptSignalWithTimeout(t, attemptsCh)

	// Second attempt should fail with invalid bond index
	err = th.meshConn.postBondInternal(th.ctx, bondReq)
	if err == nil {
		t.Fatal("Expected a post bond error")
	}

	if !errors.Is(err, errInvalidBondIndex) {
		t.Fatalf("Expected an invalid bond index error, got %v", err)
	}

	_ = receiveWithTimeout(t, th.hostBReceivedMsgs)
	receiveAttemptSignalWithTimeout(t, attemptsCh)

	// Third attempt should succeed
	err = th.meshConn.postBondInternal(th.ctx, bondReq)
	if err != nil {
		t.Fatalf("Expected a successful post bond attempt, got %v", err)
	}

	_ = receiveWithTimeout(t, th.hostBReceivedMsgs)
	receiveAttemptSignalWithTimeout(t, attemptsCh)
}

func TestMeshConnectionReconnection(t *testing.T) {
	th := newMeshConnHarness(t)
	defer th.cleanup()

	th.createMeshConnection()
	th.runMeshConnection(t)

	// Verify initial connection
	pushStreamFromA := th.hostA.Network().ConnsToPeer(th.hostB.ID())[0].GetStreams()[0]
	if pushStreamFromA == nil {
		t.Fatalf("Expected an initialized push stream from A")
	}

	// Close the push stream to force reconnection
	err := pushStreamFromA.Close()
	if err != nil {
		t.Fatalf("Unexpected error closing push stream: %v", err)
	}

	// Wait for reconnection
	receiveReadySignalWithTimeout(t, th.readyCh)

	// Verify reconnection by sending a message
	pushMsg := &protocolsPb.PushMessage{
		Topic: "test-reconnect",
		Data:  []byte("reconnect-test"),
	}

	pushStreamFromB := th.getHostBPushStream()
	if pushStreamFromB == nil {
		t.Fatalf("Expected an initialized push stream from B after reconnection")
	}

	err = codec.WriteLengthPrefixedMessage(pushStreamFromB, pushMsg)
	if err != nil {
		t.Fatalf("Failed to write push message: %v", err)
	}

	msg := receiveWithTimeout(t, th.pushedMessages)

	pushedMsg, ok := msg.(*protocolsPb.PushMessage)
	if !ok {
		t.Fatal("Received message is not a PushMessage")
	}

	if pushedMsg.Topic != pushMsg.Topic {
		t.Fatalf("Expected topic %s, got %s", pushMsg.Topic, pushedMsg.Topic)
	}
	if string(pushedMsg.Data) != string(pushMsg.Data) {
		t.Fatalf("Expected data %s, got %s", string(pushMsg.Data), string(pushedMsg.Data))
	}
}
