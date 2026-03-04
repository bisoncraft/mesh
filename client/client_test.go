package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	clientpb "github.com/bisoncraft/mesh/client/pb"
	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"google.golang.org/protobuf/proto"
)

type relayMessageParams struct {
	PeerID  peer.ID
	Message []byte
}

type broadcastParams struct {
	Topic string
	Data  []byte
}

type relayMessageReturnValue struct {
	resp []byte
	err  error
}

type tMeshConnection struct {
	mu sync.Mutex

	remotePeer peer.ID

	relayMessageCalls   []relayMessageParams
	relayMessageReturns []relayMessageReturnValue
	relayMessageIdx     int

	broadcastCalls []broadcastParams
	broadcastErr   error

	subscribeCalls []string
	subscribeErr   error

	unsubscribeCalls []string
	unsubscribeErr   error

	postBondCalls []struct{}
	postBondErr   error

	fetchNodes []peer.AddrInfo

	runErrCh chan error

	KillCalls int
}

var _ meshConn = (*tMeshConnection)(nil)

func newTMeshConnection(remotePeer peer.ID) *tMeshConnection {
	return &tMeshConnection{
		remotePeer: remotePeer,
		runErrCh:   make(chan error, 1),
		fetchNodes: []peer.AddrInfo{},
	}
}

func (m *tMeshConnection) remotePeerID() peer.ID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remotePeer
}

func (m *tMeshConnection) relayMessage(ctx context.Context, peerID peer.ID, message []byte) ([]byte, error) {
	m.mu.Lock()
	call := relayMessageParams{
		PeerID:  peerID,
		Message: append([]byte(nil), message...),
	}
	m.relayMessageCalls = append(m.relayMessageCalls, call)
	idx := m.relayMessageIdx
	m.relayMessageIdx++
	rets := m.relayMessageReturns
	m.mu.Unlock()

	if idx >= len(rets) {
		return nil, errors.New("unexpected relayMessage call")
	}

	if rets[idx].err != nil {
		return nil, rets[idx].err
	}
	return rets[idx].resp, nil
}

func (m *tMeshConnection) broadcast(ctx context.Context, topic string, data []byte) error {
	m.mu.Lock()
	call := broadcastParams{
		Topic: topic,
		Data:  append([]byte(nil), data...),
	}
	m.broadcastCalls = append(m.broadcastCalls, call)
	m.mu.Unlock()

	return m.broadcastErr
}

func (m *tMeshConnection) subscribe(ctx context.Context, topic string) error {
	m.mu.Lock()
	m.subscribeCalls = append(m.subscribeCalls, topic)
	m.mu.Unlock()

	return m.subscribeErr
}

func (m *tMeshConnection) unsubscribe(ctx context.Context, topic string) error {
	m.mu.Lock()
	m.unsubscribeCalls = append(m.unsubscribeCalls, topic)
	m.mu.Unlock()

	return m.unsubscribeErr
}

func (m *tMeshConnection) postBond(ctx context.Context) error {
	m.mu.Lock()
	m.postBondCalls = append(m.postBondCalls, struct{}{})
	m.mu.Unlock()

	return m.postBondErr
}

func (m *tMeshConnection) kill() {}

func (m *tMeshConnection) run(ctx context.Context) error {
	if m.runErrCh != nil {
		select {
		case err := <-m.runErrCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	<-ctx.Done()
	return ctx.Err()
}

func (m *tMeshConnection) fetchAvailableMeshNodes(ctx context.Context) ([]peer.AddrInfo, error) {
	m.mu.Lock()
	nodes := append([]peer.AddrInfo(nil), m.fetchNodes...)
	m.mu.Unlock()
	return nodes, nil
}

func (m *tMeshConnection) waitReady(ctx context.Context) {
}

func (m *tMeshConnection) fail(err error) {
	m.runErrCh <- err
}

type encryptPeerMessageParams struct {
	PeerID  peer.ID
	Message []byte
	Nonce   keyID
}

type decryptPeerMessageParams struct {
	PeerID  peer.ID
	Message []byte
}

type clearEncryptionKeyParams struct {
	PeerID peer.ID
	ID     keyID
}

type handleHandshakeRequestParams struct {
	PeerID peer.ID
	Nonce  []byte
	PubKey []byte
}

type tEncryptionManager struct {
	mu sync.Mutex

	encryptPeerMessageCalls []encryptPeerMessageParams
	encryptPeerMessageResp  []byte
	encryptPeerMessageID    keyID
	encryptPeerMessageErr   error

	decryptPeerMessageCalls []decryptPeerMessageParams
	decryptPeerMessageResp  []byte
	decryptPeerMessageID    keyID
	decryptPeerMessageErr   error

	clearEncryptionKeyCalls []clearEncryptionKeyParams

	handleHandshakeRequestCalls []handleHandshakeRequestParams
	handleHandshakeRequestResp  []byte
	handleHandshakeRequestErr   error
}

var _ encryptionManager = (*tEncryptionManager)(nil)

func (em *tEncryptionManager) encryptPeerMessage(ctx context.Context, peerID peer.ID, msg []byte, nonce keyID) ([]byte, keyID, error) {
	em.mu.Lock()
	em.encryptPeerMessageCalls = append(em.encryptPeerMessageCalls, encryptPeerMessageParams{
		PeerID:  peerID,
		Message: append([]byte(nil), msg...),
		Nonce:   nonce,
	})
	respB := append([]byte(nil), em.encryptPeerMessageResp...)
	retID := em.encryptPeerMessageID
	err := em.encryptPeerMessageErr
	em.mu.Unlock()

	return respB, retID, err
}

func (em *tEncryptionManager) decryptPeerMessage(peerID peer.ID, msg []byte) ([]byte, keyID, error) {
	em.mu.Lock()
	em.decryptPeerMessageCalls = append(em.decryptPeerMessageCalls, decryptPeerMessageParams{
		PeerID:  peerID,
		Message: append([]byte(nil), msg...),
	})
	respB := append([]byte(nil), em.decryptPeerMessageResp...)
	retID := em.decryptPeerMessageID
	err := em.decryptPeerMessageErr
	em.mu.Unlock()

	return respB, retID, err
}

func (em *tEncryptionManager) clearEncryptionKey(peerID peer.ID, id keyID) {
	em.mu.Lock()
	em.clearEncryptionKeyCalls = append(em.clearEncryptionKeyCalls, clearEncryptionKeyParams{
		PeerID: peerID,
		ID:     id,
	})
	em.mu.Unlock()
}

func (em *tEncryptionManager) handleHandshakeRequest(peerID peer.ID, nonceB, pubKeyB []byte) ([]byte, error) {
	em.mu.Lock()
	em.handleHandshakeRequestCalls = append(em.handleHandshakeRequestCalls, handleHandshakeRequestParams{
		PeerID: peerID,
		Nonce:  append([]byte(nil), nonceB...),
		PubKey: append([]byte(nil), pubKeyB...),
	})
	respB := append([]byte(nil), em.handleHandshakeRequestResp...)
	err := em.handleHandshakeRequestErr
	em.mu.Unlock()

	return respB, err
}

func (em *tEncryptionManager) run(ctx context.Context) {}

// setTestMeshConnection sets up a mock mesh connection for testing.
func (c *Client) setTestMeshConnection(mc meshConn) {
	if c.connManager == nil {
		c.connManager = &meshConnectionManager{}
	}
	c.connManager.setPrimaryConnection(mc)
}

func TestSubscribe(t *testing.T) {
	ctx := context.Background()

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{},
		topicRegistry: newTopicRegistry(),
	}
	c.setTestMeshConnection(mc)

	// Subscribe to a topic successfully.
	err := c.Subscribe(ctx, "topic-1", func(TopicEvent) {})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := len(mc.subscribeCalls); got != 1 {
		t.Fatalf("expected 1 subscribe call, got %d", got)
	}
	if mc.subscribeCalls[0] != "topic-1" {
		t.Fatalf("expected topic %q, got %q", "topic-1", mc.subscribeCalls[0])
	}

	// Subscribe to the same topic again should return ErrRedundantSubscription and not call mesh conn.
	err = c.Subscribe(ctx, "topic-1", func(TopicEvent) {})
	if !errors.Is(err, ErrRedundantSubscription) {
		t.Fatalf("expected ErrRedundantSubscription, got %v", err)
	}
	if got := len(mc.subscribeCalls); got != 1 {
		t.Fatalf("expected subscribe call count to remain 1, got %d", got)
	}

	// Subscribe to another topic when meshConn returns an error.
	wantErr := errors.New("subscribe failed")
	mc.subscribeErr = wantErr
	err = c.Subscribe(ctx, "topic-2", func(TopicEvent) {})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if got := len(mc.subscribeCalls); got != 2 {
		t.Fatalf("expected 2 subscribe calls, got %d", got)
	}
	if mc.subscribeCalls[1] != "topic-2" {
		t.Fatalf("expected topic %q, got %q", "topic-2", mc.subscribeCalls[1])
	}
	if _, err := c.topicRegistry.fetchHandler("topic-2"); err == nil {
		t.Fatalf("topic-2 should not be registered when subscribe returns an error")
	}

	// Unsubscribe from topic-1 with an error from meshConn.
	wantUnsubErr := errors.New("unsubscribe failed")
	mc.unsubscribeErr = wantUnsubErr
	err = c.Unsubscribe(ctx, "topic-1")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, wantUnsubErr) {
		t.Fatalf("expected error %v, got %v", wantUnsubErr, err)
	}
	if got := len(mc.unsubscribeCalls); got != 1 {
		t.Fatalf("expected 1 unsubscribe call, got %d", got)
	}
	if mc.unsubscribeCalls[0] != "topic-1" {
		t.Fatalf("expected topic %q, got %q", "topic-1", mc.unsubscribeCalls[0])
	}
	if _, err := c.topicRegistry.fetchHandler("topic-1"); err != nil {
		t.Fatalf("topic-1 should remain registered when unsubscribe returns an error")
	}

	// Unsubscribe without error.
	mc.unsubscribeErr = nil
	err = c.Unsubscribe(ctx, "topic-1")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := len(mc.unsubscribeCalls); got != 2 {
		t.Fatalf("expected 2 unsubscribe calls, got %d", got)
	}
	if mc.unsubscribeCalls[1] != "topic-1" {
		t.Fatalf("expected topic %q, got %q", "topic-1", mc.unsubscribeCalls[1])
	}
	if _, err := c.topicRegistry.fetchHandler("topic-1"); err == nil {
		t.Fatalf("topic-1 should be unregistered after successful unsubscribe")
	}

	// Unsubscribe again should return ErrRedundantUnsubscription and not call mesh connection.
	err = c.Unsubscribe(ctx, "topic-1")
	if !errors.Is(err, ErrRedundantUnsubscription) {
		t.Fatalf("expected ErrRedundantUnsubscription, got %v", err)
	}
	if got := len(mc.unsubscribeCalls); got != 2 {
		t.Fatalf("expected unsubscribe call count to remain 2, got %d", got)
	}
}

func TestPushMessage(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	type handled struct {
		topic string
		ev    TopicEvent
	}
	handledCh := make(chan handled, 10)

	handler1 := func(ev TopicEvent) { handledCh <- handled{topic: "topic-1", ev: ev} }
	handler2 := func(ev TopicEvent) { handledCh <- handled{topic: "topic-2", ev: ev} }

	if err := c.Subscribe(ctx, "topic-1", handler1); err != nil {
		t.Fatalf("subscribe topic-1: %v", err)
	}
	if err := c.Subscribe(ctx, "topic-2", handler2); err != nil {
		t.Fatalf("subscribe topic-2: %v", err)
	}

	sender := peer.ID("sender-peer")

	// BROADCAST on topic-1 should call handler1 with TopicEventData and data payload.
	broadcastData := []byte("hello")
	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_BROADCAST,
		Topic:       "topic-1",
		Data:        broadcastData,
		Sender:      []byte(sender),
	})

	// SUBSCRIBE on topic-2 should call handler2 with TopicEventPeerSubscribed.
	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_SUBSCRIBE,
		Topic:       "topic-2",
		Sender:      []byte(sender),
	})

	// UNSUBSCRIBE on topic-1 should call handler1 with TopicEventPeerUnsubscribed.
	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_UNSUBSCRIBE,
		Topic:       "topic-1",
		Sender:      []byte(sender),
	})

	// Validate exactly 3 handler invocations and that the correct handler was used per topic.
	var got []handled
	for range 3 {
		select {
		case h := <-handledCh:
			got = append(got, h)
		default:
			t.Fatalf("expected 3 handled events, got %d", len(got))
		}
	}

	// Ensure no additional handler invocations occurred.
	select {
	case extra := <-handledCh:
		t.Fatalf("unexpected extra handled event for topic %q: %+v", extra.topic, extra.ev)
	default:
	}

	// Helper to find the next event for a topic.
	nextFor := func(topic string) (TopicEvent, bool) {
		for i, h := range got {
			if h.topic == topic {
				ev := h.ev
				got = append(got[:i], got[i+1:]...)
				return ev, true
			}
		}
		return TopicEvent{}, false
	}

	ev, ok := nextFor("topic-1")
	if !ok {
		t.Fatalf("expected an event for topic-1")
	}
	if ev.Type != TopicEventData {
		t.Fatalf("topic-1 first event: expected TopicEventData, got %v", ev.Type)
	}
	if string(ev.Data) != string(broadcastData) {
		t.Fatalf("topic-1 first event: expected data %q, got %q", string(broadcastData), string(ev.Data))
	}
	if ev.Peer != sender {
		t.Fatalf("topic-1 first event: expected peer %q, got %q", sender, ev.Peer)
	}

	ev, ok = nextFor("topic-2")
	if !ok {
		t.Fatalf("expected an event for topic-2")
	}
	if ev.Type != TopicEventPeerSubscribed {
		t.Fatalf("topic-2 event: expected TopicEventPeerSubscribed, got %v", ev.Type)
	}
	if len(ev.Data) != 0 {
		t.Fatalf("topic-2 event: expected empty data, got %q", string(ev.Data))
	}
	if ev.Peer != sender {
		t.Fatalf("topic-2 event: expected peer %q, got %q", sender, ev.Peer)
	}

	ev, ok = nextFor("topic-1")
	if !ok {
		t.Fatalf("expected a second event for topic-1")
	}
	if ev.Type != TopicEventPeerUnsubscribed {
		t.Fatalf("topic-1 second event: expected TopicEventPeerUnsubscribed, got %v", ev.Type)
	}
	if len(ev.Data) != 0 {
		t.Fatalf("topic-1 second event: expected empty data, got %q", string(ev.Data))
	}
	if ev.Peer != sender {
		t.Fatalf("topic-1 second event: expected peer %q, got %q", sender, ev.Peer)
	}

	if len(got) != 0 {
		t.Fatalf("unexpected extra handled events: %d", len(got))
	}
}

func TestConcurrentBroadcasts(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	testTopic := "concurrent-broadcast-test"

	// Subscribe to the test topic.
	err := c.Subscribe(ctx, testTopic, func(TopicEvent) {})
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic %s: %v", testTopic, err)
	}

	// Verify subscribe was called.
	if got := len(mc.subscribeCalls); got != 1 {
		t.Fatalf("expected 1 subscribe call, got %d", got)
	}
	if mc.subscribeCalls[0] != testTopic {
		t.Fatalf("expected topic %q, got %q", testTopic, mc.subscribeCalls[0])
	}

	// Launch concurrent broadcasts to the same topic.
	workers := 20
	var wg sync.WaitGroup
	wg.Add(workers)

	errs := make(chan error, workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			data := fmt.Appendf(nil, "message-%d", idx)
			err := c.Broadcast(ctx, testTopic, data)
			if err != nil {
				errs <- fmt.Errorf("broadcast %d failed: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Verify no errors occurred.
	for err := range errs {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify all broadcast messages were received by the mock.
	mc.mu.Lock()
	receivedCount := len(mc.broadcastCalls)
	broadcastCalls := make([]broadcastParams, len(mc.broadcastCalls))
	copy(broadcastCalls, mc.broadcastCalls)
	mc.mu.Unlock()

	if receivedCount != workers {
		t.Fatalf("Expected %d broadcast calls, got %d", workers, receivedCount)
	}

	// Verify all messages are present.
	receivedMessages := make(map[string]bool)
	for _, call := range broadcastCalls {
		if call.Topic != testTopic {
			t.Fatalf("Expected topic %s, got %s", testTopic, call.Topic)
		}
		receivedMessages[string(call.Data)] = true
	}

	// Verify we received all unique messages.
	for i := range workers {
		expectedData := fmt.Sprintf("message-%d", i)
		if !receivedMessages[expectedData] {
			t.Fatalf("Expected to receive message %s", expectedData)
		}
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	testTopic := "concurrent-test"
	workers := 10

	// Track which handlers were registered.
	var handlerCalls atomic.Int32
	handlers := make([]func(TopicEvent), workers)
	for i := range workers {
		idx := i
		handlers[idx] = func(TopicEvent) {
			handlerCalls.Add(int32(idx + 1))
		}
	}

	// Launch concurrent subscribe attempts.
	var wg sync.WaitGroup
	wg.Add(workers)

	errs := make(chan error, workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			err := c.Subscribe(ctx, testTopic, handlers[idx])
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Count errors - we expect workers-1 redundant subscription errors.
	var redundantSubErrors int
	for err := range errs {
		if errors.Is(err, ErrRedundantSubscription) {
			redundantSubErrors++
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if redundantSubErrors != workers-1 {
		t.Fatalf("Expected %d redundant subscription errors, got %d", workers-1, redundantSubErrors)
	}

	// Verify only one topic is registered.
	topics := c.topicRegistry.fetchTopics()
	if len(topics) != 1 {
		t.Fatalf("Expected 1 registered topic, got %d", len(topics))
	}

	if topics[0] != testTopic {
		t.Fatalf("Expected topic %s, got %s", testTopic, topics[0])
	}

	// Launch concurrent unsubscribe attempts.
	wg.Add(workers)

	errs = make(chan error, workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			err := c.Unsubscribe(ctx, testTopic)
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Count errors - we expect workers-1 redundant unsubscription errors.
	var redundantUnsubErrors int
	for err := range errs {
		if errors.Is(err, ErrRedundantUnsubscription) {
			redundantUnsubErrors++
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if redundantUnsubErrors != workers-1 {
		t.Fatalf("Expected %d redundant unsubscription errors, got %d", workers-1, redundantUnsubErrors)
	}

	// Verify no topics are registered.
	topics = c.topicRegistry.fetchTopics()
	if len(topics) != 0 {
		t.Fatalf("Expected 0 registered topics, got %d", len(topics))
	}
}

func TestMalformedInput(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	emptyTopic := ""

	// Test 1: Subscribe with empty topic name should be rejected.
	t.Run("Subscribe with empty topic", func(t *testing.T) {
		err := c.Subscribe(ctx, emptyTopic, func(TopicEvent) {})
		if err == nil {
			t.Fatal("Expected error when subscribing with empty topic, got nil")
		}
		if !errors.Is(err, errEmptyTopic) {
			t.Fatalf("Expected ErrEmptyTopic, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		subCallCount := len(mc.subscribeCalls)
		mc.mu.Unlock()

		if subCallCount != 0 {
			t.Fatalf("Expected 0 subscribe calls, got %d", subCallCount)
		}

		// Verify empty topic was not registered.
		topics := c.topicRegistry.fetchTopics()
		for _, topic := range topics {
			if topic == emptyTopic {
				t.Fatal("Empty topic should not be registered")
			}
		}
	})

	// Test 2: Unsubscribe from empty topic should be rejected.
	t.Run("Unsubscribe from empty topic", func(t *testing.T) {
		err := c.Unsubscribe(ctx, emptyTopic)
		if err == nil {
			t.Fatal("Expected error when unsubscribing with empty topic, got nil")
		}
		if !errors.Is(err, errEmptyTopic) {
			t.Fatalf("Expected ErrEmptyTopic, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		unsubCallCount := len(mc.unsubscribeCalls)
		mc.mu.Unlock()

		if unsubCallCount != 0 {
			t.Fatalf("Expected 0 unsubscribe calls, got %d", unsubCallCount)
		}
	})

	// Test 3: Broadcast with empty topic name should be rejected.
	t.Run("Broadcast with empty topic", func(t *testing.T) {
		err := c.Broadcast(ctx, emptyTopic, []byte("test-data"))
		if err == nil {
			t.Fatal("Expected error when broadcasting with empty topic, got nil")
		}
		if !errors.Is(err, errEmptyTopic) {
			t.Fatalf("Expected ErrEmptyTopic, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		broadcastCallCount := len(mc.broadcastCalls)
		mc.mu.Unlock()

		if broadcastCallCount != 0 {
			t.Fatalf("Expected 0 broadcast calls, got %d", broadcastCallCount)
		}
	})

	// Test 4: Subscribe with nil handler should be rejected.
	t.Run("Subscribe with nil handler", func(t *testing.T) {
		validTopic := "valid-topic"
		err := c.Subscribe(ctx, validTopic, nil)
		if err == nil {
			t.Fatal("Expected error when subscribing with nil handler, got nil")
		}
		if !strings.Contains(err.Error(), "handler function cannot be nil") {
			t.Fatalf("Expected a nil handler error, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		subCallCount := len(mc.subscribeCalls)
		mc.mu.Unlock()

		if subCallCount != 0 {
			t.Fatalf("Expected 0 subscribe calls, got %d", subCallCount)
		}

		// Verify the topic was not registered.
		topics := c.topicRegistry.fetchTopics()
		for _, topic := range topics {
			if topic == validTopic {
				t.Fatal("Topic should not be registered after nil handler error")
			}
		}
	})

	// Test 5: Broadcast with nil data should be rejected.
	t.Run("Broadcast with nil data", func(t *testing.T) {
		validTopic := "valid-topic-for-broadcast"
		err := c.Broadcast(ctx, validTopic, nil)
		if err == nil {
			t.Fatal("Expected error when broadcasting with nil data, got nil")
		}
		if !strings.Contains(err.Error(), "data cannot be nil") {
			t.Fatalf("Expected a nil data error, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		broadcastCallCount := len(mc.broadcastCalls)
		mc.mu.Unlock()

		if broadcastCallCount != 0 {
			t.Fatalf("Expected 0 broadcast calls, got %d", broadcastCallCount)
		}
	})
}

func TestSendHandshakeRequest(t *testing.T) {
	ctx := context.Background()

	ourPriv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	ourPub := ourPriv.GetPublic()

	peerPriv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key (peer): %v", err)
	}
	peerID, err := peer.IDFromPrivateKey(peerPriv)
	if err != nil {
		t.Fatalf("peer.IDFromPrivateKey: %v", err)
	}

	var nonce keyID
	copy(nonce[:], []byte("0123456789abcdef"))

	reqEncPubKey := bytes.Repeat([]byte{0x01}, 32)
	respEncPubKey := bytes.Repeat([]byte{0x02}, 32)

	makeClient := func(mc *tMeshConnection) *Client {
		c := &Client{
			cfg:           &Config{PrivateKey: ourPriv},
			topicRegistry: newTopicRegistry(),
		}
		c.setTestMeshConnection(mc)
		return c
	}

	t.Run("success", func(t *testing.T) {
		mc := &tMeshConnection{remotePeer: peerID}

		// Craft a valid, signed handshake response from the peer.
		hsResp := pbHandshakeResponseSuccess(respEncPubKey, nonce[:])
		unsignedResp := proto.Clone(hsResp).(*clientpb.HandshakeResponse)
		unsignedResp.Signature = nil
		unsignedRespPayload, err := proto.Marshal(unsignedResp)
		if err != nil {
			t.Fatalf("marshal unsigned handshake response: %v", err)
		}
		sig, err := peerPriv.Sign(unsignedRespPayload)
		if err != nil {
			t.Fatalf("sign handshake response: %v", err)
		}
		hsResp.Signature = sig
		respBytes, err := proto.Marshal(pbClientResponseHandshake(hsResp))
		if err != nil {
			t.Fatalf("marshal client response: %v", err)
		}
		mc.relayMessageReturns = []relayMessageReturnValue{{resp: respBytes}}

		c := makeClient(mc)
		gotPub, err := c.sendHandshakeRequest(ctx, peerID, nonce, reqEncPubKey)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(gotPub, respEncPubKey) {
			t.Fatalf("expected public key %x, got %x", respEncPubKey, gotPub)
		}

		// Validate the outbound request sent via relayMessage.
		if got := len(mc.relayMessageCalls); got != 1 {
			t.Fatalf("expected 1 relayMessage call, got %d", got)
		}
		if mc.relayMessageCalls[0].PeerID != peerID {
			t.Fatalf("expected relay peerID %q, got %q", peerID, mc.relayMessageCalls[0].PeerID)
		}

		reqEnv := &clientpb.ClientRequest{}
		if err := proto.Unmarshal(mc.relayMessageCalls[0].Message, reqEnv); err != nil {
			t.Fatalf("unmarshal client request: %v", err)
		}
		hsReq := reqEnv.GetHandshakeRequest()
		if hsReq == nil {
			t.Fatalf("expected handshake request")
		}
		if !bytes.Equal(hsReq.GetNonce(), nonce[:]) {
			t.Fatalf("expected nonce %x, got %x", nonce[:], hsReq.GetNonce())
		}
		if !bytes.Equal(hsReq.GetPublicKey(), reqEncPubKey) {
			t.Fatalf("expected public key %x, got %x", reqEncPubKey, hsReq.GetPublicKey())
		}

		// Verify the client's signature on the request.
		unsignedReq := proto.Clone(hsReq).(*clientpb.HandshakeRequest)
		unsignedReq.Signature = nil
		unsignedReqPayload, err := proto.Marshal(unsignedReq)
		if err != nil {
			t.Fatalf("marshal unsigned handshake request: %v", err)
		}
		ok, err := ourPub.Verify(unsignedReqPayload, hsReq.GetSignature())
		if err != nil {
			t.Fatalf("verify signature: %v", err)
		}
		if !ok {
			t.Fatalf("expected handshake request signature to be valid")
		}
	})

	t.Run("peer error response", func(t *testing.T) {
		mc := &tMeshConnection{remotePeer: peerID}

		hsResp := pbHandshakeResponseError("boom", nonce[:])
		respBytes, err := proto.Marshal(pbClientResponseHandshake(hsResp))
		if err != nil {
			t.Fatalf("marshal client response: %v", err)
		}
		mc.relayMessageReturns = []relayMessageReturnValue{{resp: respBytes}}

		c := makeClient(mc)
		_, err = c.sendHandshakeRequest(ctx, peerID, nonce, reqEncPubKey)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "handshake response error") || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid peer signature", func(t *testing.T) {
		mc := &tMeshConnection{remotePeer: peerID}

		hsResp := pbHandshakeResponseSuccess(respEncPubKey, nonce[:])
		hsResp.Signature = []byte("definitely-not-a-valid-signature")
		respBytes, err := proto.Marshal(pbClientResponseHandshake(hsResp))
		if err != nil {
			t.Fatalf("marshal client response: %v", err)
		}
		mc.relayMessageReturns = []relayMessageReturnValue{{resp: respBytes}}

		c := makeClient(mc)
		_, err = c.sendHandshakeRequest(ctx, peerID, nonce, reqEncPubKey)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "invalid handshake response signature") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestMessagePeer(t *testing.T) {
	ctx := context.Background()

	peerID := peer.ID("peer")
	msg := []byte("hello")

	makeClient := func(mc *tMeshConnection, em *tEncryptionManager) *Client {
		c := &Client{
			cfg:               &Config{},
			topicRegistry:     newTopicRegistry(),
			encryptionManager: em,
		}
		c.setTestMeshConnection(mc)
		return c
	}

	t.Run("success", func(t *testing.T) {
		mc := &tMeshConnection{remotePeer: peerID}
		em := &tEncryptionManager{}

		encryptedReq := []byte("enc-req")
		encryptedResp := []byte("enc-resp")
		plaintextResp := []byte("plain-resp")

		var encID keyID
		copy(encID[:], []byte("enc-id-000000000"))

		em.encryptPeerMessageResp = encryptedReq
		em.encryptPeerMessageID = encID
		em.decryptPeerMessageResp = plaintextResp

		respBytes, err := proto.Marshal(pbClientResponseMessage(pbMessageResponseSuccess(encryptedResp)))
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		mc.relayMessageReturns = []relayMessageReturnValue{{resp: respBytes}}

		c := makeClient(mc, em)
		got, err := c.MessagePeer(ctx, peerID, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, plaintextResp) {
			t.Fatalf("expected %q, got %q", string(plaintextResp), string(got))
		}

		if gotN := len(em.decryptPeerMessageCalls); gotN != 1 {
			t.Fatalf("expected 1 decrypt call, got %d", gotN)
		}
		if em.decryptPeerMessageCalls[0].PeerID != peerID {
			t.Fatalf("expected decrypt peer %q, got %q", peerID, em.decryptPeerMessageCalls[0].PeerID)
		}
		if !bytes.Equal(em.decryptPeerMessageCalls[0].Message, encryptedResp) {
			t.Fatalf("expected decrypt message %x, got %x", encryptedResp, em.decryptPeerMessageCalls[0].Message)
		}
	})

	t.Run("peer reports no encryption key -> clear and retry", func(t *testing.T) {
		mc := &tMeshConnection{remotePeer: peerID}
		em := &tEncryptionManager{}

		encryptedReq := []byte("enc-req")
		encryptedResp := []byte("enc-resp")
		plaintextResp := []byte("plain-resp")

		var encID keyID
		copy(encID[:], []byte("enc-id-000000000"))

		em.encryptPeerMessageResp = encryptedReq
		em.encryptPeerMessageID = encID
		em.decryptPeerMessageResp = plaintextResp

		noKeyRespBytes, err := proto.Marshal(pbClientResponseMessage(pbMessageResponseNoEncryptionKey()))
		if err != nil {
			t.Fatalf("marshal no-key response: %v", err)
		}
		successRespBytes, err := proto.Marshal(pbClientResponseMessage(pbMessageResponseSuccess(encryptedResp)))
		if err != nil {
			t.Fatalf("marshal success response: %v", err)
		}
		mc.relayMessageReturns = []relayMessageReturnValue{
			{resp: noKeyRespBytes},
			{resp: successRespBytes},
		}

		c := makeClient(mc, em)
		got, err := c.MessagePeer(ctx, peerID, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, plaintextResp) {
			t.Fatalf("expected %q, got %q", string(plaintextResp), string(got))
		}

		if gotN := len(mc.relayMessageCalls); gotN != 2 {
			t.Fatalf("expected 2 relay calls, got %d", gotN)
		}
		if gotN := len(em.clearEncryptionKeyCalls); gotN != 1 {
			t.Fatalf("expected 1 clearEncryptionKey call, got %d", gotN)
		}
		if em.clearEncryptionKeyCalls[0].PeerID != peerID {
			t.Fatalf("expected clear peer %q, got %q", peerID, em.clearEncryptionKeyCalls[0].PeerID)
		}
		if em.clearEncryptionKeyCalls[0].ID != encID {
			t.Fatalf("expected clear keyID %x, got %x", encID, em.clearEncryptionKeyCalls[0].ID)
		}
	})

	t.Run("peer returns other error -> no retry", func(t *testing.T) {
		mc := &tMeshConnection{remotePeer: peerID}
		em := &tEncryptionManager{}

		encryptedReq := []byte("enc-req")
		em.encryptPeerMessageResp = encryptedReq

		errRespBytes, err := proto.Marshal(pbClientResponseMessage(pbMessageResponseError("nope")))
		if err != nil {
			t.Fatalf("marshal error response: %v", err)
		}
		mc.relayMessageReturns = []relayMessageReturnValue{{resp: errRespBytes}}

		c := makeClient(mc, em)
		_, err = c.MessagePeer(ctx, peerID, msg)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "peer error: nope") {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotN := len(mc.relayMessageCalls); gotN != 1 {
			t.Fatalf("expected 1 relay call, got %d", gotN)
		}
		if gotN := len(em.clearEncryptionKeyCalls); gotN != 0 {
			t.Fatalf("expected 0 clearEncryptionKey calls, got %d", gotN)
		}
	})
}

func TestHandlePeerMessage(t *testing.T) {
	newHost := func(t *testing.T) (host.Host, crypto.PrivKey) {
		t.Helper()
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatalf("GenerateEd25519Key: %v", err)
		}
		h, err := libp2p.New(
			libp2p.Identity(priv),
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		)
		if err != nil {
			t.Fatalf("libp2p.New: %v", err)
		}
		return h, priv
	}

	connect := func(t *testing.T, ctx context.Context, a, b host.Host) {
		t.Helper()
		a.Peerstore().AddAddrs(b.ID(), b.Addrs(), time.Minute)
		if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
			t.Fatalf("connect: %v", err)
		}
	}

	readClientResponse := func(t *testing.T, s network.Stream) *clientpb.ClientResponse {
		t.Helper()
		resp := &protocolsPb.TatankaRelayMessageResponse{}
		if err := codec.ReadLengthPrefixedMessage(s, resp); err != nil {
			t.Fatalf("read relay response: %v", err)
		}
		msg := resp.GetMessage()
		if len(msg) == 0 {
			t.Fatalf("expected relay response message payload")
		}
		env := &clientpb.ClientResponse{}
		if err := proto.Unmarshal(msg, env); err != nil {
			t.Fatalf("unmarshal client response: %v", err)
		}
		return env
	}

	logger := slog.NewBackend(io.Discard).Logger("client_test")

	t.Run("handshake request -> handleHandshakeRequest called, response sent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		srvHost, srvPriv := newHost(t)
		defer func() { _ = srvHost.Close() }()

		peerHost, peerPriv := newHost(t)
		defer func() { _ = peerHost.Close() }()

		connect(t, ctx, peerHost, srvHost)

		em := &tEncryptionManager{}
		ourEncPub := bytes.Repeat([]byte{0xAA}, 32)
		em.handleHandshakeRequestResp = ourEncPub

		c := &Client{
			cfg:               &Config{PrivateKey: srvPriv, Logger: logger},
			host:              srvHost,
			log:               logger,
			topicRegistry:     newTopicRegistry(),
			encryptionManager: em,
		}

		if err := c.HandlePeerMessage(func([]byte) ([]byte, error) { return nil, nil }); err != nil {
			t.Fatalf("HandlePeerMessage: %v", err)
		}

		// Build a properly signed handshake request from the peer.
		var nonce keyID
		copy(nonce[:], []byte("0123456789abcdef"))
		peerEncPub := bytes.Repeat([]byte{0xBB}, 32)
		hsReq := &clientpb.HandshakeRequest{
			Nonce:     nonce[:],
			PublicKey: peerEncPub,
		}
		unsigned := proto.Clone(hsReq).(*clientpb.HandshakeRequest)
		unsigned.Signature = nil
		unsignedPayload, err := proto.Marshal(unsigned)
		if err != nil {
			t.Fatalf("marshal unsigned hs req: %v", err)
		}
		sig, err := peerPriv.Sign(unsignedPayload)
		if err != nil {
			t.Fatalf("sign hs req: %v", err)
		}
		hsReq.Signature = sig

		reqPayload, err := proto.Marshal(pbClientRequestHandshake(hsReq))
		if err != nil {
			t.Fatalf("marshal client request: %v", err)
		}

		s, err := peerHost.NewStream(ctx, srvHost.ID(), protocols.TatankaRelayMessageProtocol)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		defer func() { _ = s.Close() }()

		if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.TatankaRelayMessageRequest{
			PeerID:  []byte(peerHost.ID()),
			Message: reqPayload,
		}); err != nil {
			t.Fatalf("write relay request: %v", err)
		}

		env := readClientResponse(t, s)
		hsResp := env.GetHandshakeResponse()
		if hsResp == nil {
			t.Fatalf("expected handshake response")
		}
		if hsResp.GetError() != nil {
			t.Fatalf("unexpected handshake error: %v", hsResp.GetError().GetMessage())
		}
		if !bytes.Equal(hsResp.GetNonce(), nonce[:]) {
			t.Fatalf("expected nonce %x, got %x", nonce[:], hsResp.GetNonce())
		}
		if !bytes.Equal(hsResp.GetPublicKey(), ourEncPub) {
			t.Fatalf("expected public key %x, got %x", ourEncPub, hsResp.GetPublicKey())
		}
		if len(hsResp.GetSignature()) == 0 {
			t.Fatalf("expected handshake response signature")
		}

		// Ensure handleHandshakeRequest was called with correct args.
		if got := len(em.handleHandshakeRequestCalls); got != 1 {
			t.Fatalf("expected 1 handleHandshakeRequest call, got %d", got)
		}
		call := em.handleHandshakeRequestCalls[0]
		if call.PeerID != peerHost.ID() {
			t.Fatalf("expected peerID %q, got %q", peerHost.ID(), call.PeerID)
		}
		if !bytes.Equal(call.Nonce, nonce[:]) {
			t.Fatalf("expected nonce %x, got %x", nonce[:], call.Nonce)
		}
		if !bytes.Equal(call.PubKey, peerEncPub) {
			t.Fatalf("expected pubkey %x, got %x", peerEncPub, call.PubKey)
		}
	})

	t.Run("message request -> decrypt ok, handler called, encrypt called", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		srvHost, srvPriv := newHost(t)
		defer func() { _ = srvHost.Close() }()

		peerHost, _ := newHost(t)
		defer func() { _ = peerHost.Close() }()

		connect(t, ctx, peerHost, srvHost)

		em := &tEncryptionManager{}
		encryptedReq := []byte("encrypted-req")
		plaintextReq := []byte("plaintext-req")
		plaintextResp := []byte("plaintext-resp")
		encryptedResp := []byte("encrypted-resp")

		var nonce keyID
		copy(nonce[:], []byte("0123456789abcdef"))
		em.decryptPeerMessageResp = plaintextReq
		em.decryptPeerMessageID = nonce
		em.encryptPeerMessageResp = encryptedResp

		handlerCalls := 0
		handler := func(b []byte) ([]byte, error) {
			handlerCalls++
			if !bytes.Equal(b, plaintextReq) {
				t.Fatalf("expected handler input %q, got %q", string(plaintextReq), string(b))
			}
			return plaintextResp, nil
		}

		c := &Client{
			cfg:               &Config{PrivateKey: srvPriv, Logger: logger},
			host:              srvHost,
			log:               logger,
			topicRegistry:     newTopicRegistry(),
			encryptionManager: em,
		}
		if err := c.HandlePeerMessage(handler); err != nil {
			t.Fatalf("HandlePeerMessage: %v", err)
		}

		reqPayload, err := proto.Marshal(pbClientRequestMessage(encryptedReq))
		if err != nil {
			t.Fatalf("marshal client request: %v", err)
		}

		s, err := peerHost.NewStream(ctx, srvHost.ID(), protocols.TatankaRelayMessageProtocol)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		defer func() { _ = s.Close() }()

		if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.TatankaRelayMessageRequest{
			PeerID:  []byte(peerHost.ID()),
			Message: reqPayload,
		}); err != nil {
			t.Fatalf("write relay request: %v", err)
		}

		env := readClientResponse(t, s)
		msgResp := env.GetMessageResponse()
		if msgResp == nil {
			t.Fatalf("expected message response")
		}
		if msgResp.GetError() != nil {
			t.Fatalf("unexpected message error: %v", msgResp.GetError().GetMessage())
		}
		if !bytes.Equal(msgResp.GetMessage(), encryptedResp) {
			t.Fatalf("expected encrypted resp %q, got %q", string(encryptedResp), string(msgResp.GetMessage()))
		}
		if handlerCalls != 1 {
			t.Fatalf("expected handlerCalls=1, got %d", handlerCalls)
		}
		if got := len(em.decryptPeerMessageCalls); got != 1 {
			t.Fatalf("expected 1 decrypt call, got %d", got)
		}
		if got := len(em.encryptPeerMessageCalls); got != 1 {
			t.Fatalf("expected 1 encrypt call, got %d", got)
		}
		if !bytes.Equal(em.encryptPeerMessageCalls[0].Message, plaintextResp) {
			t.Fatalf("expected encrypt input %q, got %q", string(plaintextResp), string(em.encryptPeerMessageCalls[0].Message))
		}
		if em.encryptPeerMessageCalls[0].Nonce != nonce {
			t.Fatalf("expected encrypt nonce %x, got %x", nonce, em.encryptPeerMessageCalls[0].Nonce)
		}
	})

	t.Run("message request -> unknown key id -> no encryption key error response", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		srvHost, srvPriv := newHost(t)
		defer func() { _ = srvHost.Close() }()

		peerHost, _ := newHost(t)
		defer func() { _ = peerHost.Close() }()

		connect(t, ctx, peerHost, srvHost)

		em := &tEncryptionManager{}
		em.decryptPeerMessageErr = ErrUnknownKeyID

		handlerCalls := 0
		handler := func(b []byte) ([]byte, error) {
			handlerCalls++
			return nil, nil
		}

		c := &Client{
			cfg:               &Config{PrivateKey: srvPriv, Logger: logger},
			host:              srvHost,
			log:               logger,
			topicRegistry:     newTopicRegistry(),
			encryptionManager: em,
		}
		if err := c.HandlePeerMessage(handler); err != nil {
			t.Fatalf("HandlePeerMessage: %v", err)
		}

		reqPayload, err := proto.Marshal(pbClientRequestMessage([]byte("encrypted-req")))
		if err != nil {
			t.Fatalf("marshal client request: %v", err)
		}

		s, err := peerHost.NewStream(ctx, srvHost.ID(), protocols.TatankaRelayMessageProtocol)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		defer func() { _ = s.Close() }()

		if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.TatankaRelayMessageRequest{
			PeerID:  []byte(peerHost.ID()),
			Message: reqPayload,
		}); err != nil {
			t.Fatalf("write relay request: %v", err)
		}

		env := readClientResponse(t, s)
		msgResp := env.GetMessageResponse()
		if msgResp == nil {
			t.Fatalf("expected message response")
		}
		if msgResp.GetError() == nil || msgResp.GetError().GetNoEncryptionKeyError() == nil {
			t.Fatalf("expected no-encryption-key error response")
		}
		if handlerCalls != 0 {
			t.Fatalf("expected handler not to be called, got %d", handlerCalls)
		}
		if got := len(em.encryptPeerMessageCalls); got != 0 {
			t.Fatalf("expected encrypt not to be called, got %d", got)
		}
	})

	t.Run("invalid message type -> error response", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		srvHost, srvPriv := newHost(t)
		defer func() { _ = srvHost.Close() }()

		peerHost, _ := newHost(t)
		defer func() { _ = peerHost.Close() }()

		connect(t, ctx, peerHost, srvHost)

		em := &tEncryptionManager{}
		c := &Client{
			cfg:               &Config{PrivateKey: srvPriv, Logger: logger},
			host:              srvHost,
			log:               logger,
			topicRegistry:     newTopicRegistry(),
			encryptionManager: em,
		}
		if err := c.HandlePeerMessage(func([]byte) ([]byte, error) { return nil, nil }); err != nil {
			t.Fatalf("HandlePeerMessage: %v", err)
		}

		// ClientRequest with no oneof set triggers the default case.
		reqPayload, err := proto.Marshal(&clientpb.ClientRequest{})
		if err != nil {
			t.Fatalf("marshal client request: %v", err)
		}

		s, err := peerHost.NewStream(ctx, srvHost.ID(), protocols.TatankaRelayMessageProtocol)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		defer func() { _ = s.Close() }()

		if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.TatankaRelayMessageRequest{
			PeerID:  []byte(peerHost.ID()),
			Message: reqPayload,
		}); err != nil {
			t.Fatalf("write relay request: %v", err)
		}

		env := readClientResponse(t, s)
		msgResp := env.GetMessageResponse()
		if msgResp == nil {
			t.Fatalf("expected message response")
		}
		if msgResp.GetError() == nil || msgResp.GetError().GetMessage() == "" {
			t.Fatalf("expected error message")
		}
		if !strings.Contains(msgResp.GetError().GetMessage(), "unknown client request type") {
			t.Fatalf("unexpected error message: %q", msgResp.GetError().GetMessage())
		}
	})
}
