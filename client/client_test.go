package client

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
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

	broadcastCalls []broadcastParams
	broadcastErr   error

	subscribeCalls []string
	subscribeErr   error

	unsubscribeCalls []string
	unsubscribeErr   error

	postBondCalls []struct{}
	postBondErr   error

	KillCalls int
}

var _ meshConn = (*tMeshConnection)(nil)

func (m *tMeshConnection) remotePeerID() peer.ID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remotePeer
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

func TestSubscribe(t *testing.T) {
	ctx := context.Background()

	mc := &tMeshConnection{
		remotePeer: peer.ID("peer"),
	}

	c := &Client{
		cfg:           &Config{},
		topicRegistry: newTopicRegistry(),
		connections:   make(map[peer.ID]meshConn),
	}
	c.setPrimaryMeshConnection(mc)

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
	if c.topicRegistry.isRegistered("topic-2") {
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
	if !c.topicRegistry.isRegistered("topic-1") {
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
	if c.topicRegistry.isRegistered("topic-1") {
		t.Fatalf("topic-1 should be unregistered after successful unsubscribe")
	}

	// Unsubscribe again should be a no-op: no error and no mesh connection call.
	err = c.Unsubscribe(ctx, "topic-1")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := len(mc.unsubscribeCalls); got != 2 {
		t.Fatalf("expected unsubscribe call count to remain 2, got %d", got)
	}
}

func TestPushMessage(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := &tMeshConnection{
		remotePeer: peer.ID("peer"),
	}

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		connections:   make(map[peer.ID]meshConn),
		log:           logger,
	}
	c.setPrimaryMeshConnection(mc)

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
