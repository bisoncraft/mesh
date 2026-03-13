//go:build harness

package client

// Client tests expect the tatanka test harness to be running a node.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sync"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/bisoncraft/mesh/bond"
)

func TestMain(m *testing.M) {
	m.Run()
}

// fetchNodeAddresses returns all node addresses from the test harness whitelist.
func fetchNodeAddresses() ([]string, error) {
	curUser, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve current user: %w", err)
	}

	whitelistPath := fmt.Sprintf("%s/%s", curUser.HomeDir, ".tatanka-test/whitelist.json")
	whitelist := struct {
		Peers []struct {
			ID      string `json:"id"`
			Address string `json:"address"`
		} `json:"peers"`
	}{}

	data, err := os.ReadFile(whitelistPath)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &whitelist); err != nil {
		return nil, err
	}

	addrs := make([]string, 0, len(whitelist.Peers))
	for _, node := range whitelist.Peers {
		if node.Address == "" || node.ID == "" {
			continue
		}
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", node.Address, node.ID))
	}
	return addrs, nil
}

// cd tatanka-mesh
// go test -v -tags=harness -run=TestClientIntegration ./client
func TestClientIntegration(t *testing.T) {
	logBackend := slog.NewBackend(os.Stdout)
	loggerC1 := logBackend.Logger("c1")
	loggerC1.SetLevel(slog.LevelDebug)
	loggerC2 := logBackend.Logger("c2")
	loggerC2.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c1Port := 12366
	c2Port := 12367

	nodeAddrs, err := fetchNodeAddresses()
	if err != nil {
		t.Fatal(err)
	}

	if len(nodeAddrs) == 0 {
		t.Fatal("no node addresses found in manifest")
	}

	nodeAddr := nodeAddrs[0]
	t.Logf("node address is: %s", nodeAddr)

	c1Priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	c2Priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	c1Cfg := Config{
		RemotePeerAddrs: []string{nodeAddr},
		Port:            c1Port,
		PrivateKey:      c1Priv,
		Logger:          loggerC1,
	}

	c2Cfg := Config{
		RemotePeerAddrs: []string{nodeAddr},
		Port:            c2Port,
		PrivateKey:      c2Priv,
		Logger:          loggerC2,
	}

	// Create two clients (c1 & c2) and connect to the node.
	c1, err := NewClient(&c1Cfg)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := NewClient(&c2Cfg)
	if err != nil {
		t.Fatal(err)
	}

	bonds := []*bond.BondParams{}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := c1.Run(ctx, bonds)
		if err != nil {
			c1.cfg.Logger.Errorf("unexpected error running c1: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := c2.Run(ctx, bonds)
		if err != nil {
			c1.cfg.Logger.Errorf("unexpected error running c2: %v", err)
		}
	}()

	// Wait briefly for initialization.
	time.Sleep(time.Second)

	// Ensure both clients can subscribe to a topic.
	pingTopic := "ping"
	makePingHandler := func(c *Client, received chan TopicEvent) func(TopicEvent) {
		return func(evt TopicEvent) {
			senderID := evt.Peer
			switch evt.Type {
			case TopicEventPeerSubscribed:
				c.cfg.Logger.Infof("%s: received ping topic subscription event, triggered by %s", c.host.ID(), senderID)
			case TopicEventPeerUnsubscribed:
				c.cfg.Logger.Infof("%s: received ping topic unsubscribe event, triggered by %s", c.host.ID(), senderID)
			case TopicEventData:
				c.cfg.Logger.Infof("%s: received broadcast from %s", c.host.ID(), senderID)
			}
			received <- evt
		}
	}

	c1ReceivedEvts := make(chan TopicEvent, 10)
	c1PingHandler := makePingHandler(c1, c1ReceivedEvts)

	c2ReceivedEvts := make(chan TopicEvent, 10)
	c2PingHandler := makePingHandler(c2, c2ReceivedEvts)

	receiveWithTimeout := func(ch <-chan TopicEvent) TopicEvent {
		select {
		case evt := <-ch:
			return evt
		case <-time.After(time.Second * 2):
			t.Fatal("Timed out waiting for push message")
			return TopicEvent{}
		}
	}

	// Subscribe to the ping topic for both c1 and c2.
	err = c1.Subscribe(ctx, pingTopic, c1PingHandler)
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic for c1: %v", err)
	}

	// Wait briefly before subscribing c2.
	time.Sleep(time.Millisecond * 100)

	err = c2.Subscribe(ctx, pingTopic, c2PingHandler)
	if err != nil {
		t.Fatalf("unexpected error subscribing to topic for c2: %v", err)
	}

	// Ensure c1 gets the topic subscription event for c2.
	evt := receiveWithTimeout(c1ReceivedEvts)
	if evt.Type != TopicEventPeerSubscribed {
		t.Fatalf("Expected a topic subscription event , got %v", evt.Type)
	}

	if !bytes.Equal([]byte(evt.Peer), []byte(c2.host.ID())) {
		t.Fatalf("Expected subscription event peer to be %s, got %s", c2.host.ID().String(), evt.Peer.String())
	}

	// Ensure there are no outstanding messages to be processed for c1 and c2.
	if len(c1ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding events for c1, got %d", len(c1ReceivedEvts))
	}

	if len(c2ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c2, got %d", len(c2ReceivedEvts))
	}

	pongData := []byte("pong")

	// Ensure c2 receives a broadcast from c1.
	err = c1.Broadcast(ctx, pingTopic, []byte(pongData))
	if err != nil {
		t.Fatalf("Unexpected error broadcasting from c1, %v", err)
	}

	evt = receiveWithTimeout(c2ReceivedEvts)
	if evt.Type != TopicEventData {
		t.Fatalf("Expected a broadcast event , got %v", evt.Type)
	}

	if !bytes.Equal([]byte(evt.Peer), []byte(c1.host.ID())) {
		t.Fatalf("Expected broadcast event peer to be %s, got %s", c1.host.ID().String(), evt.Peer.String())
	}

	if string(evt.Data) != string(pongData) {
		t.Fatalf("Expected broadcasted data to be %s, got %s", pongData, string(evt.Data))
	}

	// Ensure c2 does not receive its own broadcast.
	if len(c1ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c1, got %d", len(c1ReceivedEvts))
	}

	// Ensure c1 receives c2s unsubscription notification.
	err = c2.Unsubscribe(ctx, pingTopic)
	if err != nil {
		t.Fatalf("Unexpected error unsubscribing from ping topic by c2 %v", err)
	}

	evt = receiveWithTimeout(c1ReceivedEvts)
	if evt.Type != TopicEventPeerUnsubscribed {
		t.Fatalf("Expected a topic unsubscribe event , got %v", evt.Type)
	}

	if !bytes.Equal([]byte(evt.Peer), []byte(c2.host.ID())) {
		t.Fatalf("Expected unsubscribe event peer to be %s, got %s", c2.host.ID().String(), evt.Peer.String())
	}

	// Ensure there are no outstanding messages to be processed for c1 and c2.
	if len(c1ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c1, got %d", len(c1ReceivedEvts))
	}

	if len(c2ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c2, got %d", len(c2ReceivedEvts))
	}

	// Ensure the c1 and c2 can post bonds.
	err = c1.PostBond(ctx)
	if err != nil {
		t.Fatalf("Unexpected error posting bonds for c1: %v", err)
	}

	err = c2.PostBond(ctx)
	if err != nil {
		t.Fatalf("Unexpected error posting bonds for c2: %v", err)
	}

	// TODO: add relay test between c1 and c2.

	cancel()
	wg.Wait()
}

func TestPeerMessaging(t *testing.T) {
	nodeAddrs, err := fetchNodeAddresses()
	if err != nil {
		t.Fatal(err)
	}

	if len(nodeAddrs) == 0 {
		t.Fatal("no node addresses found in whitelist")
	}

	logBackend := slog.NewBackend(os.Stdout)
	loggerC1 := logBackend.Logger("c1")
	loggerC1.SetLevel(slog.LevelDebug)
	loggerC2 := logBackend.Logger("c2")
	loggerC2.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c1Priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	c2Priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	// Both clients can connect to the same tatanka node. If multiple
	// nodes are available, use different ones for each client.
	c2Addr := nodeAddrs[0]
	if len(nodeAddrs) > 1 {
		c2Addr = nodeAddrs[1]
	}

	c1Cfg := Config{
		RemotePeerAddrs: []string{nodeAddrs[0]},
		Port:            12376,
		PrivateKey:      c1Priv,
		Logger:          loggerC1,
	}

	c2Cfg := Config{
		RemotePeerAddrs: []string{c2Addr},
		Port:            12377,
		PrivateKey:      c2Priv,
		Logger:          loggerC2,
	}

	c1, err := NewClient(&c1Cfg)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := NewClient(&c2Cfg)
	if err != nil {
		t.Fatal(err)
	}

	bonds := []*bond.BondParams{}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := c1.Run(ctx, bonds); err != nil {
			c1.cfg.Logger.Errorf("unexpected error running c1: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := c2.Run(ctx, bonds); err != nil {
			c2.cfg.Logger.Errorf("unexpected error running c2: %v", err)
		}
	}()

	// Wait briefly for initialization.
	time.Sleep(time.Second)

	c1MsgCh := make(chan []byte, 1)
	c2MsgCh := make(chan []byte, 1)

	if err := c1.HandlePeerMessage(func(msg []byte) ([]byte, error) {
		c1MsgCh <- msg
		return []byte("ack from c1"), nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := c2.HandlePeerMessage(func(msg []byte) ([]byte, error) {
		c2MsgCh <- msg
		return []byte("ack from c2"), nil
	}); err != nil {
		t.Fatal(err)
	}

	recvWithTimeout := func(ch <-chan []byte) []byte {
		select {
		case v := <-ch:
			return v
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for peer message")
			return nil
		}
	}

	// c1 -> c2
	msgToC2 := []byte("hello from c1")
	resp, err := c1.MessagePeer(ctx, c2.host.ID(), msgToC2)
	if err != nil {
		t.Fatalf("c1 -> c2 message failed: %v", err)
	}
	receivedByC2 := recvWithTimeout(c2MsgCh)
	if !bytes.Equal(receivedByC2, msgToC2) {
		t.Fatalf("c2 received wrong message: got %s, want %s", string(receivedByC2), string(msgToC2))
	}
	if string(resp) != "ack from c2" {
		t.Fatalf("c1 expected ack from c2, got %s", string(resp))
	}

	// c2 -> c1
	msgToC1 := []byte("hello from c2")
	resp, err = c2.MessagePeer(ctx, c1.host.ID(), msgToC1)
	if err != nil {
		t.Fatalf("c2 -> c1 message failed: %v", err)
	}
	receivedByC1 := recvWithTimeout(c1MsgCh)
	if !bytes.Equal(receivedByC1, msgToC1) {
		t.Fatalf("c1 received wrong message: got %s, want %s", string(receivedByC1), string(msgToC1))
	}
	if string(resp) != "ack from c1" {
		t.Fatalf("c2 expected ack from c1, got %s", string(resp))
	}

	cancel()
	wg.Wait()
}
