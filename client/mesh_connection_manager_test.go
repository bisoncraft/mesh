package client

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshConnectionManagerFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := libp2p.New()
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer func() { _ = h.Close() }()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("mesh-conn-manager-test")

	node1ID := randomPeerID(t)
	node2ID := randomPeerID(t)
	node1Addr := ma.StringCast("/ip4/127.0.0.1/tcp/10001")
	node2Addr := ma.StringCast("/ip4/127.0.0.1/tcp/10002")
	node1Info := peer.AddrInfo{ID: node1ID, Addrs: []ma.Multiaddr{node1Addr}}
	node2Info := peer.AddrInfo{ID: node2ID, Addrs: []ma.Multiaddr{node2Addr}}

	conn1 := newTMeshConnection(node1ID)
	conn1.fetchNodes = []peer.AddrInfo{node1Info, node2Info}
	conn2 := newTMeshConnection(node2ID)

	node1Available := true
	connFactory := func(peerID peer.ID) meshConn {
		switch peerID {
		case node1ID:
			if node1Available {
				return conn1
			}
			conn := newTMeshConnection(node1ID)
			conn.fail(errors.New("node-1 unavailable"))
			return conn
		case node2ID:
			return conn2
		default:
			t.Fatalf("unexpected peer ID %s", peerID)
			return nil
		}
	}

	m := newMeshConnectionManager(&meshConnectionManagerConfig{
		host:           h,
		log:            logger,
		connFactory:    connFactory,
		bootstrapPeers: []peer.AddrInfo{node1Info},
	})

	done := make(chan struct{})
	go func() {
		m.run(ctx)
		close(done)
	}()

	// wait for the primary connection to be set to the expected peer ID
	waitForPrimary := func(expected peer.ID) {
		t.Helper()
		requireEventually(t, func() bool {
			c, err := m.primaryConnection()
			if err != nil {
				return false
			}
			return c.remotePeerID() == expected
		}, 2*time.Second, 10*time.Millisecond, "primary connection not set to %s", expected)
	}

	waitForPrimary(node1ID)

	requireEventually(t, func() bool {
		addrs := h.Peerstore().Addrs(node2ID)
		return len(addrs) > 0
	}, 2*time.Second, 10*time.Millisecond, "peerstore missing addresses for node 2")

	node1Available = false
	conn1.fail(errors.New("node-1 down"))

	waitForPrimary(node2ID)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mesh connection manager did not stop")
	}
}

func TestConnectionStateFeed(t *testing.T) {
	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("connection-state-feed-test")

	t.Run("initial state connected", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})
		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		ch, _ := m.subscribeToConnectionState()
		state := <-ch
		if state != true {
			t.Fatalf("expected initial state true, got %v", state)
		}
	})

	t.Run("initial state disconnected", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ch, _ := m.subscribeToConnectionState()
		state := <-ch
		if state != false {
			t.Fatalf("expected initial state false, got %v", state)
		}
	})

	t.Run("state change to connected", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ch, _ := m.subscribeToConnectionState()
		// Discard initial state
		<-ch

		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		state := <-ch
		if state != true {
			t.Fatalf("expected state true after connect, got %v", state)
		}
	})

	t.Run("state change to disconnected", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})
		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		ch, _ := m.subscribeToConnectionState()
		// Discard initial state
		<-ch

		m.setPrimaryConnection(nil)

		state := <-ch
		if state != false {
			t.Fatalf("expected state false after disconnect, got %v", state)
		}
	})

	t.Run("multiple subscribers all notified", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ch1, _ := m.subscribeToConnectionState()
		ch2, _ := m.subscribeToConnectionState()

		// Discard initial states
		<-ch1
		<-ch2

		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		state1 := <-ch1
		state2 := <-ch2

		if state1 != true || state2 != true {
			t.Fatalf("expected both subscribers to receive true, got %v and %v", state1, state2)
		}
	})

	t.Run("manual unsubscribe removes subscriber", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ch, id := m.subscribeToConnectionState()
		// Discard initial state
		<-ch

		// Unsubscribe
		m.unsubscribeFromConnectionState(id)
		time.Sleep(10 * time.Millisecond)

		// Try to read from channel; should be closed
		_, ok := <-ch
		if ok {
			t.Fatal("expected channel to be closed after unsubscribe")
		}

		// Verify subscriber was removed
		m.subsMtx.Lock()
		_, exists := m.subs[id]
		m.subsMtx.Unlock()
		if exists {
			t.Fatal("subscriber should be removed")
		}
	})

	t.Run("subscriber removed does not cause panic on state change", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ch, id := m.subscribeToConnectionState()
		// Discard initial state
		<-ch

		// Unsubscribe
		m.unsubscribeFromConnectionState(id)

		// This should not panic even though subscriber is removed
		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn) // Should succeed without panic
	})
}
