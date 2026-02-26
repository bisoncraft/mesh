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

func TestWaitForConnection(t *testing.T) {
	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("wait-for-connection-test")

	t.Run("already connected returns immediately", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})
		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := m.waitForConnection(ctx)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if elapsed > 50*time.Millisecond {
			t.Fatalf("expected immediate return, took %v", elapsed)
		}
	})

	t.Run("waits for connection when disconnected", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Start waiter in goroutine
		done := make(chan error, 1)
		go func() {
			done <- m.waitForConnection(ctx)
		}()

		// Wait a bit to ensure goroutine is blocking
		time.Sleep(100 * time.Millisecond)

		// Connect
		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		// Waiter should return
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not return after connection")
		}
	})

	t.Run("multiple concurrent waiters all wake up", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Start multiple waiters
		numWaiters := 5
		done := make(chan error, numWaiters)
		for i := 0; i < numWaiters; i++ {
			go func() {
				done <- m.waitForConnection(ctx)
			}()
		}

		// Wait a bit to ensure all goroutines are blocking
		time.Sleep(100 * time.Millisecond)

		// Connect
		conn := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn)

		// All waiters should return
		for i := 0; i < numWaiters; i++ {
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("waiter %d: expected no error, got %v", i, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("waiter %d did not return", i)
			}
		}
	})

	t.Run("context cancellation returns error", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ctx, cancel := context.WithCancel(context.Background())

		// Start waiter in goroutine
		done := make(chan error, 1)
		go func() {
			done <- m.waitForConnection(ctx)
		}()

		// Wait a bit to ensure goroutine is blocking
		time.Sleep(100 * time.Millisecond)

		// Cancel context
		cancel()

		// Waiter should return with context error
		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not return after context cancel")
		}
	})

	t.Run("handles connect/disconnect cycles", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		nodeID := randomPeerID(t)

		// Cycle 1: wait, then connect
		done1 := make(chan error, 1)
		go func() {
			done1 <- m.waitForConnection(ctx)
		}()
		time.Sleep(50 * time.Millisecond)
		conn1 := newTMeshConnection(nodeID)
		m.setPrimaryConnection(conn1)

		if err := <-done1; err != nil {
			t.Fatalf("cycle 1: expected no error, got %v", err)
		}

		// Cycle 2: disconnect, then wait and connect
		m.setPrimaryConnection(nil)

		done2 := make(chan error, 1)
		go func() {
			done2 <- m.waitForConnection(ctx)
		}()
		time.Sleep(50 * time.Millisecond)
		conn2 := newTMeshConnection(nodeID)
		m.setPrimaryConnection(conn2)

		if err := <-done2; err != nil {
			t.Fatalf("cycle 2: expected no error, got %v", err)
		}

		// Cycle 3: disconnect, then wait and connect
		m.setPrimaryConnection(nil)

		done3 := make(chan error, 1)
		go func() {
			done3 <- m.waitForConnection(ctx)
		}()
		time.Sleep(50 * time.Millisecond)
		conn3 := newTMeshConnection(nodeID)
		m.setPrimaryConnection(conn3)

		if err := <-done3; err != nil {
			t.Fatalf("cycle 3: expected no error, got %v", err)
		}
	})

	t.Run("new waiter after disconnection waits again", func(t *testing.T) {
		m := newMeshConnectionManager(&meshConnectionManagerConfig{log: logger})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Connect
		conn1 := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn1)

		// First waiter returns immediately (already connected)
		done1 := make(chan error, 1)
		go func() {
			done1 <- m.waitForConnection(ctx)
		}()

		if err := <-done1; err != nil {
			t.Fatalf("first waiter: expected no error, got %v", err)
		}

		// Disconnect
		m.setPrimaryConnection(nil)

		// Second waiter should block
		done2 := make(chan error, 1)
		go func() {
			done2 <- m.waitForConnection(ctx)
		}()

		time.Sleep(100 * time.Millisecond)

		// Should still be blocked
		select {
		case <-done2:
			t.Fatal("waiter returned but should still be waiting")
		default:
		}

		// Reconnect
		conn2 := newTMeshConnection(randomPeerID(t))
		m.setPrimaryConnection(conn2)

		// Now waiter should return
		select {
		case err := <-done2:
			if err != nil {
				t.Fatalf("second waiter: expected no error, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not return after reconnection")
		}
	})
}
