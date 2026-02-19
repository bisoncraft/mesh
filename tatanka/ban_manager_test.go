package tatanka

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func testLogger() slog.Logger {
	backend := slog.NewBackend(os.Stdout)
	return backend.Logger("test")
}

func generateTestPeerID(t *testing.T) peer.ID {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	peerID, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		t.Fatalf("Failed to generate peer ID: %v", err)
	}
	return peerID
}

func TestBanManagerRecordInfraction(t *testing.T) {
	tests := []struct {
		name      string
		ipAddr    string
		numInfra  int
		shouldBan bool
	}{
		{
			name:      "IPv4 below threshold",
			ipAddr:    "192.168.1.1",
			numInfra:  3,
			shouldBan: false,
		},
		{
			name:      "IPv4 at threshold",
			ipAddr:    "10.0.0.1",
			numInfra:  10,
			shouldBan: true,
		},
		{
			name:      "IPv6 below threshold",
			ipAddr:    "2001:db8::1",
			numInfra:  2,
			shouldBan: false,
		},
		{
			name:      "IPv6 at threshold",
			ipAddr:    "fe80::1",
			numInfra:  10,
			shouldBan: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disconnected := false
			bm := newBanManager(&banManagerConfig{
				disconnectClient: func(ip string) {
					disconnected = true
				},
				nodeID:     peer.ID("this-node"),
				publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
				log:        testLogger(),
				now:        time.Now,
			})

			clientID := peer.ID("test-client")
			for i := 0; i < tt.numInfra; i++ {
				err := bm.recordInfraction(tt.ipAddr, clientID, MalformedMessage)
				if err != nil {
					t.Fatalf("recordInfraction: %v", err)
				}
			}

			if disconnected != tt.shouldBan {
				t.Errorf("expected disconnect=%v, got disconnect=%v", tt.shouldBan, disconnected)
			}
		})
	}
}

func TestBanManagerMultipleIPs(t *testing.T) {
	disconnectedIPs := make(map[string]bool)

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {
			disconnectedIPs[ip] = true
		},
		nodeID:     peer.ID("this-node"),
		publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:        testLogger(),
		now:        time.Now,
	})

	clientID := peer.ID("test-client")
	ipAddrs := []string{"192.168.1.1", "192.168.1.2", "2001:db8::1", "2001:db8::2"}

	// Record 9 infractions for each IP (below threshold)
	for _, ip := range ipAddrs {
		for i := 0; i < 9; i++ {
			err := bm.recordInfraction(ip, clientID, MalformedMessage)
			if err != nil {
				t.Fatalf("recordInfraction: %v", err)
			}
		}
	}

	// Record one more infraction for first IP to trigger threshold
	err := bm.recordInfraction(ipAddrs[0], clientID, MalformedMessage)
	if err != nil {
		t.Fatalf("recordInfraction: %v", err)
	}

	if !disconnectedIPs[ipAddrs[0]] {
		t.Errorf("expected disconnect for %s", ipAddrs[0])
	}

	for _, ip := range ipAddrs[1:] {
		if disconnectedIPs[ip] {
			t.Errorf("unexpected disconnect for %s", ip)
		}
	}
}

func TestBanManagerPublishInfractionCallback(t *testing.T) {
	t.Run("Publish each infraction", func(t *testing.T) {
		var publishedInfractions []*pb.ClientInfractionMsg
		var mtx sync.Mutex
		nodeID := generateTestPeerID(t)

		bm := newBanManager(&banManagerConfig{
			disconnectClient: func(ip string) {},
			nodeID:           nodeID,
			publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error {
				mtx.Lock()
				publishedInfractions = append(publishedInfractions, msg)
				mtx.Unlock()
				return nil
			},
			log: testLogger(),
			now: time.Now,
		})

		clientID := peer.ID("test-client")
		ipAddr := "192.168.1.100"

		// Record 10 infractions - each should be published
		for i := 0; i < 10; i++ {
			err := bm.recordInfraction(ipAddr, clientID, MalformedMessage)
			if err != nil {
				t.Fatalf("recordInfraction: %v", err)
			}
		}

		// Verify each infraction was published
		mtx.Lock()
		if len(publishedInfractions) != 10 {
			t.Errorf("expected 10 published infractions, got %d", len(publishedInfractions))
		}

		for i, infraction := range publishedInfractions {
			if infraction.Ip != ipAddr {
				t.Errorf("infraction %d: expected IP %s, got %s", i, ipAddr, infraction.Ip)
			}
			if infraction.Penalty != 1 {
				t.Errorf("infraction %d: expected penalty 1, got %d", i, infraction.Penalty)
			}
			if len(infraction.Reporter) == 0 {
				t.Errorf("infraction %d: reporter should not be empty", i)
			}
			if infraction.Expiry == 0 {
				t.Errorf("infraction %d: expiry should be set", i)
			}
			if infraction.InfractionType != uint32(MalformedMessage) {
				t.Errorf("infraction %d: expected type MalformedMessage, got %d", i, infraction.InfractionType)
			}
		}
		mtx.Unlock()
	})

	t.Run("Publish all infractions", func(t *testing.T) {
		var publishedInfractions []*pb.ClientInfractionMsg
		var mtx sync.Mutex

		bm := newBanManager(&banManagerConfig{
			disconnectClient: func(ip string) {},
			nodeID:           generateTestPeerID(t),
			publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error {
				mtx.Lock()
				publishedInfractions = append(publishedInfractions, msg)
				mtx.Unlock()
				return nil
			},
			log: testLogger(),
			now: time.Now,
		})

		clientID := peer.ID("test-client")
		ipAddr := "192.168.1.100"

		// Record 9 infractions - each should be published, even though below ban threshold
		for i := 0; i < 9; i++ {
			bm.recordInfraction(ipAddr, clientID, MalformedMessage)
		}

		mtx.Lock()
		if len(publishedInfractions) != 9 {
			t.Errorf("expected 9 published infractions, got %d", len(publishedInfractions))
		}
		mtx.Unlock()
	})

	t.Run("Publish error handling", func(t *testing.T) {
		var publishedBans []*pb.ClientInfractionMsg
		var mtx sync.Mutex
		publishErr := fmt.Errorf("publish failed")

		bm := newBanManager(&banManagerConfig{
			disconnectClient: func(ip string) {},
			nodeID:           generateTestPeerID(t),
			publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error {
				return publishErr
			},
			log: testLogger(),
			now: time.Now,
		})

		clientID := peer.ID("test-client")
		ipAddr := "192.168.1.100"

		// Record 10 infractions
		var lastErr error
		for i := 0; i < 10; i++ {
			err := bm.recordInfraction(ipAddr, clientID, MalformedMessage)
			if err != nil {
				lastErr = err
			}
		}

		// Verify error was returned
		if lastErr == nil {
			t.Errorf("expected error from recordInfraction")
		}

		mtx.Lock()
		if len(publishedBans) != 0 {
			t.Errorf("expected 0 bans (due to error), got %d", len(publishedBans))
		}
		mtx.Unlock()
	})
}

func TestBanManagerPublishInfractionMultipleTypes(t *testing.T) {
	var publishedInfractions []*pb.ClientInfractionMsg
	var mtx sync.Mutex

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error {
			mtx.Lock()
			publishedInfractions = append(publishedInfractions, msg)
			mtx.Unlock()
			return nil
		},
		log: testLogger(),
		now: time.Now,
	})

	clientID := peer.ID("test-client")
	ipAddr := "192.168.1.100"

	// Record 5 malformed messages (penalty 1 each) and 5 invalid bonds (penalty 1 each)
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
	}
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, InvalidBond)
	}

	// Should publish 10 individual infractions
	mtx.Lock()
	if len(publishedInfractions) != 10 {
		t.Errorf("expected 10 published infractions, got %d", len(publishedInfractions))
	}

	malformedCount := 0
	invalidBondCount := 0
	for _, inf := range publishedInfractions {
		if inf.InfractionType == uint32(MalformedMessage) {
			malformedCount++
		} else if inf.InfractionType == uint32(InvalidBond) {
			invalidBondCount++
		}
	}

	if malformedCount != 5 {
		t.Errorf("expected 5 malformed message infractions, got %d", malformedCount)
	}
	if invalidBondCount != 5 {
		t.Errorf("expected 5 invalid bond infractions, got %d", invalidBondCount)
	}
	mtx.Unlock()
}

func TestBanManagerIsClientBanned(t *testing.T) {
	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           peer.ID("this-node"),
		publishInfraction:       func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	clientID := peer.ID("test-client")
	ipAddr := "192.168.1.100"

	// Record infractions below threshold
	for i := 0; i < 9; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
	}

	// Should not be banned yet
	if bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to not be banned", ipAddr)
	}

	// Record one more to hit threshold
	bm.recordInfraction(ipAddr, clientID, MalformedMessage)

	// Should now be banned
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned", ipAddr)
	}
}

func TestBanManagerPurgeExpiredInfractions(t *testing.T) {
	now := time.Unix(1000, 0)
	currentTime := now

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           peer.ID("this-node"),
		publishInfraction:       func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:              testLogger(),
		now:              func() time.Time { return currentTime },
	})

	clientID := peer.ID("test-client")
	ipAddr := "192.168.1.50"

	// Record infractions at current time
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
	}

	// Verify infractions are stored
	bm.mtx.Lock()
	if len(bm.clientInfractions[ipAddr]) != 5 {
		bm.mtx.Unlock()
		t.Errorf("expected 5 infractions, got %d", len(bm.clientInfractions[ipAddr]))
		return
	}
	bm.mtx.Unlock()

	// Advance time beyond retention period
	currentTime = now.Add(infractionRetentionPeriod + 1*time.Second)

	// Purge expired infractions
	bm.purgeExpiredInfractions()

	// All infractions should be gone
	bm.mtx.Lock()
	if _, exists := bm.clientInfractions[ipAddr]; exists {
		bm.mtx.Unlock()
		t.Errorf("expected infractions to be purged")
		return
	}
	bm.mtx.Unlock()
}

func TestBanManagerMultipleInfractionTypes(t *testing.T) {
	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           peer.ID("this-node"),
		publishInfraction:       func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	clientID := peer.ID("test-client")
	ipAddr := "2001:db8::100"

	// Record 5 malformed messages and 5 invalid bonds (total 10)
	for i := 0; i < 5; i++ {
		err := bm.recordInfraction(ipAddr, clientID, MalformedMessage)
		if err != nil {
			t.Fatalf("recordInfraction: %v", err)
		}
	}

	for i := 0; i < 5; i++ {
		err := bm.recordInfraction(ipAddr, clientID, InvalidBond)
		if err != nil {
			t.Fatalf("recordInfraction: %v", err)
		}
	}

	// Should be banned (total 10 penalties)
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned with 10 total infractions", ipAddr)
	}
}

func TestBanManagerRun(t *testing.T) {
	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           peer.ID("this-node"),
		publishInfraction:       func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start the cleanup loop
	bm.run(ctx)

	// Should exit cleanly after context cancels
}

func TestBanManagerRecordRemoteInfraction(t *testing.T) {
	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:               testLogger(),
		now:               time.Now,
	})

	ipAddr := "192.168.1.100"

	// Record 10 infractions worth 1 point each to reach the ban threshold
	// Use different expiry times for each to avoid deduplication
	for i := 0; i < 10; i++ {
		infractionMsg := &pb.ClientInfractionMsg{
			Ip:             ipAddr,
			Reporter:       reporterBytes,
			InfractionType: uint32(MalformedMessage),
			Penalty:        1,
			Expiry:         time.Now().Add(time.Hour).Add(time.Duration(i) * time.Millisecond).UnixMilli(),
		}

		err := bm.recordRemoteInfraction(infractionMsg)
		if err != nil {
			t.Fatalf("recordRemoteInfraction: %v", err)
		}
	}

	// Verify client is banned
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned after recording remote infractions", ipAddr)
	}
}

func TestBanManagerRemoteInfractionExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	currentTime := now

	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:               testLogger(),
		now:               func() time.Time { return currentTime },
	})

	ipAddr := "192.168.1.100"

	// Record 10 remote infractions that expire in 1 hour
	// Use different expiry times for each to avoid deduplication
	for i := 0; i < 10; i++ {
		infractionMsg := &pb.ClientInfractionMsg{
			Ip:             ipAddr,
			Reporter:       reporterBytes,
			InfractionType: uint32(MalformedMessage),
			Penalty:        1,
			Expiry:         now.Add(time.Hour).Add(time.Duration(i) * time.Millisecond).UnixMilli(),
		}

		err := bm.recordRemoteInfraction(infractionMsg)
		if err != nil {
			t.Fatalf("recordRemoteInfraction: %v", err)
		}
	}

	// Should be banned initially
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned", ipAddr)
	}

	// Advance time beyond expiry
	currentTime = now.Add(2 * time.Hour)

	// Should no longer be banned
	if bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to not be banned after expiry", ipAddr)
	}
}



func TestGetIPFromStream(t *testing.T) {
	// This test is minimal since we can't easily mock network streams
	// But it verifies the function handles nil input gracefully
	ip, err := getIPFromStream(nil)
	if err == nil {
		t.Errorf("expected error for nil stream, got none")
	}
	if ip != "" {
		t.Errorf("expected empty string for nil stream, got %s", ip)
	}
}

func TestBanManagerLocalAndRemoteInfractions(t *testing.T) {
	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
		log:               testLogger(),
		now:               time.Now,
	})

	clientID := generateTestPeerID(t)
	ipAddr := "192.168.1.100"

	// Record 5 local infractions (with small delays to ensure different timestamps)
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
		time.Sleep(1 * time.Millisecond) // Ensure different timestamps
	}

	// Record 5 remote infractions
	// Use different expiry times for each to avoid deduplication
	for i := 0; i < 5; i++ {
		infractionMsg := &pb.ClientInfractionMsg{
			Ip:             ipAddr,
			Reporter:       reporterBytes,
			InfractionType: uint32(MalformedMessage),
			Penalty:        1,
			Expiry:         time.Now().Add(time.Hour).Add(time.Duration(i) * time.Millisecond).UnixMilli(),
		}

		err := bm.recordRemoteInfraction(infractionMsg)
		if err != nil {
			t.Fatalf("recordRemoteInfraction: %v", err)
		}
	}

	// Client should be banned (5 local + 5 remote = 10 total)
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned with combined local and remote infractions", ipAddr)
	}
}

func TestBanManagerDisconnectCallback(t *testing.T) {
	tests := []struct {
		name             string
		ipAddr           string
		infractions      int
		shouldDisconnect bool
	}{
		{
			name:             "Disconnect at threshold",
			ipAddr:           "192.168.1.1",
			infractions:      10,
			shouldDisconnect: true,
		},
		{
			name:             "No disconnect below threshold",
			ipAddr:           "192.168.1.2",
			infractions:      9,
			shouldDisconnect: false,
		},
		{
			name:             "Multiple IPs, only one at threshold",
			ipAddr:           "192.168.1.3",
			infractions:      10,
			shouldDisconnect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disconnectCalls := []string{}
			var mtx sync.Mutex

			bm := newBanManager(&banManagerConfig{
				disconnectClient: func(ip string) {
					mtx.Lock()
					disconnectCalls = append(disconnectCalls, ip)
					mtx.Unlock()
				},
				nodeID:     peer.ID("this-node"),
				publishInfraction: func(ctx context.Context, msg *pb.ClientInfractionMsg) error { return nil },
				log:        testLogger(),
				now:        time.Now,
			})

			clientID := peer.ID("test-client")

			// Record infractions up to the specified count
			for i := 0; i < tt.infractions; i++ {
				err := bm.recordInfraction(tt.ipAddr, clientID, MalformedMessage)
				if err != nil {
					t.Fatalf("recordInfraction: %v", err)
				}
			}

			// Verify disconnect callback was invoked correctly
			mtx.Lock()
			if tt.shouldDisconnect {
				if len(disconnectCalls) != 1 {
					t.Errorf("expected 1 disconnect call, got %d", len(disconnectCalls))
				} else if disconnectCalls[0] != tt.ipAddr {
					t.Errorf("expected disconnect for %s, got %s", tt.ipAddr, disconnectCalls[0])
				}
			} else {
				if len(disconnectCalls) != 0 {
					t.Errorf("expected 0 disconnect calls, got %d", len(disconnectCalls))
				}
			}
			mtx.Unlock()
		})
	}
}
