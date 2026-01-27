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
				publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
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
		publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
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

func TestBanManagerPublishBanCallback(t *testing.T) {
	t.Run("Publish at threshold", func(t *testing.T) {
		var publishedBans []*pb.ClientBanMsg
		var mtx sync.Mutex
		nodeID := generateTestPeerID(t)

		bm := newBanManager(&banManagerConfig{
			disconnectClient: func(ip string) {},
			nodeID:           nodeID,
			publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error {
				mtx.Lock()
				publishedBans = append(publishedBans, msg)
				mtx.Unlock()
				return nil
			},
			log: testLogger(),
			now: time.Now,
		})

		clientID := peer.ID("test-client")
		ipAddr := "192.168.1.100"

		// Record 10 infractions to hit threshold
		for i := 0; i < 10; i++ {
			err := bm.recordInfraction(ipAddr, clientID, MalformedMessage)
			if err != nil {
				t.Fatalf("recordInfraction: %v", err)
			}
		}

		// Verify publish was called
		mtx.Lock()
		if len(publishedBans) != 1 {
			t.Errorf("expected 1 published ban, got %d", len(publishedBans))
		} else {
			ban := publishedBans[0]
			if ban.Ip != ipAddr {
				t.Errorf("expected IP %s, got %s", ipAddr, ban.Ip)
			}
			if ban.TotalPenalties != 10 {
				t.Errorf("expected 10 penalties, got %d", ban.TotalPenalties)
			}
			if len(ban.Reporter) == 0 {
				t.Errorf("reporter should not be empty")
			}
			if ban.Expiry == 0 {
				t.Errorf("expiry should be set")
			}
		}
		mtx.Unlock()
	})

	t.Run("No publish below threshold", func(t *testing.T) {
		var publishedBans []*pb.ClientBanMsg
		var mtx sync.Mutex

		bm := newBanManager(&banManagerConfig{
			disconnectClient: func(ip string) {},
			nodeID:           generateTestPeerID(t),
			publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error {
				mtx.Lock()
				publishedBans = append(publishedBans, msg)
				mtx.Unlock()
				return nil
			},
			log: testLogger(),
			now: time.Now,
		})

		clientID := peer.ID("test-client")
		ipAddr := "192.168.1.100"

		// Record 9 infractions (below threshold)
		for i := 0; i < 9; i++ {
			bm.recordInfraction(ipAddr, clientID, MalformedMessage)
		}

		mtx.Lock()
		if len(publishedBans) != 0 {
			t.Errorf("expected 0 published bans, got %d", len(publishedBans))
		}
		mtx.Unlock()
	})

	t.Run("Publish error handling", func(t *testing.T) {
		var publishedBans []*pb.ClientBanMsg
		var mtx sync.Mutex
		publishErr := fmt.Errorf("publish failed")

		bm := newBanManager(&banManagerConfig{
			disconnectClient: func(ip string) {},
			nodeID:           generateTestPeerID(t),
			publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error {
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

func TestBanManagerPublishBanMultipleInfractions(t *testing.T) {
	var publishedBans []*pb.ClientBanMsg
	var mtx sync.Mutex

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error {
			mtx.Lock()
			publishedBans = append(publishedBans, msg)
			mtx.Unlock()
			return nil
		},
		log: testLogger(),
		now: time.Now,
	})

	clientID := peer.ID("test-client")
	ipAddr := "192.168.1.100"

	// Record 5 malformed messages and 5 invalid bonds
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
	}
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, InvalidBond)
	}

	// Should publish with combined penalties
	mtx.Lock()
	if len(publishedBans) != 1 {
		t.Errorf("expected 1 published ban, got %d", len(publishedBans))
	} else {
		ban := publishedBans[0]
		if ban.TotalPenalties != 10 {
			t.Errorf("expected 10 total penalties, got %d", ban.TotalPenalties)
		}
	}
	mtx.Unlock()
}

func TestBanManagerIsClientBanned(t *testing.T) {
	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           peer.ID("this-node"),
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
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
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
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
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
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
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start the cleanup loop
	bm.run(ctx)

	// Should exit cleanly after context cancels
}

func TestBanManagerRecordRemoteBan(t *testing.T) {
	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	ipAddr := "192.168.1.100"

	banMsg := &pb.ClientBanMsg{
		Ip:             ipAddr,
		Reporter:       reporterBytes,
		TotalPenalties: 10,
		Expiry:         time.Now().Add(time.Hour).UnixMilli(),
	}

	bm.recordRemoteBan(banMsg)

	// Verify client is banned
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned after recording remote ban", ipAddr)
	}
}

func TestBanManagerRemoteBanExpiry(t *testing.T) {
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
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              func() time.Time { return currentTime },
	})

	ipAddr := "192.168.1.100"

	// Record a remote ban that expires in 1 hour
	banMsg := &pb.ClientBanMsg{
		Ip:             ipAddr,
		Reporter:       reporterBytes,
		TotalPenalties: 10,
		Expiry:         now.Add(time.Hour).UnixMilli(),
	}

	bm.recordRemoteBan(banMsg)

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

func TestBanManagerActiveBansLocal(t *testing.T) {
	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           peer.ID("this-node"),
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	clientID := peer.ID("test-client")
	ipAddr := "192.168.1.100"

	// Record infractions to reach ban threshold
	for i := 0; i < 10; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
	}

	// Get active bans
	bans := bm.activeBans()

	if len(bans) != 1 {
		t.Errorf("expected 1 active ban, got %d", len(bans))
		return
	}

	if bans[0].Ip != ipAddr {
		t.Errorf("expected ban for %s, got %s", ipAddr, bans[0].Ip)
	}

	if bans[0].TotalPenalties != 10 {
		t.Errorf("expected 10 penalties, got %d", bans[0].TotalPenalties)
	}
}

func TestBanManagerActiveBansRemote(t *testing.T) {
	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	ipAddr := "192.168.1.100"

	banMsg := &pb.ClientBanMsg{
		Ip:             ipAddr,
		Reporter:       reporterBytes,
		TotalPenalties: 10,
		Expiry:         time.Now().Add(time.Hour).UnixMilli(),
	}

	bm.recordRemoteBan(banMsg)

	// Get active bans
	bans := bm.activeBans()

	if len(bans) != 1 {
		t.Errorf("expected 1 active ban, got %d", len(bans))
		return
	}

	if bans[0].Ip != ipAddr {
		t.Errorf("expected ban for %s, got %s", ipAddr, bans[0].Ip)
	}
}

func TestBanManagerLocalAndRemoteBans(t *testing.T) {
	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	clientID1 := generateTestPeerID(t)
	ip1 := "192.168.1.100"
	ip2 := "192.168.1.101"

	// Record local ban for client 1
	for i := 0; i < 10; i++ {
		bm.recordInfraction(ip1, clientID1, MalformedMessage)
	}

	// Record remote ban for client 2
	banMsg := &pb.ClientBanMsg{
		Ip:             ip2,
		Reporter:       reporterBytes,
		TotalPenalties: 10,
		Expiry:         time.Now().Add(time.Hour).UnixMilli(),
	}

	bm.recordRemoteBan(banMsg)

	// Both should be banned
	if !bm.isClientBanned(ip1) {
		t.Errorf("expected %s to be banned", ip1)
	}

	if !bm.isClientBanned(ip2) {
		t.Errorf("expected %s to be banned", ip2)
	}

	// Get active bans
	bans := bm.activeBans()

	if len(bans) != 2 {
		t.Errorf("expected 2 active bans, got %d", len(bans))
	}
}

func TestBanManagerPurgeExpiredRemoteBans(t *testing.T) {
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
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              func() time.Time { return currentTime },
	})

	ip1 := "192.168.1.100"
	ip2 := "192.168.1.101"

	// Record two remote bans with different expiry times
	banMsg1 := &pb.ClientBanMsg{
		Ip:             ip1,
		Reporter:       reporterBytes,
		TotalPenalties: 10,
		Expiry:         now.Add(30 * time.Minute).UnixMilli(),
	}

	banMsg2 := &pb.ClientBanMsg{
		Ip:             ip2,
		Reporter:       reporterBytes,
		TotalPenalties: 10,
		Expiry:         now.Add(2 * time.Hour).UnixMilli(),
	}

	bm.recordRemoteBan(banMsg1)
	bm.recordRemoteBan(banMsg2)

	// Advance time to expire first ban
	currentTime = now.Add(45 * time.Minute)

	// Purge expired bans
	bm.purgeExpiredInfractions()

	// First should be gone, second should remain
	if bm.isClientBanned(ip1) {
		t.Errorf("expected %s to be unbanned after expiry", ip1)
	}

	if !bm.isClientBanned(ip2) {
		t.Errorf("expected %s to still be banned", ip2)
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

func TestBanManagerIsClientBannedMixedInfractions(t *testing.T) {
	reporterID := generateTestPeerID(t)
	reporterBytes, err := reporterID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal reporter ID: %v", err)
	}

	bm := newBanManager(&banManagerConfig{
		disconnectClient: func(ip string) {},
		nodeID:           generateTestPeerID(t),
		publishBan:       func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
		log:              testLogger(),
		now:              time.Now,
	})

	clientID := generateTestPeerID(t)
	ipAddr := "192.168.1.100"

	// Record 5 local infractions
	for i := 0; i < 5; i++ {
		bm.recordInfraction(ipAddr, clientID, MalformedMessage)
	}

	// Record a remote ban with 5 penalties
	banMsg := &pb.ClientBanMsg{
		Ip:             ipAddr,
		Reporter:       reporterBytes,
		TotalPenalties: 5,
		Expiry:         time.Now().Add(time.Hour).UnixMilli(),
	}

	bm.recordRemoteBan(banMsg)

	// Client should be banned (5 local + 5 remote = 10 total)
	if !bm.isClientBanned(ipAddr) {
		t.Errorf("expected %s to be banned with combined local and remote infractions", ipAddr)
	}

	// Verify active bans - since local already reached threshold, both local and remote are recorded
	bans := bm.activeBans()
	if len(bans) < 1 {
		t.Errorf("expected at least 1 active ban, got %d", len(bans))
	}

	// Check that we have bans for the IP address
	foundBan := false
	for _, ban := range bans {
		if ban.Ip == ipAddr {
			foundBan = true
			break
		}
	}

	if !foundBan {
		t.Errorf("expected ban for %s in active bans", ipAddr)
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
				publishBan: func(ctx context.Context, msg *pb.ClientBanMsg) error { return nil },
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
