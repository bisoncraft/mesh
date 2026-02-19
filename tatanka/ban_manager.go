package tatanka

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

type infractionType int

const (
	MalformedMessage infractionType = iota
	InvalidBond
	NodeImpersonation
	RateLimitViolation
	RateLimitAbuse
)

const (
	maxInfractionPenalties    = 10
	infractionRetentionPeriod = time.Hour
)

func infractionTypeString(it infractionType) string {
	switch it {
	case MalformedMessage:
		return "sent malformed message"
	case InvalidBond:
		return "invalid bond"
	case NodeImpersonation:
		return "attempted to impersonate node"
	case RateLimitViolation:
		return "rate limit violation"
	case RateLimitAbuse:
		return "rate limit abuse"
	default:
		return "unknown infraction"
	}
}

type infraction struct {
	infractionType infractionType
	penalty        uint32
	expiry         time.Time
}


type banManagerConfig struct {
	disconnectClient   func(string)
	nodeID             peer.ID
	publishInfraction  func(context.Context, *pb.ClientInfractionMsg) error
	now                func() time.Time
	log                slog.Logger
}

type banManager struct {
	cfg               *banManagerConfig
	mtx               sync.RWMutex
	clientInfractions map[string][]infraction
	log               slog.Logger
}

func newBanManager(cfg *banManagerConfig) *banManager {
	return &banManager{
		cfg:               cfg,
		clientInfractions: make(map[string][]infraction),
		log:               cfg.log,
	}
}

func infractionPenaltyAndDuration(it infractionType) (uint32, time.Duration, error) {
	switch it {
	case MalformedMessage, InvalidBond:
		return 1, infractionRetentionPeriod, nil
	case NodeImpersonation:
		return maxInfractionPenalties, infractionRetentionPeriod * 24, nil
	case RateLimitViolation:
		return 2, infractionRetentionPeriod * 2, nil
	case RateLimitAbuse:
		return 5, infractionRetentionPeriod * 6, nil
	default:
		return 0, 0, fmt.Errorf("unexpected infraction type provided %v", it)
	}
}

func (bm *banManager) recordInfraction(ip string, id peer.ID, infractionType infractionType) error {
	bm.mtx.Lock()
	defer bm.mtx.Unlock()

	if _, ok := bm.clientInfractions[ip]; !ok {
		bm.clientInfractions[ip] = make([]infraction, 0)
	}

	infractions := bm.clientInfractions[ip]

	penalty, duration, err := infractionPenaltyAndDuration(infractionType)
	if err != nil {
		return err
	}

	expiry := bm.cfg.now().Add(duration)
	infractions = append(infractions, infraction{
		infractionType: infractionType,
		penalty:        penalty,
		expiry:         expiry,
	})

	bm.clientInfractions[ip] = infractions

	now := bm.cfg.now()
	var penalties uint32
	for _, inf := range infractions {
		if !inf.expiry.Before(now) {
			penalties += inf.penalty
		}
	}

	// Publish the infraction for other nodes to gossip
	reporterBytes, err := bm.cfg.nodeID.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal node ID for infraction: %v", err)
	}

	infractionMsg := &pb.ClientInfractionMsg{
		Ip:             ip,
		Reporter:       reporterBytes,
		InfractionType: uint32(infractionType),
		Penalty:        penalty,
		Expiry:         expiry.UnixMilli(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	if err := bm.cfg.publishInfraction(ctx, infractionMsg); err != nil {
		return fmt.Errorf("failed to publish infraction for client %s (%s): %v", id.ShortString(), ip, err)
	}

	// Disconnect the client once it meets or exceeds the maximum infraction penalties.
	if penalties >= maxInfractionPenalties {
		bm.cfg.disconnectClient(ip)
	}

	return nil
}

func (bm *banManager) recordRemoteInfraction(msg *pb.ClientInfractionMsg) error {
	if msg == nil {
		return fmt.Errorf("client infraction message cannot be nil")
	}

	bm.mtx.Lock()
	defer bm.mtx.Unlock()

	ip := msg.Ip
	if _, ok := bm.clientInfractions[ip]; !ok {
		bm.clientInfractions[ip] = make([]infraction, 0)
	}

	// Prevent duplicate infractions from gossip re-delivery
	infractionKey := InfractionDedupKey(msg.Ip, msg.Reporter, msg.InfractionType, msg.Expiry)

	infractions := bm.clientInfractions[ip]
	for _, existing := range infractions {
		existingKey := InfractionDedupKey(ip, msg.Reporter, uint32(existing.infractionType), existing.expiry.UnixMilli())
		if existingKey == infractionKey {
			return nil
		}
	}

	infractions = append(infractions, infraction{
		infractionType: infractionType(msg.InfractionType),
		penalty:        msg.Penalty,
		expiry:         time.UnixMilli(msg.Expiry),
	})
	bm.clientInfractions[ip] = infractions


	now := bm.cfg.now()
	var penalties uint32
	for _, inf := range infractions {
		if !inf.expiry.Before(now) {
			penalties += inf.penalty
		}
	}

	if penalties >= maxInfractionPenalties {
		bm.cfg.disconnectClient(ip)
	}

	return nil
}

// InfractionDedupKey generates a unique key for deduplication based on (IP, reporter, type, expiry)
// This is used by both recordRemoteInfraction and syncInfractionsFromRandomPeer
func InfractionDedupKey(ip string, reporter []byte, infractionType uint32, expiry int64) string {
	return fmt.Sprintf("%s_%x_%d_%d", ip, reporter, infractionType, expiry)
}


func (bm *banManager) purgeExpiredInfractions() {
	bm.mtx.Lock()
	defer bm.mtx.Unlock()

	cutoffTime := bm.cfg.now()
	for ip, infractions := range bm.clientInfractions {
		filtered := slices.DeleteFunc(infractions, func(inf infraction) bool {
			return inf.expiry.Before(cutoffTime)
		})
		infractions = filtered
		bm.clientInfractions[ip] = infractions

		if len(infractions) == 0 {
			delete(bm.clientInfractions, ip)
		}
	}
}

func (bm *banManager) isClientBanned(ip string) bool {
	bm.mtx.RLock()
	defer bm.mtx.RUnlock()

	now := bm.cfg.now()

	if infractions, ok := bm.clientInfractions[ip]; ok {
		var penalties uint32
		for _, inf := range infractions {
			if !inf.expiry.Before(now) {
				penalties += inf.penalty
			}
		}

		if penalties >= maxInfractionPenalties {
			return true
		}
	}

	return false
}

func (bm *banManager) getClientBanReason(ip string) string {
	bm.mtx.RLock()
	defer bm.mtx.RUnlock()

	now := bm.cfg.now()

	if infractions, ok := bm.clientInfractions[ip]; ok {
		var penalties uint32
		var reasons []string
		for _, inf := range infractions {
			if !inf.expiry.Before(now) {
				penalties += inf.penalty
				reasons = append(reasons, infractionTypeString(inf.infractionType))
			}
		}

		if penalties >= maxInfractionPenalties {
			return strings.Join(reasons, ", ")
		}
	}

	return ""
}

// getActiveInfractions returns all non-expired infractions across all clients. It is safe for
// concurrent use.
func (bm *banManager) getActiveInfractions() []*pb.ClientInfractionMsg {
	bm.mtx.RLock()
	defer bm.mtx.RUnlock()

	now := bm.cfg.now()
	var result []*pb.ClientInfractionMsg

	for ip, infractions := range bm.clientInfractions {
		for _, inf := range infractions {
			if !inf.expiry.Before(now) {
				result = append(result, &pb.ClientInfractionMsg{
					Ip:             ip,
					InfractionType: uint32(inf.infractionType),
					Penalty:        inf.penalty,
					Expiry:         inf.expiry.UnixMilli(),
				})
			}
		}
	}

	return result
}

func (bm *banManager) run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bm.purgeExpiredInfractions()
		}
	}
}
