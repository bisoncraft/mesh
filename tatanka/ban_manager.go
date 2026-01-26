package tatanka

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
	pb "github.com/martonp/tatanka-mesh/tatanka/pb"
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

type infraction struct {
	infractionType infractionType
	penalty        uint32
	expiry         time.Time
}

type remoteBan struct {
	ip       string
	reporter peer.ID
	penalty  uint32
	expiry   time.Time
}

type banManagerConfig struct {
	disconnectClient func(string)
	nodeID           peer.ID
	publishBan       func(context.Context, *pb.ClientBanMsg) error
	now              func() time.Time
	log              slog.Logger
}

type banManager struct {
	cfg               *banManagerConfig
	mtx               sync.RWMutex
	clientInfractions map[string][]infraction
	remoteBans        map[string]remoteBan
	log               slog.Logger
}

func newBanManager(cfg *banManagerConfig) *banManager {
	return &banManager{
		cfg:               cfg,
		clientInfractions: make(map[string][]infraction),
		remoteBans:        make(map[string]remoteBan),
		log:               cfg.log,
	}
}

func (bm *banManager) recordInfraction(ip string, id peer.ID, infractionType infractionType) error {
	bm.mtx.Lock()
	defer bm.mtx.Unlock()

	if _, ok := bm.clientInfractions[ip]; !ok {
		bm.clientInfractions[ip] = make([]infraction, 0)
	}

	infractions := bm.clientInfractions[ip]

	// TODO: add more infraction types and their penalties.
	switch infractionType {
	case MalformedMessage:
		infractions = append(infractions,
			infraction{
				infractionType: infractionType,
				penalty:        1,
				expiry:         bm.cfg.now().Add(infractionRetentionPeriod),
			})
	case InvalidBond:
		infractions = append(infractions,
			infraction{
				infractionType: infractionType,
				penalty:        1,
				expiry:         bm.cfg.now().Add(infractionRetentionPeriod),
			})
	case NodeImpersonation:
		infractions = append(infractions,
			infraction{
				infractionType: infractionType,
				penalty:        maxInfractionPenalties,
				expiry:         bm.cfg.now().Add(infractionRetentionPeriod * 24),
			})
	case RateLimitViolation:
		infractions = append(infractions,
			infraction{
				infractionType: infractionType,
				penalty:        2,
				expiry:         bm.cfg.now().Add(infractionRetentionPeriod * 2),
			})
	case RateLimitAbuse:
		infractions = append(infractions,
			infraction{
				infractionType: infractionType,
				penalty:        5,
				expiry:         bm.cfg.now().Add(infractionRetentionPeriod * 6),
			})
	default:
		return fmt.Errorf("unexpected infraction type provided %T", infractionType)
	}

	bm.clientInfractions[ip] = infractions

	var penalties uint32
	for _, inf := range infractions {
		penalties += inf.penalty
	}

	// Disconnect the client once it meets or exceeds the maximum infraction penalties.
	if penalties >= maxInfractionPenalties {
		bm.cfg.disconnectClient(ip)

		now := bm.cfg.now()
		reporterBytes, err := bm.cfg.nodeID.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal node ID for client ban: %v", err)
		}

		banMsg := &pb.ClientBanMsg{
			Ip:             ip,
			Reporter:       reporterBytes,
			TotalPenalties: penalties,
			Expiry:         now.Add(infractionRetentionPeriod).UnixMilli(),
		}

		if err := bm.cfg.publishBan(context.Background(), banMsg); err != nil {
			return fmt.Errorf("failed to publish ban for client %s (%s): %v", id.ShortString(), ip, err)
		}
		return nil
	}

	return nil
}

func (bm *banManager) recordRemoteBan(msg *pb.ClientBanMsg) error {
	if msg == nil {
		return fmt.Errorf("client ban message cannot be nil")
	}

	bm.mtx.Lock()
	defer bm.mtx.Unlock()

	reporterID, err := peer.IDFromBytes(msg.Reporter)
	if err != nil {
		return fmt.Errorf("failed to parse client ban reporter: %v", err)
	}

	expiry := time.UnixMilli(msg.Expiry)

	bm.remoteBans[msg.Ip] = remoteBan{
		ip:       msg.Ip,
		reporter: reporterID,
		penalty:  msg.TotalPenalties,
		expiry:   expiry,
	}

	bm.cfg.disconnectClient(msg.Ip)

	return nil
}

func (bm *banManager) activeBans() []*pb.ClientBanMsg {
	bm.mtx.RLock()
	defer bm.mtx.RUnlock()

	now := bm.cfg.now()
	var bans []*pb.ClientBanMsg

	for ip, infractions := range bm.clientInfractions {
		var penalties uint32
		for _, inf := range infractions {
			if !inf.expiry.Before(now) {
				penalties += inf.penalty
			}
		}

		if penalties >= maxInfractionPenalties {
			var latestExpiry time.Time
			for _, inf := range infractions {
				if !inf.expiry.Before(now) && inf.expiry.After(latestExpiry) {
					latestExpiry = inf.expiry
				}
			}

			reporterBytes, err := bm.cfg.nodeID.Marshal()
			if err != nil {
				bm.log.Errorf("failed to marshal node ID for active ban: %v", err)
				continue
			}

			bans = append(bans, &pb.ClientBanMsg{
				Ip:             ip,
				Reporter:       reporterBytes,
				TotalPenalties: penalties,
				Expiry:         latestExpiry.UnixMilli(),
			})
		}
	}

	for ip, rb := range bm.remoteBans {
		if !rb.expiry.Before(now) {
			reporterBytes, err := rb.reporter.Marshal()
			if err != nil {
				bm.log.Errorf("failed to marshal remote ban reporter: %v", err)
				continue
			}
			bans = append(bans, &pb.ClientBanMsg{
				Ip:             ip,
				Reporter:       reporterBytes,
				TotalPenalties: rb.penalty,
				Expiry:         rb.expiry.UnixMilli(),
			})
		}
	}

	return bans
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

	for ip, rb := range bm.remoteBans {
		if rb.expiry.Before(cutoffTime) {
			delete(bm.remoteBans, ip)
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

	if rb, ok := bm.remoteBans[ip]; ok {
		if !rb.expiry.Before(now) {
			return true
		}
	}

	return false
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
