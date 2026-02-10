package tatanka

import (
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/bisoncraft/mesh/codec"
)

var errUnauthorized = errors.New("unauthorized")

// requireAny returns a single decorator that succeeds if AT LEAST ONE
// underlying permission passes. It only returns an error if ALL permissions fail.
func requireAny(permissions ...permissionDecorator) permissionDecorator {
	return func(s network.Stream) error {
		var lastErr error
		for _, p := range permissions {
			err := p(s)
			if err == nil {
				return nil
			}
			lastErr = err
		}

		return lastErr
	}
}

// requireAll returns a single decorator that succeeds only if ALL
// underlying permissions pass. It returns an error if ANY permission fails.
func requireAll(permissions ...permissionDecorator) permissionDecorator {
	return func(s network.Stream) error {
		for _, p := range permissions {
			if err := p(s); err != nil {
				return err
			}
		}
		return nil
	}
}

func requireNoPermission(s network.Stream) error {
	return nil
}

type permissionDecorator func(s network.Stream) error

func (t *TatankaNode) requireBonds(s network.Stream) error {
	peerID := s.Conn().RemotePeer()
	if peerID == t.node.ID() {
		return nil
	}

	bonds := t.bondStorage.bondStrength(peerID)
	if bonds == 0 {
		return errUnauthorized
	}

	return nil
}

func (t *TatankaNode) isWhitelistPeer(s network.Stream) error {
	peerID := s.Conn().RemotePeer()
	if _, ok := t.getWhitelist().allPeerIDs()[peerID]; !ok {
		return errUnauthorized
	}
	return nil
}

func (t *TatankaNode) setStreamHandler(protocolID string, handler func(s network.Stream), permission permissionDecorator) {
	finalHandler := func(s network.Stream) {
		if err := permission(s); err != nil {
			if errors.Is(err, errUnauthorized) {
				err = codec.WriteLengthPrefixedMessage(s, pbResponseUnauthorizedError())
				if err != nil {
					t.log.Errorf("Error sending unauthorized response: %v", err)
				}
			} else {
				err = codec.WriteLengthPrefixedMessage(s, pbResponseError(err))
				if err != nil {
					t.log.Errorf("Error sending error response: %v", err)
				}
			}
			_ = s.Close()
			return
		}

		handler(s)
	}

	t.node.SetStreamHandler(protocol.ID(protocolID), finalHandler)
}

func (t *TatankaNode) requireNotBanned(s network.Stream) error {
	ip, err := getIPFromStream(s)
	if err != nil {
		return errUnauthorized
	}

	if t.banManager.isClientBanned(ip) {
		reason := t.banManager.getClientBanReason(ip)
		return fmt.Errorf("%w: %s", errUnauthorized, reason)
	}

	return nil
}
