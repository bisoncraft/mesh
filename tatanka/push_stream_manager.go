package tatanka

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/bisoncraft/mesh/codec"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
)

const (
	// writeTimeout is the timeout for individual writes to a client push stream.
	writeTimeout = 5 * time.Second

	// pushQueueSize is the buffer size for the write channel of a client push stream.
	pushQueueSize = 100
)

// pushStreamWrapper contains a long running stream used to push broadcast messages
// to a client and channels to signal new messages and shutdown.
type pushStreamWrapper struct {
	stream  network.Stream
	writeCh chan []byte
}

type notifyConnectedFunc func(client peer.ID, timestamp time.Time, connected bool)

// pushStreamManager handles the management of long running streams where
// clients listen for broadcast messages.
type pushStreamManager struct {
	log             slog.Logger
	notifyConnected notifyConnectedFunc

	pushStreamMtx sync.RWMutex
	pushStreams   map[peer.ID]*pushStreamWrapper
	ipsMtx        sync.RWMutex
	ips           map[string]peer.ID
}

// newPushStreamManager creates a new pushStreamManager. The notifyConnectedFunc
// is used to notify the caller when a client has created a push stream or no longer
// has an open push stream.
func newPushStreamManager(log slog.Logger, f notifyConnectedFunc) *pushStreamManager {
	return &pushStreamManager{
		log:             log,
		notifyConnected: f,
		pushStreams:     make(map[peer.ID]*pushStreamWrapper),
		ips:             make(map[string]peer.ID),
	}
}

// newPushStream is called when the client opens a push stream to the node.
// Any data the client sends on this stream is ignored, and the stream remains
// open until the client disconnects, or the client opens a new push stream in
// which case the tatanka node disconnects the old stream.
func (p *pushStreamManager) newPushStream(stream network.Stream) {
	client := stream.Conn().RemotePeer()

	ip, err := getIPFromStream(stream)
	if err != nil {
		p.log.Errorf("Failed to fetch ip from stream: %v", err)
	}

	p.ipsMtx.Lock()
	p.ips[ip] = client
	p.ipsMtx.Unlock()

	p.pushStreamMtx.Lock()
	oldWrapper := p.pushStreams[client]
	wrapper := &pushStreamWrapper{
		stream:  stream,
		writeCh: make(chan []byte, pushQueueSize),
	}
	p.pushStreams[client] = wrapper
	newStreamTimestamp := time.Now()
	p.pushStreamMtx.Unlock()

	if oldWrapper != nil {
		_ = oldWrapper.stream.Close()
		close(oldWrapper.writeCh)
	} else {
		p.notifyConnected(client, newStreamTimestamp, true)
	}

	// Goroutine for writes. This will run until the write channel is closed.
	go func() {
		for data := range wrapper.writeCh {
			if err := codec.WriteBytes(wrapper.stream, data, writeTimeout); err != nil {
				p.log.Errorf("Write failed for client %s: %v", client.ShortString(), err)
			}
		}
	}()

	// Goroutine for reads. All data read from the stream is discarded. When
	// the stream is closed, this goroutine will close the write channel,
	// ending the write goroutine.
	go func() {
		_, err := io.Copy(io.Discard, stream)
		if err != nil && !errors.Is(err, io.ErrClosedPipe) {
			p.log.Debugf("Error discarding data from client %s push stream: %v", client.ShortString(), err)
		}

		p.pushStreamMtx.Lock()
		if p.pushStreams[client] == wrapper {
			close(wrapper.writeCh)
			delete(p.pushStreams, client)
			timestamp := time.Now()
			p.pushStreamMtx.Unlock()

			p.ipsMtx.Lock()
			delete(p.ips, ip)
			p.ipsMtx.Unlock()

			p.notifyConnected(client, timestamp, false)
		} else {
			p.pushStreamMtx.Unlock()
		}
	}()
}

// distribute distributes a message to all the clients except the sender.
func (p *pushStreamManager) distribute(clients []peer.ID, msg *protocolsPb.PushMessage) {
	sender, err := peer.IDFromBytes(msg.Sender)
	if err != nil {
		p.log.Errorf("Failed to parse sender ID for message %s: %v", msg.Topic, err)
		return
	}

	data, err := codec.MarshalProtoWithLengthPrefix(msg)
	if err != nil {
		p.log.Errorf("Failed to marshal push message: %v", err)
		return
	}

	p.pushStreamMtx.RLock()
	defer p.pushStreamMtx.RUnlock()

	for _, client := range clients {
		if client == sender {
			continue
		}

		if wrapper, ok := p.pushStreams[client]; ok {
			select {
			case wrapper.writeCh <- data:
			default:
				p.log.Warnf("Dropped message for client %s: write queue full", client.ShortString())
			}
		}
	}
}

func (p *pushStreamManager) disconnectClientByIP(ip string) {
	p.ipsMtx.RLock()
	clientID, ok := p.ips[ip]
	p.ipsMtx.RUnlock()

	if !ok {
		p.log.Debugf("No connected client found with IP %s", ip)
		return
	}

	p.pushStreamMtx.RLock()
	wrapper, ok := p.pushStreams[clientID]
	p.pushStreamMtx.RUnlock()

	if !ok {
		p.log.Debugf("No stream found for connected client %s", clientID.ShortString())
		return
	}

	if err := wrapper.stream.Close(); err != nil {
		p.log.Errorf("Error closing stream for client %s: %v", clientID.ShortString(), err)
		return
	}
}

func getIPFromStream(s network.Stream) (string, error) {
	if s == nil {
		return "", errors.New("network stream cannot be nil")
	}

	remoteMa := s.Conn().RemoteMultiaddr()
	if remoteMa == nil {
		return "", errors.New("remote multi address cannot be nil")
	}

	if ipv4, err := remoteMa.ValueForProtocol(ma.P_IP4); err == nil && ipv4 != "" {
		return ipv4, nil
	}

	if ipv6, err := remoteMa.ValueForProtocol(ma.P_IP6); err == nil && ipv6 != "" {
		return ipv6, nil
	}

	return "", errors.New("failed to deduce ip address from stream")
}
