package tatanka

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
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

	mtx         sync.RWMutex
	pushStreams map[peer.ID]*pushStreamWrapper
}

// newPushStreamManager creates a new pushStreamManager. The notifyConnectedFunc
// is used to notify the caller when a client has created a push stream or no longer
// has an open push stream.
func newPushStreamManager(log slog.Logger, f notifyConnectedFunc) *pushStreamManager {
	return &pushStreamManager{
		log:             log,
		notifyConnected: f,
		pushStreams:     make(map[peer.ID]*pushStreamWrapper),
	}
}

// newPushStream is called when the client opens a push stream to the node.
// Any data the client sends on this stream is ignored, and the stream remains
// open until the client disconnects, or the client opens a new push stream in
// which case the tatanka node disconnects the old stream.
func (p *pushStreamManager) newPushStream(stream network.Stream) {
	client := stream.Conn().RemotePeer()

	p.mtx.Lock()
	oldWrapper := p.pushStreams[client]
	wrapper := &pushStreamWrapper{
		stream:  stream,
		writeCh: make(chan []byte, pushQueueSize),
	}
	p.pushStreams[client] = wrapper
	newStreamTimestamp := time.Now()
	p.mtx.Unlock()

	if oldWrapper != nil {
		oldWrapper.stream.Reset()
		close(oldWrapper.writeCh)
	} else {
		p.notifyConnected(client, newStreamTimestamp, true)
	}

	// Goroutine for writes. This will run until the write channel is closed.
	go func() {
		for data := range wrapper.writeCh {
			if err := wrapper.stream.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil && !deadlineNotSupportedError(err) {
				p.log.Debugf("Failed to set write deadline for client %s: %v", client.ShortString(), err)
				continue
			}

			if _, err := wrapper.stream.Write(data); err != nil {
				p.log.Debugf("Write failed for client %s: %v", client.ShortString(), err)
			}

			wrapper.stream.SetWriteDeadline(time.Time{})
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

		p.mtx.Lock()
		if p.pushStreams[client] == wrapper {
			close(wrapper.writeCh)
			delete(p.pushStreams, client)
			timestamp := time.Now()
			p.mtx.Unlock()

			p.notifyConnected(client, timestamp, false)
		} else {
			p.mtx.Unlock()
		}
	}()
}

// distribute distributes a message to all the clients except the sender.
func (p *pushStreamManager) distribute(clients []peer.ID, msg *protocolsPb.ClientPushMessage) {
	sender, err := peer.IDFromBytes(msg.Sender)
	if err != nil {
		p.log.Errorf("Failed to parse sender ID for message %s: %v", msg.Topic, err)
		return
	}

	data, err := marshalProtoWithLengthPrefix(msg)
	if err != nil {
		p.log.Errorf("Failed to marshal push message: %v", err)
		return
	}

	p.mtx.RLock()
	defer p.mtx.RUnlock()

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
