package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/bond"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	"google.golang.org/protobuf/proto"
)

var (
	errInvalidBondIndex = errors.New("invalid bond index")
)

// meshConnection represents a connection to a mesh peer.
type meshConnection struct {
	peerID            peer.ID
	host              host.Host
	handleMessage     func(*protocolsPb.PushMessage)
	fetchTopics       func() []string
	setMeshConnection func(meshConn)
	bondInfo          *bond.BondInfo
	log               slog.Logger

	cancelFuncMtx sync.RWMutex
	cancelFunc    context.CancelFunc
}

var _ meshConn = (*meshConnection)(nil)

func newMeshConnection(host host.Host, peerID peer.ID, logger slog.Logger, bondInfo *bond.BondInfo, fetchTopics func() []string, handleMessage func(*protocolsPb.PushMessage), setMeshConnection func(meshConn)) *meshConnection {
	return &meshConnection{
		host:              host,
		peerID:            peerID,
		log:               logger,
		handleMessage:     handleMessage,
		fetchTopics:       fetchTopics,
		setMeshConnection: setMeshConnection,
		bondInfo:          bondInfo,
	}
}

func (m *meshConnection) remotePeerID() peer.ID {
	return m.peerID
}

// subscribeTopics subscribes to all registered topics.
func (m *meshConnection) subscribeTopics(ctx context.Context) error {
	topics := m.fetchTopics()
	if len(topics) == 0 {
		// Nothing to do.
		return nil
	}

	for _, topic := range topics {
		err := m.subscribe(ctx, topic)
		if err != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
		}
	}

	m.log.Infof("%s: resubscribed to topics on reconnection", m.host.ID())

	return nil
}

// run starts the mesh connection and maintains it until the context is done.
func (m *meshConnection) run(ctx context.Context) error {
	if m.handleMessage == nil {
		return fmt.Errorf("%s: no handler function provided", m.host.ID())
	}

	err := m.host.Connect(ctx, m.host.Peerstore().PeerInfo(m.peerID))
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", m.peerID, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	m.cancelFuncMtx.Lock()
	m.cancelFunc = cancel
	m.cancelFuncMtx.Unlock()

	// Post the bonds associated with the connection.The mesh connection is required to have
	// adequate bond strength in order to establish a push stream.
	if err := m.postBond(runCtx); err != nil {
		return err
	}

	if m.bondInfo.BondStrength() < bond.MinRequiredBondStrength {
		return fmt.Errorf("%s: connection does not have the minimum required bond strength to establish a push stream", m.host.ID())
	}

	hostID := m.host.ID()
	if err := m.subscribeTopics(runCtx); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%s: failed to subscribe to topics on mesh connection: %w", hostID, err)
		}

		return nil
	}

	m.maintainPushStream(runCtx)

	return nil
}

// subscribe subscribes to the provided topic.
func (m *meshConnection) subscribe(ctx context.Context, topic string) error {
	s, err := m.host.NewStream(ctx, m.peerID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.SubscribeRequest{
		Topic:     topic,
		Subscribe: true,
	}
	return codec.WriteLengthPrefixedMessage(s, req, writeTimeout)
}

// broadcast publishes the provided message bytes on a mesh topic.
func (m *meshConnection) broadcast(ctx context.Context, topic string, data []byte) error {
	s, err := m.host.NewStream(ctx, m.peerID, protocols.ClientPublishProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.PublishRequest{
		Topic: topic,
		Data:  data,
	}
	return codec.WriteLengthPrefixedMessage(s, req)
}

// unsubscribe unsubscribes from the provided topic.
func (m *meshConnection) unsubscribe(ctx context.Context, topic string) error {
	s, err := m.host.NewStream(ctx, m.peerID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.SubscribeRequest{
		Topic:     topic,
		Subscribe: false,
	}
	return codec.WriteLengthPrefixedMessage(s, req, writeTimeout)
}

// postBondInternal posts the provided bond request.
func (m *meshConnection) postBondInternal(ctx context.Context, req *protocolsPb.PostBondRequest) error {
	// Post the provided bond.
	s, err := m.host.NewStream(ctx, m.peerID, protocols.PostBondsProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	err = codec.WriteLengthPrefixedMessage(s, req)
	if err != nil {
		return fmt.Errorf("failed to write post bond request: %w", err)
	}

	// Process the post bond response.
	hostID := m.host.ID()
	resp := &protocolsPb.Response{}
	err = codec.ReadLengthPrefixedMessage(s, resp)
	if err != nil {
		return fmt.Errorf("failed to read message bytes: %w", err)
	}

	switch v := resp.Response.(type) {
	case *protocolsPb.Response_Error:
		switch v.Error.GetError().(type) {
		case *protocolsPb.Error_PostBondError:
			bondErr := v.Error.GetPostBondError()
			err := m.bondInfo.RemoveBondAtIndex(bondErr.InvalidBondIndex)
			if err != nil {
				return fmt.Errorf("failed to remove invalid bond at index %d: %v", bondErr.InvalidBondIndex, err)
			}

			m.log.Infof("%s: removed invalid bond at index %d", hostID, bondErr.InvalidBondIndex)
			return errInvalidBondIndex

		case *protocolsPb.Error_Message:
			errMsg := v.Error.GetMessage()
			return fmt.Errorf("%s: %v", hostID, errMsg)

		default:
			return fmt.Errorf("%s: unknown error type %T", hostID, v)
		}

	case *protocolsPb.Response_PostBondResponse:
		resp := v.PostBondResponse
		m.log.Infof("%s: bond strength is %d", hostID, resp.BondStrength)
		return nil

	default:
		return fmt.Errorf("%s: unexpected response for post bond request: %T", hostID, v)
	}
}

// postBond posts the connection's bond, retrying on invalid bond index errors. This needs to be
// called before the connection's push stream is established.
func (m *meshConnection) postBond(ctx context.Context) error {
	hostID := m.host.ID()
	for range maxPostBondRetries {
		req, err := bond.PostBondReqFromBondInfo(m.bondInfo)
		if err != nil {
			return fmt.Errorf("failed to create post bond request: %w", err)
		}

		err = m.postBondInternal(ctx, req)
		if err == nil {
			return nil
		}

		switch {
		case errors.Is(err, errInvalidBondIndex):
			// Retry if an invalid bond index error is returned.
			continue

		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return fmt.Errorf("%s: post bond retry cancelled due to context: %w", hostID, err)

		default:
			return fmt.Errorf("%s: failed to post bond: %w", hostID, err)
		}
	}

	return fmt.Errorf("%s: maximum retry attempts for posting bond reached", hostID)
}

// kill terminates the mesh connection.
func (m *meshConnection) kill() {
	m.cancelFuncMtx.RLock()
	defer m.cancelFuncMtx.RUnlock()

	if m.cancelFunc == nil {
		// Do nothing if the context cancellation is not set.
		return
	}

	m.log.Infof("%s: terminating mesh connection to peer with ID %s", m.host.ID(), m.peerID)
	m.cancelFunc()
}

// maintainPushStream starts a push stream to the remote peer and listens for messages
// on the stream. If the push stream is closed, the function will attempt to create
// a new one, maintaining a connection to the remote peer. The function will block
// until the context is done.
func (m *meshConnection) maintainPushStream(ctx context.Context) {
	reconnectTimer := time.NewTimer(0)
	defer reconnectTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconnectTimer.C:
			stream, err := m.host.NewStream(ctx, m.peerID, protocols.ClientPushProtocol)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					m.log.Errorf("%s: failed to create push stream to peer with ID %s: %v", m.host.ID(), m.peerID, err)
					return
				}

				// Periodically retry to reconnect on stream failure.
				reconnectTimer = time.NewTimer(time.Minute)
				continue
			}

			// Process push stream response.
			hostID := m.host.ID()
			resp := &protocolsPb.Response{}
			err = codec.ReadLengthPrefixedMessage(stream, resp)
			if err != nil {
				m.log.Errorf("%s: failed to read push stream response: %v", hostID, err)
				return
			}

			switch v := resp.Response.(type) {
			case *protocolsPb.Response_Success:
				// ACK, no action needed.
				m.log.Infof("%s: push stream established", hostID)

			case *protocolsPb.Response_Error:
				rErr := v.Error
				switch v := rErr.GetError().(type) {
				case *protocolsPb.Error_Unauthorized:
					m.log.Errorf("%s: unauthorized, cannot establish push stream", hostID)
					return

				default:
					m.log.Errorf("%s: unexpected push stream error response: %T", hostID, v)
					return
				}

			default:
				m.log.Errorf("%s: unexpected push stream response: %T", hostID, v)
				return
			}

			// Setting the mesh connection indicates it is ready for use.
			m.setMeshConnection(m)

			m.handlePushedData(ctx, stream)

			// Reset the reconnect timer  to trigger a reconnection.
			reconnectTimer = time.NewTimer(0)
		}
	}
}

// readPayload wraps the result of the blocking push stream read.
type readPayload struct {
	data []byte
	err  error
}

// handlePushedData listens for messages on the push stream and processes them using the
// handler function. This function will block until the context is done or the push
// stream is closed.
func (m *meshConnection) handlePushedData(ctx context.Context, pushStream network.Stream) {
	defer func() { _ = pushStream.Close() }()

	for {
		readCh := make(chan readPayload, 1)

		go func() {
			// Timeout is 0, so no deadline is set.
			data, err := codec.ReadLengthPrefixedBytes(pushStream, 0)
			readCh <- readPayload{
				data: data,
				err:  err,
			}
		}()

		select {
		case <-ctx.Done():
			m.kill()
			return
		case payload := <-readCh:
			if payload.err != nil {
				if errors.Is(payload.err, io.EOF) || errors.Is(payload.err, network.ErrReset) {
					return
				}

				m.log.Errorf("%s: failed to read message: %v", m.host.ID(), payload.err)
				continue
			}

			msg := &protocolsPb.PushMessage{}
			if err := proto.Unmarshal(payload.data, msg); err == nil {
				m.handleMessage(msg)
			}
		}
	}
}
