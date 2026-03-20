package codec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/bisoncraft/mesh/protocols"
	"google.golang.org/protobuf/proto"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
)

const lengthPrefixBytes = 4 // Size of the 4-byte big-endian length prefix

// mockStream is a test mock that implements network.Stream for testing deadline errors
type mockStream struct {
	readDeadlineErr  error
	writeDeadlineErr error
	closeErr         error
	readData         []byte
	readPos          int
	readErr          error
	writeErr         error
}

func (m *mockStream) Read(p []byte) (int, error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	if m.readPos >= len(m.readData) {
		return 0, io.EOF
	}
	n := copy(p, m.readData[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockStream) Write(p []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return len(p), nil
}

func (m *mockStream) Close() error {
	return m.closeErr
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) Reset() error {
	return nil
}

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	return nil
}

func (m *mockStream) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	return m.readDeadlineErr
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return m.writeDeadlineErr
}

func (m *mockStream) Conn() network.Conn {
	return nil
}

func (m *mockStream) Scope() network.StreamScope {
	return nil
}

func (m *mockStream) ID() string {
	return "mock-stream"
}

func (m *mockStream) Protocol() protocol.ID {
	return protocol.ID("mock")
}

func (m *mockStream) SetProtocol(pid protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func createHost(t *testing.T, port int) (host.Host, string) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	host, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
	)
	if err != nil {
		t.Fatal(err)
	}

	return host, fmt.Sprintf("%s/p2p/%s", addr, host.ID().String())
}

func TestReadWriteHelpers(t *testing.T) {
	// Create host A.
	hostAPort := 5577
	hostA, hostAAddr := createHost(t, hostAPort)
	defer func() { _ = hostA.Close() }()

	hostAReceivedMsgs := make(chan proto.Message, 10)
	hostASendHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		// Ensure reading messages succeeds.
		id := s.Conn().RemotePeer()
		msg := &protocolsPb.PublishRequest{}
		if err := ReadLengthPrefixedMessage(s, msg); err != nil {
			t.Fatalf("Failed to read message %s: %v", id, err)
			return
		}

		hostAReceivedMsgs <- msg
	}

	hostA.SetStreamHandler(protocols.ClientPublishProtocol, hostASendHandler)

	// Create host B.
	hostBPort := 5588
	hostB, _ := createHost(t, hostBPort)
	defer func() { _ = hostB.Close() }()

	hostBReceivedMsgs := make(chan proto.Message, 10)
	hostBSendHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		// Ensure reading messages with a timeout succeeds.
		id := s.Conn().RemotePeer()
		msg := &protocolsPb.PublishRequest{}
		if err := ReadLengthPrefixedMessage(s, msg, time.Second*5); err != nil {
			t.Fatalf("Failed to read message %s: %v", id, err)
			return
		}

		hostBReceivedMsgs <- msg
	}

	hostB.SetStreamHandler(protocols.ClientPublishProtocol, hostBSendHandler)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	aAddr, err := ma.NewMultiaddr(hostAAddr)
	if err != nil {
		t.Fatalf("Failed to parse host address: %v", err)
	}

	// Connect B to A.
	err = hostB.Connect(ctx, peer.AddrInfo{
		ID:    hostA.ID(),
		Addrs: []ma.Multiaddr{aAddr},
	})
	if err != nil {
		t.Fatalf("Failed to connect B to A: %v", err)
	}

	// Ensure writing messages succeeds.
	s, err := hostB.NewStream(ctx, hostA.ID(), protocols.ClientPublishProtocol)
	if err != nil {
		t.Fatalf("Failed to create a stream to A: %v", err)
	}

	defer func() { _ = s.Close() }()

	testTopic := "test"
	data := []byte("hello")
	msg := &protocolsPb.PublishRequest{
		Topic: testTopic,
		Data:  data,
	}

	err = WriteLengthPrefixedMessage(s, msg)
	if err != nil {
		t.Fatalf("Failed to write message to host B: %v", err)
	}

	select {
	case rcvMsg := <-hostAReceivedMsgs:
		pubMsg, ok := rcvMsg.(*protocolsPb.PublishRequest)
		if !ok {
			t.Fatal("Received message is not a PublishRequest")
		}

		if pubMsg.Topic != testTopic {
			t.Fatalf("Expected topic %s, got %s", testTopic, pubMsg.Topic)
		}

		if !bytes.Equal(pubMsg.Data, data) {
			t.Fatalf("Expected data %s, got %s", string(data), string(pubMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive message")
	}

	// Ensure writing messages with a timeout succeeds.
	s, err = hostA.NewStream(ctx, hostB.ID(), protocols.ClientPublishProtocol)
	if err != nil {
		t.Fatalf("Failed to create a stream to A: %v", err)
	}

	defer func() { _ = s.Close() }()

	err = WriteLengthPrefixedMessage(s, msg, time.Second*5)
	if err != nil {
		t.Fatalf("Failed to write message to host B: %v", err)
	}

	select {
	case rcvMsg := <-hostBReceivedMsgs:
		pubMsg, ok := rcvMsg.(*protocolsPb.PublishRequest)
		if !ok {
			t.Fatal("Received message is not a PublishRequest")
		}

		if pubMsg.Topic != testTopic {
			t.Fatalf("Expected topic %s, got %s", testTopic, pubMsg.Topic)
		}

		if !bytes.Equal(pubMsg.Data, data) {
			t.Fatalf("Expected data %s, got %s", string(data), string(pubMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive message")
	}
}

func TestMarshalProtoWithLengthPrefix(t *testing.T) {
	tests := []struct {
		name    string
		msg     proto.Message
		wantErr bool
		check   func(t *testing.T, data []byte)
	}{
		{
			name:    "valid message",
			msg:     &protocolsPb.PublishRequest{Topic: "test", Data: []byte("hello")},
			wantErr: false,
			check: func(t *testing.T, data []byte) {
				// Should have lengthPrefixBytes + marshalled data
				if len(data) <= lengthPrefixBytes {
					t.Fatal("data too short, should have length prefix + content")
				}
				// Verify we can extract and unmarshal it
				msgLen := binary.BigEndian.Uint32(data[:lengthPrefixBytes])
				if msgLen != uint32(len(data)-lengthPrefixBytes) {
					t.Fatalf("length prefix mismatch: got %d, want %d", msgLen, len(data)-lengthPrefixBytes)
				}
				msg := &protocolsPb.PublishRequest{}
				if err := proto.Unmarshal(data[lengthPrefixBytes:], msg); err != nil {
					t.Fatalf("failed to unmarshal: %v", err)
				}
				if msg.Topic != "test" || !bytes.Equal(msg.Data, []byte("hello")) {
					t.Fatal("unmarshalled message doesn't match original")
				}
			},
		},
		{
			name:    "empty message",
			msg:     &protocolsPb.PublishRequest{},
			wantErr: false,
			check: func(t *testing.T, data []byte) {
				if len(data) < lengthPrefixBytes {
					t.Fatal("data should have at least length prefix")
				}
				msgLen := binary.BigEndian.Uint32(data[:lengthPrefixBytes])
				if int(msgLen) != len(data)-lengthPrefixBytes {
					t.Fatalf("length prefix mismatch: got %d, want %d", msgLen, len(data)-lengthPrefixBytes)
				}
			},
		},
		{
			name:    "message with large data",
			msg:     &protocolsPb.PublishRequest{Topic: "test", Data: bytes.Repeat([]byte("x"), 500)},
			wantErr: false,
			check: func(t *testing.T, data []byte) {
				if len(data) <= lengthPrefixBytes {
					t.Fatal("data too short")
				}
				msgLen := binary.BigEndian.Uint32(data[:lengthPrefixBytes])
				if msgLen != uint32(len(data)-lengthPrefixBytes) {
					t.Fatalf("length prefix mismatch: got %d, want %d", msgLen, len(data)-lengthPrefixBytes)
				}
			},
		},
		{
			name:    "message exceeding MaxMessageSize",
			msg:     &protocolsPb.PublishRequest{Topic: "test", Data: bytes.Repeat([]byte("x"), MaxMessageSize+1)},
			wantErr: true,
			check: func(t *testing.T, data []byte) {
				if data != nil {
					t.Fatal("data should be nil on error")
				}
			},
		},
		{
			name:    "message exactly at MaxMessageSize",
			msg:     &protocolsPb.PublishRequest{Topic: "test", Data: bytes.Repeat([]byte("x"), MaxMessageSize-20)},
			wantErr: false,
			check: func(t *testing.T, data []byte) {
				if len(data) <= lengthPrefixBytes {
					t.Fatal("data too short")
				}
				msgLen := binary.BigEndian.Uint32(data[:lengthPrefixBytes])
				if msgLen > uint32(MaxMessageSize) {
					t.Fatalf("message size %d exceeds MaxMessageSize %d", msgLen, MaxMessageSize)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := MarshalProtoWithLengthPrefix(tt.msg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MarshalProtoWithLengthPrefix() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.check(t, data)
		})
	}
}

func TestDeadlineErrors(t *testing.T) {
	errorTests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{
			name:    "deadline not supported error is ignored",
			err:     fmt.Errorf("deadline not supported"),
			wantErr: false,
		},
		{
			name:    "other errors are propagated",
			err:     fmt.Errorf("network error"),
			wantErr: true,
		},
		{
			name:    "nil error succeeds",
			err:     nil,
			wantErr: false,
		},
	}

	t.Run("setReadDeadline", func(t *testing.T) {
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				stream := &mockStream{readDeadlineErr: tt.err}
				err := setReadDeadline(time.Second, stream)
				if (err != nil) != tt.wantErr {
					t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("clearReadDeadline", func(t *testing.T) {
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				stream := &mockStream{readDeadlineErr: tt.err}
				err := clearReadDeadline(stream)
				if (err != nil) != tt.wantErr {
					t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("setWriteDeadline", func(t *testing.T) {
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				stream := &mockStream{writeDeadlineErr: tt.err}
				err := setWriteDeadline(time.Second, stream)
				if (err != nil) != tt.wantErr {
					t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("clearWriteDeadline", func(t *testing.T) {
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				stream := &mockStream{writeDeadlineErr: tt.err}
				err := clearWriteDeadline(stream)
				if (err != nil) != tt.wantErr {
					t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})
}

func TestReadLengthPrefixedBytesValidation(t *testing.T) {
	tests := []struct {
		name        string
		lengthValue uint32
		wantErr     bool
		errContains string
	}{
		{
			name:        "message size exceeds MaxMessageSize",
			lengthValue: MaxMessageSize + 1,
			wantErr:     true,
			errContains: "exceeds max size",
		},
		{
			name:        "message size at MaxMessageSize boundary",
			lengthValue: MaxMessageSize,
			wantErr:     false,
			errContains: "",
		},
		{
			name:        "zero-length message",
			lengthValue: 0,
			wantErr:     false,
			errContains: "",
		},
		{
			name:        "normal message size",
			lengthValue: 100,
			wantErr:     false,
			errContains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create length prefix in big-endian format
			lengthBuf := make([]byte, lengthPrefixBytes)
			binary.BigEndian.PutUint32(lengthBuf, tt.lengthValue)

			// For non-error cases, add dummy message data
			var readData []byte
			readData = append(readData, lengthBuf...)
			if tt.lengthValue > 0 && tt.lengthValue <= MaxMessageSize {
				readData = append(readData, bytes.Repeat([]byte("x"), int(tt.lengthValue))...)
			}

			stream := &mockStream{readData: readData}
			data, err := ReadLengthPrefixedBytes(stream)

			if (err != nil) != tt.wantErr {
				t.Fatalf("ReadLengthPrefixedBytes() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr && err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error message %q should contain %q", err.Error(), tt.errContains)
				}
			}

			// For zero-length message, verify nil is returned
			if tt.lengthValue == 0 && data != nil {
				t.Fatalf("zero-length message should return nil, got %v", data)
			}

			// For valid non-zero messages, verify data length matches
			if !tt.wantErr && tt.lengthValue > 0 && len(data) != int(tt.lengthValue) {
				t.Fatalf("data length %d should match message length %d", len(data), tt.lengthValue)
			}
		})
	}
}

func TestErrorPaths(t *testing.T) {
	t.Run("ReadLengthPrefixedMessage_unmarshal_error", func(t *testing.T) {
		// Invalid protobuf data returns unmarshal error
		lengthBuf := make([]byte, lengthPrefixBytes)
		binary.BigEndian.PutUint32(lengthBuf, 5)
		invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

		stream := &mockStream{readData: append(lengthBuf, invalidData...)}
		msg := &protocolsPb.PublishRequest{}

		err := ReadLengthPrefixedMessage(stream, msg)
		if err == nil {
			t.Fatal("expected unmarshal error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to unmarshal") {
			t.Fatalf("error should contain 'failed to unmarshal', got: %v", err)
		}
	})

	t.Run("WriteLengthPrefixedBytes_write_error", func(t *testing.T) {
		// Stream write error is propagated
		stream := &mockStream{
			writeErr: fmt.Errorf("write failed"),
		}

		err := WriteLengthPrefixedBytes(stream, []byte("test data"))
		if err == nil {
			t.Fatal("expected write error, got nil")
		}
	})

	t.Run("ReadLengthPrefixedBytes_read_deadline_error", func(t *testing.T) {
		// Read deadline setting error is propagated
		stream := &mockStream{
			readDeadlineErr: fmt.Errorf("permission denied"),
		}

		_, err := ReadLengthPrefixedBytes(stream, time.Second)
		if err == nil {
			t.Fatal("expected deadline error, got nil")
		}
		if !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("error should propagate underlying cause, got: %v", err)
		}
	})

	t.Run("WriteLengthPrefixedBytes_write_deadline_error", func(t *testing.T) {
		// Write deadline setting error is propagated
		stream := &mockStream{
			writeDeadlineErr: fmt.Errorf("connection reset"),
		}

		err := WriteLengthPrefixedBytes(stream, []byte("data"), time.Second)
		if err == nil {
			t.Fatal("expected deadline error, got nil")
		}
		if !strings.Contains(err.Error(), "connection reset") {
			t.Fatalf("error should propagate underlying cause, got: %v", err)
		}
	})
}
