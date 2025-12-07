package codec

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"google.golang.org/protobuf/proto"
)

const (
	MaxMessageSize   = 1024 // 1KB max message size
	lengthPrefixSize = 4
	DefaultTimeout   = 10 * time.Second
)

// ReadLengthPrefixedBytes reads a 4-byte big-endian length prefix from the
// stream, then reads that many bytes. A read deadline is applied using the
// provided timeout or DefaultTimeout; if timeout is 0, no deadline is set.
func ReadLengthPrefixedBytes(s network.Stream, timeout ...time.Duration) ([]byte, error) {
	useTimeout := resolveTimeout(timeout)

	if useTimeout > 0 {
		if err := setReadDeadline(useTimeout, s); err != nil {
			return nil, err
		}
		defer func() { _ = clearReadDeadline(s) }()
	}

	lengthBuf := make([]byte, lengthPrefixSize)
	if _, err := io.ReadFull(s, lengthBuf); err != nil {
		return nil, fmt.Errorf("failed to read length prefix: %w", err)
	}

	msgLen := binary.BigEndian.Uint32(lengthBuf)
	if msgLen > MaxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds max size %d", msgLen, MaxMessageSize)
	}
	if msgLen == 0 {
		return nil, fmt.Errorf("message size is 0")
	}

	data := make([]byte, msgLen)
	if _, err := io.ReadFull(s, data); err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	return data, nil
}

// ReadLengthPrefixedMessage reads a 4-byte big-endian length prefix from the
// stream, then reads that many bytes and unmarshals them into the provided
// proto.Message. The DefaultTimeout will be used for the request unless
// otherwise specified in the params.
func ReadLengthPrefixedMessage(s network.Stream, msg proto.Message, timeout ...time.Duration) error {
	data, err := ReadLengthPrefixedBytes(s, timeout...)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}

// WriteLengthPrefixedBytes writes a 4-byte big-endian length prefix followed by
// the provided data to the stream. A write deadline is applied using the
// provided timeout or DefaultTimeout; if timeout is 0, no deadline is set.
func WriteLengthPrefixedBytes(s network.Stream, data []byte, timeout ...time.Duration) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("message size %d exceeds max size %d", len(data), MaxMessageSize)
	}

	lengthBytes := make([]byte, lengthPrefixSize)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))

	return WriteBytes(s, append(lengthBytes, data...), timeout...)
}

// WriteLengthPrefixedMessage writes a 4-byte big-endian length prefix,
// then the marshalled proto message to the stream. A write deadline is applied
// using the provided timeout or DefaultTimeout. If timeout is 0 no deadline is set.
func WriteLengthPrefixedMessage(s network.Stream, msg proto.Message, timeout ...time.Duration) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return WriteLengthPrefixedBytes(s, data, timeout...)
}

// MarshalProtoWithLengthPrefix marshals a proto message and adds a 4-byte
// length prefix.
func MarshalProtoWithLengthPrefix(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds max size %d", len(data), MaxMessageSize)
	}

	lengthBytes := make([]byte, lengthPrefixSize)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))
	return append(lengthBytes, data...), nil
}

// WriteBytes writes data with a write deadline, then clears the deadline.
// If timeout <= 0 no deadline is set. Deadline-not-supported errors are ignored.
// When no timeout is provided, DefaultTimeout is used.
func WriteBytes(s network.Stream, data []byte, timeout ...time.Duration) error {
	useTimeout := resolveTimeout(timeout)

	if useTimeout > 0 {
		if err := setWriteDeadline(useTimeout, s); err != nil {
			return err
		}
		defer func() {
			_ = clearWriteDeadline(s)
		}()
	}

	_, err := s.Write(data)
	return err
}

// isDeadlineNotSupportedError returns true if the error is due to the stream
// not supporting deadlines.
func isDeadlineNotSupportedError(err error) bool {
	return strings.Contains(err.Error(), "deadline not supported")
}

func setReadDeadline(timeout time.Duration, s network.Stream) error {
	if err := s.SetReadDeadline(time.Now().Add(timeout)); err != nil && !isDeadlineNotSupportedError(err) {
		return err
	}

	return nil
}

func clearReadDeadline(s network.Stream) error {
	if err := s.SetReadDeadline(time.Time{}); err != nil && !isDeadlineNotSupportedError(err) {
		return err
	}

	return nil
}

func setWriteDeadline(timeout time.Duration, s network.Stream) error {
	if err := s.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !isDeadlineNotSupportedError(err) {
		return err
	}

	return nil
}

func clearWriteDeadline(s network.Stream) error {
	if err := s.SetWriteDeadline(time.Time{}); err != nil && !isDeadlineNotSupportedError(err) {
		return err
	}

	return nil
}

func resolveTimeout(timeout []time.Duration) time.Duration {
	if len(timeout) == 0 {
		return DefaultTimeout
	}

	return timeout[0]
}
