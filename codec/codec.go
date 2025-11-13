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
)

// ReadLengthPrefixedMessage reads a 4-byte big-endian length prefix,
// then reads that many bytes and unmarshals them into the provided
// proto.Message. Returns an error if the length exceeds maxMessageSize.
func ReadLengthPrefixedMessage(s network.Stream, msg proto.Message) error {
	lengthBuf := make([]byte, lengthPrefixSize)
	if _, err := io.ReadFull(s, lengthBuf); err != nil {
		return fmt.Errorf("failed to read length prefix: %w", err)
	}

	msgLen := binary.BigEndian.Uint32(lengthBuf)
	if msgLen > MaxMessageSize {
		return fmt.Errorf("message size %d exceeds max size %d", msgLen, MaxMessageSize)
	}
	if msgLen == 0 {
		return fmt.Errorf("message size is 0")
	}

	data := make([]byte, msgLen)
	if _, err := io.ReadFull(s, data); err != nil {
		return fmt.Errorf("failed to read message data: %w", err)
	}

	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
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

// WriteLengthPrefixedMessage writes a 4-byte big-endian length prefix,
// then the marshalled proto message to the stream.
func WriteLengthPrefixedMessage(s network.Stream, msg proto.Message) error {
	data, err := MarshalProtoWithLengthPrefix(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if _, err := s.Write(data); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// DeadlineNotSupportedError returns true if the error is due to the stream
// not supporting deadlines. This is the case for the mocknet.
func DeadlineNotSupportedError(err error) bool {
	return strings.Contains(err.Error(), "deadline not supported")
}

// SetReadDeadline sets a read deadline on the stream. If deadline is not
// supported, which is the case for the mocknet, we ignore the error.
func SetReadDeadline(timeout time.Duration, s network.Stream) error {
	if err := s.SetReadDeadline(time.Now().Add(timeout)); err != nil && !DeadlineNotSupportedError(err) {
		return err
	}

	return nil
}
