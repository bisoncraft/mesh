package tatanka

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"google.golang.org/protobuf/proto"
)

// readLengthPrefixedMessage reads a 4-byte big-endian length prefix,
// then reads that many bytes and unmarshals them into the provided
// proto.Message. Returns an error if the length exceeds maxMessageSize.
func readLengthPrefixedMessage(s network.Stream, msg proto.Message) error {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(s, lengthBuf); err != nil {
		return fmt.Errorf("failed to read length prefix: %w", err)
	}

	msgLen := binary.BigEndian.Uint32(lengthBuf)
	if msgLen > maxMessageSize {
		return fmt.Errorf("message size %d exceeds max size %d", msgLen, maxMessageSize)
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

// marshalProtoWithLengthPrefix marshals a proto message and adds a 4-byte
// length prefix.
func marshalProtoWithLengthPrefix(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if len(data) > maxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds max size %d", len(data), maxMessageSize)
	}

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))
	return append(lengthBytes, data...), nil
}

// writeLengthPrefixedMessage writes a 4-byte big-endian length prefix,
// then the marshalled proto message to the stream.
func writeLengthPrefixedMessage(s network.Stream, msg proto.Message) error {
	data, err := marshalProtoWithLengthPrefix(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if _, err := s.Write(data); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// deadlineNotSupportedError returns true if the error is due to the stream
// not supporting deadlines. This is the case for the mocknet.
func deadlineNotSupportedError(err error) bool {
	return strings.Contains(err.Error(), "deadline not supported")
}

// setReadDeadline sets a read deadline on the stream. If deadline is not
// supported, which is the case for the mocknet, we ignore the error.
func setReadDeadline(timeout time.Duration, s network.Stream) error {
	if err := s.SetReadDeadline(time.Now().Add(timeout)); err != nil && !deadlineNotSupportedError(err) {
		return err
	}

	return nil
}
