package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

const (
	maxMessageSize = 1024 // 1KB max message size
)

type broadcastPayload struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

type subscribePayload struct {
	Topic string `json:"topic"`
}

type bondPayload struct {
	ID       string `json:"id"`
	Expiry   int64  `json:"expiry"`
	Strength uint32 `json:"strength"`
}

// readRequest reads and decodes the JSON body of the request into the target struct.
func readRequest(r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(http.ResponseWriter(nil), r.Body, maxMessageSize)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("failed to decode request: %w", err)
	}

	return nil
}

// writeResponse writes the provided status code and JSON-encoded payload to the response.
func writeResponse(w http.ResponseWriter, statusCode int, payload any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	return nil
}

// writeStatusResponse writes the provided status code as response.
func writeStatusResponse(w http.ResponseWriter, statusCode int) error {
	w.WriteHeader(statusCode)

	return nil
}

// writeErrorResponse write the provided error and status code as response.
func writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	payload := map[string]string{"error": message}
	writeResponse(w, statusCode, payload)
}

// LogStreamBackend represents a log streaming slog backend.
type LogStreamBackend struct {
	mtx    sync.Mutex
	stream chan string
}

func NewLogStreamBackend(stream chan string) *LogStreamBackend {
	return &LogStreamBackend{
		stream: stream,
	}
}

// Ensure the log streaming backend implements the io.Writer interface.
var _ io.Writer = (*LogStreamBackend)(nil)

// Write writes the provided data to the the log stream.
func (l *LogStreamBackend) Write(data []byte) (int, error) {
	logLine := string(data)
	select {
	case l.stream <- logLine:
		return len(data), nil
	default:
		// Fallthrough
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()

	// Purge oldest slots of the stream channel if it is full.
	if len(l.stream) == cap(l.stream) {
		const purgeThreshold = 50
		for range purgeThreshold {
			<-l.stream
		}
	}

	// Write the latest log line.
	l.stream <- logLine

	return len(data), nil
}

// Close terminates the stream backend.
func (l *LogStreamBackend) Close() {
	close(l.stream)
}
