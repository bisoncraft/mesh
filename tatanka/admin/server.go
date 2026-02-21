package admin

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/bisoncraft/mesh/oracle"
)

// NodeConnectionState defines the status of a peer connection
type NodeConnectionState string

const (
	StateConnected         NodeConnectionState = "connected"
	StateDisconnected      NodeConnectionState = "disconnected"
	StateWhitelistMismatch NodeConnectionState = "whitelist_mismatch"

	adminRequestBodyLimitBytes = 1 << 20 // 1 MiB
	adminWebSocketReadLimit    = 1 << 20 // 1 MiB
)

// NodeInfo contains information about a specific peer
type NodeInfo struct {
	PeerID        string              `json:"peer_id"`
	State         NodeConnectionState `json:"state"`
	Addresses     []string            `json:"addresses,omitempty"`
	PeerWhitelist []string            `json:"peer_whitelist,omitempty"`
}

// AdminState represents the global admin state
type AdminState struct {
	Nodes        map[string]NodeInfo `json:"nodes"`
	OurWhitelist []string            `json:"our_whitelist"`
}

func (s AdminState) DeepCopy() AdminState {
	newState := AdminState{
		Nodes:        make(map[string]NodeInfo, len(s.Nodes)),
		OurWhitelist: make([]string, len(s.OurWhitelist)),
	}
	for k, v := range s.Nodes {
		newState.Nodes[k] = v
	}
	copy(newState.OurWhitelist, s.OurWhitelist)
	return newState
}

// WSMessage is the envelope for all WebSocket messages.
type WSMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Client represents a connected WebSocket user.
type Client struct {
	conn *websocket.Conn
	send chan WSMessage
}

// Server manages the admin server for a tatanka node.
type Server struct {
	log        slog.Logger
	httpServer *http.Server
	upgrader   websocket.Upgrader

	stateMtx sync.RWMutex
	state    AdminState

	clientsMtx sync.RWMutex
	clients    map[*Client]bool

	oracle Oracle
}

// Oracle supplies data for admin oracle endpoints.
type Oracle interface {
	OracleSnapshot() *oracle.OracleSnapshot
}

// NewServer initializes the admin server.
func NewServer(log slog.Logger, addr string, oracle Oracle) *Server {
	return &Server{
		log: log,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return allowWebSocketOrigin(r)
			},
		},
		clients: make(map[*Client]bool),
		state: AdminState{
			Nodes:        make(map[string]NodeInfo),
			OurWhitelist: []string{},
		},
		httpServer: &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		oracle: oracle,
	}
}

// Start launches the HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/ws", s.localOnly(s.handleWebSocket))
	s.httpServer.Handler = mux

	s.log.Infof("Starting admin server on %s", s.httpServer.Addr)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(shutdownCtx)
	}()

	return s.httpServer.ListenAndServe()
}

func (s *Server) localOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackRequest(r) {
			http.Error(w, "admin interface is only available from localhost", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func isLoopbackRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func allowWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Non-browser clients (e.g. tatankactl) usually don't set Origin.
		return true
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	return isLocalhostHost(u.Host)
}

func isLocalhostHost(hostPort string) bool {
	host := hostPort
	if parsedHost, _, err := net.SplitHostPort(hostPort); err == nil {
		host = parsedHost
	}

	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// UpdateConnectionState updates a specific peer's info and broadcasts to clients
func (s *Server) UpdateConnectionState(peerID peer.ID, state NodeConnectionState, addresses []string, peerWhitelist []string) {
	s.stateMtx.Lock()
	node := NodeInfo{
		PeerID:    peerID.String(),
		State:     state,
		Addresses: addresses,
	}
	if state == StateWhitelistMismatch {
		node.PeerWhitelist = peerWhitelist
	}
	s.state.Nodes[peerID.String()] = node
	snapshot := s.state.DeepCopy()
	s.stateMtx.Unlock()

	s.broadcastState(snapshot)
}

// UpdateWhitelist updates the local whitelist and broadcasts to clients
func (s *Server) UpdateWhitelist(whitelist []string) {
	s.stateMtx.Lock()
	s.state.OurWhitelist = whitelist
	snapshot := s.state.DeepCopy()
	s.stateMtx.Unlock()

	s.broadcastState(snapshot)
}

// broadcastState sends the admin state to all clients.
func (s *Server) broadcastState(state AdminState) {
	data, err := json.Marshal(state)
	if err != nil {
		s.log.Errorf("Failed to marshal admin state: %v", err)
		return
	}
	msg := WSMessage{
		Type: "admin_state",
		Data: json.RawMessage(data),
	}
	s.broadcast(msg)
}

// BroadcastOracleUpdate broadcasts a typed oracle update to all connected clients.
func (s *Server) BroadcastOracleUpdate(msgType string, snapshotDiff *oracle.OracleSnapshot) {
	data, err := json.Marshal(snapshotDiff)
	if err != nil {
		s.log.Errorf("Failed to marshal oracle update (%s): %v", msgType, err)
		return
	}
	msg := WSMessage{
		Type: msgType,
		Data: json.RawMessage(data),
	}
	s.broadcast(msg)
}

