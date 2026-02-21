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

	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/decred/slog"
	"github.com/gorilla/websocket"
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

// PeerInfo contains information about a specific peer
type PeerInfo struct {
	PeerID         string                `json:"peer_id"`
	State          NodeConnectionState   `json:"state"`
	Addresses      []string              `json:"addresses,omitempty"`
	WhitelistState *types.WhitelistState `json:"whitelist_state,omitempty"`
}

// AdminState represents the global admin state
type AdminState struct {
	OurPeerID      string                `json:"our_peer_id"`
	WhitelistState *types.WhitelistState `json:"whitelist_state"`
	Peers          map[string]PeerInfo   `json:"peers"` // remote peers only
}

// WhitelistUpdate is sent when whitelist membership changes.
type WhitelistUpdate struct {
	WhitelistState *types.WhitelistState `json:"whitelist_state"`
}

// WSMessage is the envelope for all WebSocket messages.
type WSMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// client represents a connected WebSocket user.
type client struct {
	conn *websocket.Conn
	send chan WSMessage
}

// Config holds the configuration for the admin server.
type Config struct {
	Log              slog.Logger
	Addr             string
	PeerID           string
	Oracle           Oracle
	GetState         func() AdminState
	ProposeWhitelist func(peers []string) error
	ClearProposal    func()
	ForceWhitelist   func(peers []string) error
}

func (c *Config) verify() {
	if c.Log == nil {
		panic("admin.Config.Log is nil")
	}
	if c.Addr == "" {
		panic("admin.Config.Addr is empty")
	}
	if c.Oracle == nil {
		panic("admin.Config.Oracle is nil")
	}
	if c.GetState == nil {
		panic("admin.Config.GetState is nil")
	}
	if c.ProposeWhitelist == nil {
		panic("admin.Config.ProposeWhitelist is nil")
	}
	if c.ClearProposal == nil {
		panic("admin.Config.ClearProposal is nil")
	}
	if c.ForceWhitelist == nil {
		panic("admin.Config.ForceWhitelist is nil")
	}
}

// Server manages the admin server for a tatanka node.
type Server struct {
	log              slog.Logger
	oracle           Oracle
	getState         func() AdminState
	proposeWhitelist func(peers []string) error
	clearProposal    func()
	forceWhitelist   func(peers []string) error

	httpServer *http.Server
	upgrader   websocket.Upgrader

	clientsMtx sync.RWMutex
	clients    map[*client]bool
}

// Oracle supplies data for admin oracle endpoints.
type Oracle interface {
	OracleSnapshot() *oracle.OracleSnapshot
}

// NewServer initializes the admin server.
func NewServer(cfg *Config) *Server {
	cfg.verify()
	return &Server{
		log:              cfg.Log,
		oracle:           cfg.Oracle,
		getState:         cfg.GetState,
		proposeWhitelist: cfg.ProposeWhitelist,
		clearProposal:    cfg.ClearProposal,
		forceWhitelist:   cfg.ForceWhitelist,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return allowWebSocketOrigin(r)
			},
		},
		clients: make(map[*client]bool),
		httpServer: &http.Server{
			Addr:              cfg.Addr,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

// Start launches the HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/ws", s.localOnly(s.handleWebSocket))
	mux.HandleFunc("/admin/propose-whitelist", s.localOnly(s.handleProposeWhitelist))
	mux.HandleFunc("/admin/adopt-whitelist", s.localOnly(s.handleAdoptWhitelist))
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

// BroadcastPeerUpdate sends a single peer update to all connected clients.
func (s *Server) BroadcastPeerUpdate(pi PeerInfo) {
	data, err := json.Marshal(pi)
	if err != nil {
		s.log.Errorf("Failed to marshal peer update: %v", err)
		return
	}
	s.broadcast(WSMessage{Type: "peer_update", Data: json.RawMessage(data)})
}

// BroadcastWhitelistState sends our own whitelist state to all connected clients.
func (s *Server) BroadcastWhitelistState(ws *types.WhitelistState) {
	data, err := json.Marshal(ws)
	if err != nil {
		s.log.Errorf("Failed to marshal whitelist state: %v", err)
		return
	}
	s.broadcast(WSMessage{Type: "whitelist_state", Data: json.RawMessage(data)})
}

// BroadcastWhitelistUpdate sends whitelist membership changes to all connected clients.
func (s *Server) BroadcastWhitelistUpdate(wu WhitelistUpdate) {
	data, err := json.Marshal(wu)
	if err != nil {
		s.log.Errorf("Failed to marshal whitelist update: %v", err)
		return
	}
	s.broadcast(WSMessage{Type: "whitelist_update", Data: json.RawMessage(data)})
}

// BroadcastOracleUpdate broadcasts a typed oracle update to all connected clients.
func (s *Server) BroadcastOracleUpdate(msgType string, snapshotDiff *oracle.OracleSnapshot) {
	data, err := json.Marshal(snapshotDiff)
	if err != nil {
		s.log.Errorf("Failed to marshal oracle update (%s): %v", msgType, err)
		return
	}
	s.broadcast(WSMessage{Type: msgType, Data: json.RawMessage(data)})
}
