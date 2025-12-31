package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NodeConnectionState defines the status of a peer connection
type NodeConnectionState string

const (
	StateConnected         NodeConnectionState = "connected"
	StateDisconnected      NodeConnectionState = "disconnected"
	StateWhitelistMismatch NodeConnectionState = "whitelist_mismatch"
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

// Client represents a connected WebSocket user.
type Client struct {
	conn *websocket.Conn
	send chan AdminState
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
}

// NewServer initializes the admin server
func NewServer(log slog.Logger, addr string) *Server {
	return &Server{
		log:      log,
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		clients:  make(map[*Client]bool),
		state: AdminState{
			Nodes:        make(map[string]NodeInfo),
			OurWhitelist: []string{},
		},
		httpServer: &http.Server{Addr: addr},
	}
}

// Start launches the HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/state", s.handleGetState)
	mux.HandleFunc("/admin/ws", s.handleWebSocket)
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

// broadcastState sends the state to all clients.
func (s *Server) broadcastState(state AdminState) {
	s.clientsMtx.RLock()
	defer s.clientsMtx.RUnlock()

	for client := range s.clients {
		select {
		case client.send <- state:
		default:
			s.log.Errorf("Client buffer full, skipping update")
		}
	}
}

func (s *Server) handleGetState(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.stateMtx.RLock()
	state := s.state.DeepCopy()
	s.stateMtx.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warnf("WebSocket upgrade failed: %v", err)
		return
	}

	client := &Client{
		conn: conn,
		send: make(chan AdminState, 10),
	}

	s.clientsMtx.Lock()
	s.clients[client] = true
	s.clientsMtx.Unlock()

	// Send initial state immediately
	s.stateMtx.RLock()
	initialState := s.state.DeepCopy()
	s.stateMtx.RUnlock()
	select {
	case client.send <- initialState:
	default:
	}

	// 1. Writer Goroutine
	go func() {
		defer conn.Close()
		for state := range client.send {
			if err := conn.WriteJSON(state); err != nil {
				return
			}
		}
	}()

	// 2. Reader Goroutine
	go func() {
		defer func() {
			s.clientsMtx.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client.send)
			}
			s.clientsMtx.Unlock()
			conn.Close()
		}()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}
