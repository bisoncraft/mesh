package admin

import (
	"encoding/json"
	"net/http"
)

// broadcast sends a WSMessage to all connected clients.
func (s *Server) broadcast(msg WSMessage) {
	s.clientsMtx.RLock()
	defer s.clientsMtx.RUnlock()

	for c := range s.clients {
		select {
		case c.send <- msg:
		default:
			s.log.Errorf("Client buffer full, skipping update")
		}
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warnf("WebSocket upgrade failed: %v", err)
		return
	}
	conn.SetReadLimit(adminWebSocketReadLimit)

	c := &client{
		conn: conn,
		send: make(chan WSMessage, 10),
	}

	s.clientsMtx.Lock()
	s.clients[c] = true
	s.clientsMtx.Unlock()

	// Send initial admin state
	initialState := s.getState()
	stateData, err := json.Marshal(initialState)
	if err == nil {
		select {
		case c.send <- WSMessage{Type: "admin_state", Data: json.RawMessage(stateData)}:
		default:
		}
	}

	// Send oracle snapshot
	snapshot := s.oracle.OracleSnapshot()
	if snapshot != nil {
		snapshotData, err := json.Marshal(snapshot)
		if err == nil {
			select {
			case c.send <- WSMessage{Type: "oracle_snapshot", Data: json.RawMessage(snapshotData)}:
			default:
			}
		}
	}

	// Writer goroutine
	go func() {
		defer conn.Close()
		for msg := range c.send {
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		}
	}()

	// Reader goroutine
	go func() {
		defer func() {
			s.clientsMtx.Lock()
			if _, ok := s.clients[c]; ok {
				delete(s.clients, c)
				close(c.send)
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
