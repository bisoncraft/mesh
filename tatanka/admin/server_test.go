package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIsLoopbackRequest(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		want       bool
	}{
		{name: "ipv4 loopback", remoteAddr: "127.0.0.1:9000", want: true},
		{name: "ipv6 loopback", remoteAddr: "[::1]:9000", want: true},
		{name: "non loopback", remoteAddr: "192.168.1.10:9000", want: false},
		{name: "invalid remote addr", remoteAddr: "localhost:9000", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/ws", nil)
			req.RemoteAddr = tc.remoteAddr
			got := isLoopbackRequest(req)
			if got != tc.want {
				t.Fatalf("isLoopbackRequest(%q) = %v, want %v", tc.remoteAddr, got, tc.want)
			}
		})
	}
}

func TestAllowWebSocketOrigin(t *testing.T) {
	tests := []struct {
		name   string
		origin string
		want   bool
	}{
		{name: "empty origin allowed", origin: "", want: true},
		{name: "localhost origin", origin: "http://localhost:2000", want: true},
		{name: "loopback ipv4 origin", origin: "http://127.0.0.1:2000", want: true},
		{name: "loopback ipv6 origin", origin: "http://[::1]:2000", want: true},
		{name: "non loopback origin", origin: "https://example.com", want: false},
		{name: "malformed origin", origin: "://bad-origin", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/ws", nil)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			got := allowWebSocketOrigin(req)
			if got != tc.want {
				t.Fatalf("allowWebSocketOrigin(%q) = %v, want %v", tc.origin, got, tc.want)
			}
		})
	}
}

func TestLocalOnlyMiddleware(t *testing.T) {
	s := &Server{}

	tests := []struct {
		name       string
		remoteAddr string
		wantStatus int
		wantCalled bool
	}{
		{name: "loopback allowed", remoteAddr: "127.0.0.1:1111", wantStatus: http.StatusNoContent, wantCalled: true},
		{name: "non loopback forbidden", remoteAddr: "10.0.0.5:1111", wantStatus: http.StatusForbidden, wantCalled: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			})

			req := httptest.NewRequest(http.MethodGet, "/admin/propose-whitelist", nil)
			req.RemoteAddr = tc.remoteAddr
			rr := httptest.NewRecorder()

			s.localOnly(next).ServeHTTP(rr, req)

			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tc.wantStatus)
			}
			if called != tc.wantCalled {
				t.Fatalf("handler called = %v, want %v", called, tc.wantCalled)
			}
		})
	}
}
