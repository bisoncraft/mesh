package tatanka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

func newTestLogger() slog.Logger {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	log.SetLevel(slog.LevelOff)
	return log
}

type lazyString func() string

func (ls lazyString) String() string {
	return ls()
}

// TestBootstrapListWhitelistUpdate verifies that bootstrap list file and HTTP
// outputs reflect whitelist changes.
func TestBootstrapListWhitelistUpdate(t *testing.T) {
	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mnet.Hosts()
	h := hosts[0]

	addr1, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	addr2, _ := ma.NewMultiaddr("/ip4/5.6.7.8/tcp/5678")
	h.Peerstore().AddAddrs(hosts[1].ID(), []ma.Multiaddr{addr1}, 1<<62)
	h.Peerstore().AddAddrs(hosts[2].ID(), []ma.Multiaddr{addr2}, 1<<62)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "bootstrap.json")
	port := reserveHTTPPort(t)
	url := fmt.Sprintf("http://127.0.0.1:%d/bootstrap", port)

	// Start with 2 peers.
	whitelist := map[peer.ID]struct{}{
		hosts[0].ID(): {},
		hosts[1].ID(): {},
	}
	wantInitial := map[string][]string{
		hosts[0].ID().String(): normalizeAddrStrings(localBootstrapAddrStrings(h.Addrs(), true)),
		hosts[1].ID().String(): {addr1.String()},
	}
	wantUpdated := map[string][]string{
		hosts[0].ID().String(): normalizeAddrStrings(localBootstrapAddrStrings(h.Addrs(), true)),
		hosts[1].ID().String(): {addr1.String()},
		hosts[2].ID().String(): {addr2.String()},
	}

	pub := newBootstrapListPublisher(newTestLogger(), h, func() map[peer.ID]struct{} {
		return whitelist
	}, filePath, port, true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pub.run(ctx)
	}()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := pub.shutdown(shutdownCtx); err != nil {
			t.Errorf("Failed to shutdown bootstrap list publisher: %v", err)
		}
		cancel()
		<-done
	}()

	waitForBootstrapListStateInFile(t, filePath, wantInitial)
	waitForBootstrapListStateOverHTTP(t, url, wantInitial)

	// Simulate whitelist update by adding peer 2.
	whitelist[hosts[2].ID()] = struct{}{}
	pub.publish()

	waitForBootstrapListStateInFile(t, filePath, wantUpdated)
	waitForBootstrapListStateOverHTTP(t, url, wantUpdated)
}

// TestBootstrapListPublishSerialized verifies that concurrent publish calls
// do not overlap file writes.
func TestBootstrapListPublishSerialized(t *testing.T) {
	fx := newBootstrapPublisherFixture(t, "/ip4/1.2.3.4/tcp/1234")

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	fx.pub.writeFile = func(string, []byte) error {
		n := inFlight.Add(1)
		for {
			maxSeen := maxInFlight.Load()
			if n <= maxSeen || maxInFlight.CompareAndSwap(maxSeen, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fx.pub.publish()
		}()
	}
	wg.Wait()

	if got := maxInFlight.Load(); got != 1 {
		t.Fatalf("publish calls overlapped; max concurrent writes = %d", got)
	}

	entries := cachedBootstrapEntries(t, fx.pub)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries in cached bootstrap list, got %d", len(entries))
	}
}

// TestBootstrapListFileWriteBehavior verifies that unchanged content is skipped after success and retried after a write failure.
func TestBootstrapListFileWriteBehavior(t *testing.T) {
	type publishExpectation struct {
		wantWrites int32
		wantDirty  bool
	}

	tests := []struct {
		name         string
		writeResults []error
		expectations []publishExpectation
	}{
		{
			name:         "skip unchanged after success",
			writeResults: []error{nil},
			expectations: []publishExpectation{
				{wantWrites: 1, wantDirty: false},
				{wantWrites: 1, wantDirty: false},
			},
		},
		{
			name:         "retry unchanged after failure",
			writeResults: []error{errors.New("boom"), nil},
			expectations: []publishExpectation{
				{wantWrites: 1, wantDirty: true},
				{wantWrites: 2, wantDirty: false},
				{wantWrites: 2, wantDirty: false},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fx := newBootstrapPublisherFixture(t, "/ip4/1.2.3.4/tcp/1234")

			var writes atomic.Int32
			fx.pub.writeFile = func(string, []byte) error {
				attempt := writes.Add(1)
				if int(attempt) <= len(test.writeResults) {
					return test.writeResults[int(attempt-1)]
				}
				return nil
			}

			for i, expectation := range test.expectations {
				fx.pub.publish()
				if got := writes.Load(); got != expectation.wantWrites {
					t.Fatalf("publish %d: expected %d file writes, got %d", i+1, expectation.wantWrites, got)
				}
				if fx.pub.fileDirty != expectation.wantDirty {
					t.Fatalf("publish %d: expected fileDirty=%t, got %t", i+1, expectation.wantDirty, fx.pub.fileDirty)
				}
			}
		})
	}
}

func waitForBootstrapListStateInFile(t *testing.T, filePath string, want map[string][]string) {
	t.Helper()

	wantState := normalizeBootstrapListWant(want)
	lastState := "no file read yet"

	requireEventually(t, func() bool {
		data, err := os.ReadFile(filePath)
		if err != nil {
			lastState = fmt.Sprintf("read failed: %v", err)
			return false
		}

		entries, err := decodeBootstrapListEntries(data)
		if err != nil {
			lastState = fmt.Sprintf("unmarshal failed: %v", err)
			return false
		}

		gotState := normalizeBootstrapListEntries(entries)
		lastState = fmt.Sprintf("got %v", gotState)
		return reflect.DeepEqual(gotState, wantState)
	}, 5*time.Second, 10*time.Millisecond, "bootstrap list file %q did not match %v (%v)", filePath, wantState, lazyString(func() string {
		return lastState
	}))
}

func waitForBootstrapListStateOverHTTP(t *testing.T, url string, want map[string][]string) {
	t.Helper()

	wantState := normalizeBootstrapListWant(want)
	lastState := "no HTTP response yet"

	requireEventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			lastState = fmt.Sprintf("request failed: %v", err)
			return false
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastState = fmt.Sprintf("read failed: %v", err)
			return false
		}
		if resp.StatusCode != http.StatusOK {
			lastState = fmt.Sprintf("got status %d", resp.StatusCode)
			return false
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			lastState = fmt.Sprintf("got Content-Type %q", ct)
			return false
		}

		entries, err := decodeBootstrapListEntries(body)
		if err != nil {
			lastState = fmt.Sprintf("unmarshal failed: %v", err)
			return false
		}

		gotState := normalizeBootstrapListEntries(entries)
		lastState = fmt.Sprintf("got %v", gotState)
		return reflect.DeepEqual(gotState, wantState)
	}, 5*time.Second, 10*time.Millisecond, "bootstrap HTTP %q did not match %v (%v)", url, wantState, lazyString(func() string {
		return lastState
	}))
}

func decodeBootstrapListEntries(data []byte) ([]bootstrapPeerEntry, error) {
	var entries []bootstrapPeerEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func normalizeBootstrapListEntries(entries []bootstrapPeerEntry) map[string][]string {
	got := make(map[string][]string, len(entries))
	for _, entry := range entries {
		got[entry.PeerID] = normalizeAddrStrings(entry.Addrs)
	}
	return got
}

func normalizeBootstrapListWant(want map[string][]string) map[string][]string {
	normalized := make(map[string][]string, len(want))
	for peerID, addrs := range want {
		normalized[peerID] = normalizeAddrStrings(addrs)
	}
	return normalized
}

func normalizeAddrStrings(addrs []string) []string {
	copied := append([]string(nil), addrs...)
	sort.Strings(copied)
	if len(copied) == 0 {
		return []string{}
	}
	return copied
}

func cachedBootstrapEntries(t *testing.T, pub *bootstrapListPublisher) []bootstrapPeerEntry {
	t.Helper()

	pub.cacheMtx.RLock()
	data := append([]byte(nil), pub.cache...)
	pub.cacheMtx.RUnlock()

	entries, err := decodeBootstrapListEntries(data)
	if err != nil {
		t.Fatalf("Failed to decode cached bootstrap list: %v", err)
	}

	return entries
}

type bootstrapPublisherFixture struct {
	pub         *bootstrapListPublisher
	remoteID    peer.ID
	remoteAddrs []ma.Multiaddr
}

func newBootstrapPublisherFixture(t *testing.T, remoteAddrStrs ...string) *bootstrapPublisherFixture {
	t.Helper()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatal(err)
	}

	hosts := mnet.Hosts()
	h := hosts[0]

	addrs := make([]ma.Multiaddr, 0, len(remoteAddrStrs))
	for _, addrStr := range remoteAddrStrs {
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			t.Fatalf("Failed to create multiaddr %q: %v", addrStr, err)
		}
		addrs = append(addrs, addr)
	}
	h.Peerstore().AddAddrs(hosts[1].ID(), addrs, 1<<62)

	pub := newBootstrapListPublisher(newTestLogger(), h, func() map[peer.ID]struct{} {
		return map[peer.ID]struct{}{
			h.ID():        {},
			hosts[1].ID(): {},
		}
	}, filepath.Join(t.TempDir(), "bootstrap.json"), 0, true)

	return &bootstrapPublisherFixture{
		pub:         pub,
		remoteID:    hosts[1].ID(),
		remoteAddrs: addrs,
	}
}

func reserveHTTPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to reserve HTTP port: %v", err)
	}
	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port
}

type blockingConnectHost struct {
	host.Host
	connectStarted chan struct{}
	notifyOnce     sync.Once
}

func (h *blockingConnectHost) Connect(ctx context.Context, _ peer.AddrInfo) error {
	h.notifyOnce.Do(func() {
		close(h.connectStarted)
	})
	<-ctx.Done()
	return ctx.Err()
}
