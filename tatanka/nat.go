package tatanka

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/decred/slog"
	igd "github.com/emersion/go-upnp-igd"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// natMappingLifetime is the lifetime requested for UPnP port mappings.
	natMappingLifetime = 1 * time.Hour

	// natRenewalInterval is how often we renew the mapping (50% of lifetime
	// per RFC 6886 recommendation).
	natRenewalInterval = 30 * time.Minute

	// natDiscoveryTimeout is the timeout for UPnP IGD discovery.
	natDiscoveryTimeout = 10 * time.Second
)

// natMapper manages a UPnP IGD port mapping where the requested external port
// equals the internal listen port, giving a stable public address across
// restarts.
type natMapper struct {
	log        slog.Logger
	listenPort int
	device     *igd.IGD
	externalIP net.IP
	mappedPort int
	closed     bool
	mu         sync.RWMutex
}

// newNATMapper discovers a UPnP IGD device, retrieves the external IP, and
// requests a port mapping with the external port equal to the listen port.
func newNATMapper(log slog.Logger, listenPort int) (*natMapper, error) {
	// Run discovery in a goroutine so we can take the first device as soon
	// as it responds instead of waiting for the full timeout. Buffer the
	// channel generously so Discover doesn't block on send after we stop
	// reading.
	ch := make(chan igd.Device, 16)
	go igd.Discover(ch, natDiscoveryTimeout)

	var device *igd.IGD
	for d := range ch {
		if igdDev, ok := d.(*igd.IGD); ok {
			device = igdDev
			break
		}
	}
	if device == nil {
		return nil, fmt.Errorf("no UPnP IGD device found")
	}

	externalIP, err := device.GetExternalIPAddress()
	if err != nil {
		return nil, fmt.Errorf("UPnP GetExternalIPAddress failed: %w", err)
	}

	mappedPort, err := device.AddPortMapping(igd.TCP, listenPort, listenPort, "tatanka", natMappingLifetime)
	if err != nil {
		return nil, fmt.Errorf("UPnP AddPortMapping failed: %w", err)
	}

	if mappedPort != listenPort {
		log.Warnf("UPnP: requested external port %d but router assigned %d; address will not be deterministic",
			listenPort, mappedPort)
	}
	log.Infof("UPnP: mapped %s:%d -> :%d (lifetime %s)",
		externalIP, mappedPort, listenPort, natMappingLifetime)

	return &natMapper{
		log:        log,
		listenPort: listenPort,
		device:     device,
		externalIP: externalIP,
		mappedPort: mappedPort,
	}, nil
}

// publicAddr returns the NAT-mapped public multiaddr.
func (n *natMapper) publicAddr() ma.Multiaddr {
	n.mu.RLock()
	defer n.mu.RUnlock()

	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", n.externalIP, n.mappedPort))
	if err != nil {
		n.log.Errorf("UPnP: failed to build multiaddr: %v", err)
		return nil
	}
	return addr
}

// run periodically renews the UPnP port mapping and refreshes the external IP.
// It blocks until the context is cancelled.
func (n *natMapper) run(ctx context.Context) {
	ticker := time.NewTicker(natRenewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.renew()
		}
	}
}

// renew refreshes the external IP and renews the port mapping.
//
// The primary lifecycle guarantee is that serve() waits for all goroutines
// (including run()) via wg.Wait() before shutdown() calls close(). The closed
// flag is a secondary guard: we check it before and after the network I/O to
// avoid state updates if close() ran concurrently.
func (n *natMapper) renew() {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return
	}
	// Use the previously assigned external port as the suggested external port
	// when renewing, not the originally requested one.
	suggestedExtPort := n.mappedPort
	n.mu.RUnlock()

	newIP, err := n.device.GetExternalIPAddress()
	if err != nil {
		n.log.Warnf("UPnP: failed to refresh external IP: %v", err)
		// Still try to renew the mapping with the old IP.
	}

	newPort, err := n.device.AddPortMapping(igd.TCP, n.listenPort, suggestedExtPort, "tatanka", natMappingLifetime)
	if err != nil {
		n.log.Warnf("UPnP: failed to renew port mapping: %v", err)
		return
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	if newIP != nil && !newIP.Equal(n.externalIP) {
		n.log.Infof("UPnP: external IP changed to %s", newIP)
		n.externalIP = newIP
	}
	if newPort != n.mappedPort {
		n.log.Infof("UPnP: mapped port changed to %d", newPort)
		n.mappedPort = newPort
	}
	n.mu.Unlock()

	n.log.Debugf("UPnP: renewed mapping %s:%d -> :%d",
		n.externalIP, newPort, n.listenPort)
}

// close deletes the UPnP port mapping.
func (n *natMapper) close(timeout time.Duration) {
	n.mu.Lock()
	n.closed = true
	port := n.mappedPort
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := n.device.DeletePortMapping(igd.TCP, port); err != nil {
			// Many routers expose multiple IGD services and the library
			// iterates all of them; services without the mapping return a
			// SOAP fault (HTTP 500). This is harmless — the mapping will
			// expire naturally at the end of its lease.
			n.log.Debugf("UPnP: DeletePortMapping returned error (mapping will expire naturally): %v", err)
			return
		}
		n.log.Infof("UPnP: deleted port mapping for port %d", port)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		n.log.Warnf("UPnP: timed out deleting port mapping; mapping will expire naturally")
	}
}
