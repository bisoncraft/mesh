package client

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/martonp/tatanka-mesh/bond"
	tmc "github.com/martonp/tatanka-mesh/client"
)

var (
	defaultTimeout = time.Second * 10

	DefaultWebPort    = 12465
	DefaultClientPort = 12455

	//go:embed index.html
	index embed.FS
)

// Config represents the test client configuration.
type Config struct {
	NodeAddr   string
	PrivateKey crypto.PrivKey
	ClientPort int
	WebPort    int
	Logger     slog.Logger
	LogStream  chan string
}

// Client represents a tatanka test client.
type Client struct {
	cfg           *Config
	https         *http.Server
	tatankaClient *tmc.Client
	log           slog.Logger

	logSubsMtx sync.RWMutex
	logSubs    map[string]chan string
}

func (c *Client) route() {
	r := chi.NewRouter()
	r.Use(cors.AllowAll().Handler)

	r.Post("/broadcast", c.broadcast)
	r.Post("/subscribe", c.subscribe)
	r.Post("/unsubscribe", c.unsubscribe)
	r.Post("/bond/add", c.addBond)
	r.Get("/bond/post", c.postBond)
	r.Get("/logs", c.streamLogs)

	r.Handle("/", http.FileServer(http.FS(index)))

	c.https = &http.Server{
		Addr:    fmt.Sprintf(":%d", c.cfg.WebPort),
		Handler: r,
	}
}

func NewClient(cfg *Config) (*Client, error) {
	c := &Client{
		cfg:     cfg,
		log:     cfg.Logger,
		logSubs: make(map[string]chan string),
	}

	tcCfg := &tmc.Config{
		Port:           cfg.ClientPort,
		PrivateKey:     cfg.PrivateKey,
		RemotePeerAddr: cfg.NodeAddr,
		Logger:         cfg.Logger,
	}

	tc, err := tmc.NewClient(tcCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tatanka client: %w", err)
	}

	c.tatankaClient = tc

	c.route()

	return c, nil
}

func (c *Client) handleSubscribe(evt tmc.TopicEvent) {
	switch evt.Type {
	case tmc.TopicEventData:
		c.log.Infof("Received event data for peer %s with data: %s", evt.Peer.String(), hex.Dump(evt.Data))
	case tmc.TopicEventPeerSubscribed:
		c.log.Infof("Received subscription event for peer %s", evt.Peer.String())
	default:
		c.log.Errorf("Unexpected event type received: %T", evt.Type)
	}
}

func (c *Client) broadcast(w http.ResponseWriter, r *http.Request) {
	var payload broadcastPayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read broadcast request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read broadcast request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	data, err := base64.StdEncoding.DecodeString(payload.Data)
	if err != nil {
		c.log.Error("Failed to decode payload data: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to decode payload data")
		return
	}

	err = c.tatankaClient.Broadcast(ctx, payload.Topic, data)
	if err != nil {
		c.log.Errorf("Failed to broadcast message: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to broadcast message")
		return
	}

	c.log.Infof("Broadcasted message on topic %s", payload.Topic)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) subscribe(w http.ResponseWriter, r *http.Request) {
	var payload subscribePayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read subscribe request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read subscribe request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err = c.tatankaClient.Subscribe(ctx, payload.Topic, c.handleSubscribe)
	if err != nil {
		c.log.Errorf("Failed to subscribe to topic %s: %v", payload.Topic, err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to subscribe to topic")
		return
	}

	c.log.Infof("Subscribed to topic %s", payload.Topic)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) unsubscribe(w http.ResponseWriter, r *http.Request) {
	var payload subscribePayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read unsubscribe request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read unsubscribe request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err = c.tatankaClient.Unsubscribe(ctx, payload.Topic)
	if err != nil {
		c.log.Errorf("Failed to unsubscribe from topic %s: %v", payload.Topic, err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to unsubscribe from topic")
		return
	}

	c.log.Infof("Unsubscribed from topic %s", payload.Topic)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) addBond(w http.ResponseWriter, r *http.Request) {
	var payload bondPayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read bond request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read add bond request")
		return
	}

	params := []*bond.BondParams{{
		ID:       payload.ID,
		Expiry:   time.Unix(payload.Expiry, 0),
		Strength: payload.Strength,
	}}

	c.tatankaClient.AddBond(params)

	c.log.Infof("Added bond %s with strength %d", payload.ID, payload.Strength)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) postBond(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err := c.tatankaClient.PostBond(ctx)
	if err != nil {
		c.log.Errorf("Failed to post bond: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to post bond")
		return
	}

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) streamLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")

	sub := make(chan string, 10)

	c.logSubsMtx.RLock()
	c.logSubs[r.RemoteAddr] = sub
	c.logSubsMtx.RUnlock()

	rc := http.NewResponseController(w)
	_, err := fmt.Fprint(w, "data: connected\n\n")
	if err != nil {
		c.log.Errorf("Failed to write to stream: %v", err)
		return
	}
	rc.Flush()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case logLine, ok := <-sub:
			if !ok {
				return
			}

			if _, err := fmt.Fprintf(w, "data: %s\n\n", logLine); err != nil {
				c.log.Errorf("Failed to write to stream: %v", err)
				return
			}
			rc.Flush()

		case <-ticker.C:
			if _, err := fmt.Fprint(w, "data: ping\n\n"); err != nil {
				c.log.Errorf("Failed to write to stream: %v", err)
				return
			}
			rc.Flush()

		case <-r.Context().Done():
			c.logSubsMtx.Lock()
			delete(c.logSubs, r.RemoteAddr)
			c.logSubsMtx.Unlock()

			return
		}
	}
}

func (c *Client) Run(ctx context.Context, bonds []*bond.BondParams) {
	c.log.Infof("Running test client ...")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				for _, sub := range c.logSubs {
					close(sub)
				}

				return
			case msg := <-c.cfg.LogStream:
				for _, sub := range c.logSubs {
					sub <- msg
				}
			}
		}
	}()

	go func() {
		c.log.Infof("Tatanka client listening on: %d", c.cfg.ClientPort)
		err := c.tatankaClient.Run(ctx, bonds)
		if err != nil {
			c.log.Errorf("Failed to run tatanka client: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_ = c.https.Shutdown(ctx)

		wg.Done()
	}()

	go func() {
		switch c.cfg.ClientPort {
		case DefaultClientPort:
			c.log.Infof("Web interface available on: http://localhost:%d", c.cfg.WebPort)
		default:
			c.log.Infof("Web interface available on: http://localhost:%d?clientport=%d", c.cfg.WebPort, c.cfg.ClientPort)
		}

		if err := c.https.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				c.log.Errorf("Failed to start server: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}
