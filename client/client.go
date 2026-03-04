package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bisoncraft/mesh/bond"
	clientpb "github.com/bisoncraft/mesh/client/pb"
	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/protocols"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = 7565
)

var (
	// ErrRedundantSubscription indicates a redundant subscription has been made.
	ErrRedundantSubscription = errors.New("redundant subscription")
	// ErrRedundantUnsubscription indicates an unsubscription from a topic that is not subscribed.
	ErrRedundantUnsubscription = errors.New("redundant unsubscription")
	// errEmptyTopic indicates an empty topic name was provided.
	errEmptyTopic = errors.New("topic name cannot be empty")
	// errNoMeshConnection indicates that there is no mesh connection established.
	errNoMeshConnection = errors.New("no mesh connection established")

	errPeerNoEncryptionKey = errors.New("peer reports no encryption key")
)

// Config represents a tatanka client configuration.
type Config struct {
	Port            int
	PrivateKey      crypto.PrivKey
	RemotePeerAddrs []string
	Logger          slog.Logger
}

// Client represents a tatanka client.
type Client struct {
	cfg               *Config
	host              host.Host
	topicRegistry     *topicRegistry
	bondInfo          *bond.BondInfo
	log               slog.Logger
	connFactory       meshConnFactory
	connManager       *meshConnectionManager
	encryptionManager encryptionManager
}

// PeerID returns the peer ID of the client. If the client has not yet been
// initialized, an empty string is returned.
func (c *Client) PeerID() peer.ID {
	if c.host == nil {
		return ""
	}
	return c.host.ID()
}

// ConnectedTatankaNodePeerID returns the peer ID of the currently connected
// tatanka node. If no tatanka node connection is active, an empty string
// is returned.
func (c *Client) ConnectedTatankaNodePeerID() string {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return ""
	}
	return mc.remotePeerID().String()
}

// NewClient initializes a new tatanka client.
func NewClient(cfg *Config) (*Client, error) {
	c := &Client{
		cfg:           cfg,
		topicRegistry: newTopicRegistry(),
		bondInfo:      bond.NewBondInfo(),
		log:           cfg.Logger,
	}

	ourPeerID, err := peer.IDFromPrivateKey(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get our peer ID: %w", err)
	}

	if cfg.PrivateKey.Type() != crypto.Ed25519 {
		return nil, fmt.Errorf("expected ed25519 private key, but got %s", cfg.PrivateKey.Type().String())
	}

	c.encryptionManager = newEncryptionManager(ourPeerID, c.sendHandshakeRequest)

	// Add a placeholder bond to validate client. Remove once bond pipeline is fully implemented.
	c.bondInfo.AddBonds([]*bond.BondParams{{
		ID:       "placeholder",
		Expiry:   time.Now().Add(time.Hour * 6),
		Strength: bond.MinRequiredBondStrength}},
		time.Now())

	if c.cfg.Port == 0 {
		c.cfg.Port = defaultPort
	}

	return c, nil
}

func (c *Client) primaryMeshConnection() (meshConn, error) {
	if c.connManager == nil {
		return nil, errNoMeshConnection
	}
	return c.connManager.primaryConnection()
}

// signedHandshakeRequest returns a signed handshake request for the given
// key ID and encryption public key.
func (c *Client) signedHandshakeRequest(id keyID, encryptionPubKey []byte) ([]byte, error) {
	hsReq := &clientpb.HandshakeRequest{
		Nonce:     id[:],
		PublicKey: encryptionPubKey,
	}
	reqPayload, err := proto.Marshal(hsReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal handshake request: %w", err)
	}
	signature, err := c.cfg.PrivateKey.Sign(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to sign handshake request: %w", err)
	}
	hsReq.Signature = signature

	return proto.Marshal(pbClientRequestHandshake(hsReq))
}

// verifyHandshakeResponse verifies that the signature in the handshake response
// was signed by the given peer.
func verifyHandshakeResponse(peerID peer.ID, hsResp *clientpb.HandshakeResponse) error {
	peerPub, err := peerID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("failed to extract public key: %w", err)
	}
	unsignedReq := proto.Clone(hsResp).(*clientpb.HandshakeResponse)
	unsignedReq.Signature = nil
	unsignedReqPayload, err := proto.Marshal(unsignedReq)
	if err != nil {
		return fmt.Errorf("failed to marshal unsigned handshake request: %w", err)
	}
	valid, err := peerPub.Verify(unsignedReqPayload, hsResp.GetSignature())
	if err != nil {
		return fmt.Errorf("failed to verify handshake response signature: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid handshake response signature")
	}
	return nil
}

// sendHandshakeRequest sends a handshake request to the given peer, waits for the
// response, verifies the signature, and returns the public key.
func (c *Client) sendHandshakeRequest(ctx context.Context, peerID peer.ID, id keyID, encryptionPubKey []byte) ([]byte, error) {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to send handshake request: %w", err)
	}

	reqPayload, err := c.signedHandshakeRequest(id, encryptionPubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign handshake request: %w", err)
	}

	respBytes, err := mc.relayMessage(ctx, peerID, reqPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to relay message: %w", err)
	}

	envResp := &clientpb.ClientResponse{}
	if err := proto.Unmarshal(respBytes, envResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal client response: %w", err)
	}

	hsResp := envResp.GetHandshakeResponse()
	if hsResp == nil {
		return nil, fmt.Errorf("unexpected response type")
	}

	if hsResp.GetError() != nil {
		return nil, fmt.Errorf("handshake response error: %s", hsResp.GetError().GetMessage())
	}

	if !bytes.Equal(hsResp.GetNonce(), id[:]) {
		return nil, fmt.Errorf("handshake response nonce mismatch")
	}

	err = verifyHandshakeResponse(peerID, hsResp)
	if err != nil {
		return nil, fmt.Errorf("failed to verify handshake response: %w", err)
	}

	return hsResp.GetPublicKey(), nil
}

// sendEncryptedMessage encrypts the given message and sends it to the given peer.
// If the peer does not have the encryption key we used to encrypt the message,
// errPeerNoEncryptionKey is returned allowing the caller to clear the encryption
// key and retry with a new handshake.
func (c *Client) sendEncryptedMessage(ctx context.Context, mc meshConn, peerID peer.ID, message []byte) ([]byte, keyID, error) {
	encrypted, id, err := c.encryptionManager.encryptPeerMessage(ctx, peerID, message, keyID{})
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to encrypt peer message: %w", err)
	}

	reqPayload, err := proto.Marshal(pbClientRequestMessage(encrypted))
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to marshal message request: %w", err)
	}

	respBytes, err := mc.relayMessage(ctx, peerID, reqPayload)
	if err != nil {
		return nil, keyID{}, fmt.Errorf("failed to relay message: %w", err)
	}

	envResp := &clientpb.ClientResponse{}
	if err := proto.Unmarshal(respBytes, envResp); err != nil {
		return nil, keyID{}, fmt.Errorf("failed to unmarshal client response: %w", err)
	}

	msgResp := envResp.GetMessageResponse()
	if msgResp == nil {
		return nil, keyID{}, fmt.Errorf("unexpected response type")
	}

	switch r := msgResp.Response.(type) {
	case *clientpb.MessageResponse_Message:
		plaintext, _, err := c.encryptionManager.decryptPeerMessage(peerID, r.Message)
		if err != nil {
			return nil, keyID{}, fmt.Errorf("failed to decrypt peer message: %w", err)
		}
		return plaintext, id, nil
	case *clientpb.MessageResponse_Error:
		if r.Error.GetNoEncryptionKeyError() != nil {
			return nil, id, errPeerNoEncryptionKey
		}
		if msg := r.Error.GetMessage(); msg != "" {
			return nil, keyID{}, fmt.Errorf("peer error: %s", msg)
		}
		return nil, keyID{}, fmt.Errorf("peer returned unknown error")

	default:
		return nil, keyID{}, fmt.Errorf("unexpected message response type")
	}
}

// MessagePeer sends a relay message to the given peer through the mesh and returns the response payload.
func (c *Client) MessagePeer(ctx context.Context, peerID peer.ID, message []byte) ([]byte, error) {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to send peer message: %w", err)
	}

	respPayload, id, err := c.sendEncryptedMessage(ctx, mc, peerID, message)
	if err == nil {
		return respPayload, nil
	}

	// If the peer does not have the encryption key we used to encrypt the message,
	// force a new handshake and retry.
	if errors.Is(err, errPeerNoEncryptionKey) {
		c.encryptionManager.clearEncryptionKey(peerID, id)
		respPayload, _, err = c.sendEncryptedMessage(ctx, mc, peerID, message)
		if err != nil {
			return nil, fmt.Errorf("failed to send peer message after clearing encryption key: %w", err)
		}

		return respPayload, nil
	}

	return nil, err
}

func verifyHandshakeRequest(initiatorID peer.ID, hsReq *clientpb.HandshakeRequest) error {
	if len(hsReq.GetPublicKey()) != 32 || len(hsReq.GetNonce()) != keyIDSize {
		return fmt.Errorf("invalid handshake request")
	}

	peerPub, err := initiatorID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("failed to extract public key: %w", err)
	}

	unsignedReq := proto.Clone(hsReq).(*clientpb.HandshakeRequest)
	unsignedReq.Signature = nil
	unsignedReqPayload, err := proto.Marshal(unsignedReq)
	if err != nil {
		return fmt.Errorf("failed to marshal unsigned handshake request: %w", err)
	}

	valid, err := peerPub.Verify(unsignedReqPayload, hsReq.GetSignature())
	if err != nil {
		return fmt.Errorf("failed to verify handshake request signature: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid handshake request signature")
	}

	return nil
}

func (c *Client) signedHandshakeResponse(nonce, ourPubKey []byte, err error) ([]byte, error) {
	if err != nil {
		// Error responses do not need to be signed.
		b, mErr := proto.Marshal(pbClientResponseHandshake(pbHandshakeResponseError(err.Error(), nonce)))
		if mErr != nil {
			return nil, fmt.Errorf("failed to marshal handshake error response: %w", mErr)
		}
		return b, nil
	}

	hsResponse := pbHandshakeResponseSuccess(ourPubKey, nonce)
	unsignedResponsePayload, mErr := proto.Marshal(hsResponse)
	if mErr != nil {
		return nil, fmt.Errorf("failed to marshal unsigned handshake response: %w", mErr)
	}
	signature, err := c.cfg.PrivateKey.Sign(unsignedResponsePayload)
	if err != nil {
		return nil, fmt.Errorf("failed to sign handshake response: %w", err)
	}

	hsResponse.Signature = signature

	b, mErr := proto.Marshal(pbClientResponseHandshake(hsResponse))
	if mErr != nil {
		return nil, fmt.Errorf("failed to marshal handshake response: %w", mErr)
	}
	return b, nil
}

// HandlePeerMessage registers a handler for incoming TatankaRelayMessage requests.
func (c *Client) HandlePeerMessage(handler func([]byte) ([]byte, error)) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	if c.host == nil {
		return fmt.Errorf("client host not initialized")
	}

	c.host.SetStreamHandler(protocols.TatankaRelayMessageProtocol, func(s network.Stream) {
		defer func() { _ = s.Close() }()

		req := &protocolsPb.TatankaRelayMessageRequest{}
		if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
			c.log.Debugf("failed to read/unmarshal relay message request: %v", err)
			return
		}

		initiatorID, err := peer.IDFromBytes(req.GetPeerID())
		if err != nil {
			return
		}

		sendResponse := func(resp proto.Message) {
			b, err := proto.Marshal(resp)
			if err != nil {
				c.log.Debugf("failed to marshal client response: %v", err)
				return
			}
			_ = codec.WriteLengthPrefixedMessage(s, pbPeerMessageSuccess(b))
		}

		clientReq := &clientpb.ClientRequest{}
		if err := proto.Unmarshal(req.GetMessage(), clientReq); err != nil {
			sendResponse(pbClientResponseMessage(pbMessageResponseError("failed to unmarshal client request")))
			return
		}

		switch r := clientReq.Request.(type) {
		case *clientpb.ClientRequest_HandshakeRequest:
			handshakeReq := r.HandshakeRequest
			if handshakeReq == nil {
				sendResponse(pbClientResponseHandshake(pbHandshakeResponseError("missing handshake request", nil)))
				return
			}

			if err := verifyHandshakeRequest(initiatorID, handshakeReq); err != nil {
				sendResponse(pbClientResponseHandshake(pbHandshakeResponseError(err.Error(), handshakeReq.GetNonce())))
				return
			}

			ourPubKey, hsErr := c.encryptionManager.handleHandshakeRequest(initiatorID, handshakeReq.GetNonce(), handshakeReq.GetPublicKey())
			respPayload, err := c.signedHandshakeResponse(handshakeReq.GetNonce(), ourPubKey, hsErr)
			if err != nil {
				sendResponse(pbClientResponseHandshake(pbHandshakeResponseError(err.Error(), handshakeReq.GetNonce())))
				return
			}

			_ = codec.WriteLengthPrefixedMessage(s, pbPeerMessageSuccess(respPayload))

		case *clientpb.ClientRequest_MessageRequest:
			msgReq := r.MessageRequest
			if msgReq == nil {
				sendResponse(pbClientResponseMessage(pbMessageResponseError("missing message request")))
				return
			}

			plaintext, nonce, err := c.encryptionManager.decryptPeerMessage(initiatorID, msgReq.GetMessage())
			if err != nil {
				if errors.Is(err, ErrUnknownKeyID) {
					sendResponse(pbClientResponseMessage(pbMessageResponseNoEncryptionKey()))
				} else {
					sendResponse(pbClientResponseMessage(pbMessageResponseError("failed to decrypt message")))
				}
				return
			}

			respPayload, err := handler(plaintext)
			if err != nil {
				sendResponse(pbClientResponseMessage(pbMessageResponseError(err.Error())))
				return
			}

			encryptedResp, _, encErr := c.encryptionManager.encryptPeerMessage(context.Background(), initiatorID, respPayload, nonce)
			if encErr != nil {
				sendResponse(pbClientResponseMessage(pbMessageResponseError("failed to encrypt response")))
				return
			}

			sendResponse(pbClientResponseMessage(pbMessageResponseSuccess(encryptedResp)))

		default:
			sendResponse(pbClientResponseMessage(pbMessageResponseError("unknown client request type")))
		}
	})

	return nil
}

// Broadcast publishes the provided message bytes on a mesh topic.
func (c *Client) Broadcast(ctx context.Context, topic string, data []byte) error {
	if topic == "" {
		return errEmptyTopic
	}

	if data == nil {
		return errors.New("data cannot be nil")
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return fmt.Errorf("failed to broadcast message: %w", err)
	}

	return mc.broadcast(ctx, topic, data)
}

// Topics returns the list of topics the client is currently subscribed to.
func (c *Client) Topics() []string {
	return c.topicRegistry.fetchTopics()
}

// Subscribe subscribes the client to the provided topic.
func (c *Client) Subscribe(ctx context.Context, topic string, handlerFunc TopicHandler) error {
	if topic == "" {
		return errEmptyTopic
	}

	if handlerFunc == nil {
		return errors.New("handler function cannot be nil")
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	if !c.topicRegistry.register(topic, handlerFunc) {
		return ErrRedundantSubscription
	}

	err = mc.subscribe(ctx, topic)
	if err != nil {
		// Unregister the topic if subscription fails.
		if !c.topicRegistry.unregister(topic) {
			c.log.Warnf("Failed to unregister topic %s", topic)
		}

		return err
	}

	return nil
}

// Unsubscribe unsubscribes the client from the provided topic.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	if topic == "" {
		return errEmptyTopic
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	// Fetch the handler for the topic before unregistering.
	topicHandler, err := c.topicRegistry.fetchHandler(topic)
	if err != nil {
		return ErrRedundantUnsubscription
	}

	if !c.topicRegistry.unregister(topic) {
		return ErrRedundantUnsubscription
	}

	err = mc.unsubscribe(ctx, topic)
	if err != nil {
		// Re-register the topic if unsubscription fails.
		if !c.topicRegistry.register(topic, topicHandler) {
			c.log.Warnf("Failed to re-register topic %s", topic)
		}

		return err
	}

	return nil
}

// handlePushMessage processes incoming pushed messages. The messages are processed based on their topics.
func (c *Client) handlePushMessage(msg *protocolsPb.PushMessage) {
	handlerFunc, err := c.topicRegistry.fetchHandler(msg.Topic)
	if err != nil {
		c.log.Warnf("Failed to fetch handler for topic %s: %v", msg.Topic, err)
		return
	}

	event := TopicEvent{
		Peer: peer.ID(msg.GetSender()),
		Type: TopicEventData,
	}

	switch msg.GetMessageType() {
	case protocolsPb.PushMessage_BROADCAST:
		event.Type = TopicEventData
		event.Data = msg.GetData()
	case protocolsPb.PushMessage_SUBSCRIBE:
		event.Type = TopicEventPeerSubscribed
	case protocolsPb.PushMessage_UNSUBSCRIBE:
		event.Type = TopicEventPeerUnsubscribed
	default:
		c.log.Warnf("Received push message with unknown type %v on topic %s", msg.GetMessageType(), msg.Topic)
		return
	}

	handlerFunc(event)
}

// PostBond posts the client's bond.
func (c *Client) PostBond(ctx context.Context) error {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}
	return mc.postBond(ctx)
}

// AddBond adds the provided bond parameters to the client.
func (c *Client) AddBond(params []*bond.BondParams) {
	c.bondInfo.AddBonds(params, time.Now())
}

// parseBootstrapAddrs parses a list of multiaddr strings into peer.AddrInfo.
// Multiple addresses for the same peer ID are combined into a single AddrInfo.
func parseBootstrapAddrs(addrs []string) ([]peer.AddrInfo, error) {
	peerMap := make(map[peer.ID]*peer.AddrInfo)

	for _, addrStr := range addrs {
		maddr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse address %q: %w", addrStr, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse peer info from %q: %w", addrStr, err)
		}

		if existing, ok := peerMap[info.ID]; ok {
			existing.Addrs = append(existing.Addrs, info.Addrs...)
		} else {
			peerMap[info.ID] = info
		}
	}

	result := make([]peer.AddrInfo, 0, len(peerMap))
	for _, info := range peerMap {
		result = append(result, *info)
	}
	return result, nil
}

// Run starts the mesh client.
func (c *Client) Run(ctx context.Context, bonds []*bond.BondParams) error {
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", defaultHost, c.cfg.Port)

	if c.cfg.PrivateKey == nil {
		return fmt.Errorf("no private key provided for client")
	}

	var err error
	c.host, err = libp2p.New(libp2p.ListenAddrStrings(listenAddr), libp2p.Identity(c.cfg.PrivateKey))
	if err != nil {
		return fmt.Errorf("failed to create host: %w", err)
	}
	defer func() { _ = c.host.Close() }()

	bootstrapPeers, err := parseBootstrapAddrs(c.cfg.RemotePeerAddrs)
	if err != nil {
		return err
	}

	// Set default connection factory if not already set (tests may override).
	if c.connFactory == nil {
		c.connFactory = func(peerID peer.ID) meshConn {
			return newMeshConnection(c.host, peerID, c.log, c.bondInfo, c.topicRegistry.fetchTopics, c.handlePushMessage)
		}
	}

	// Create the connection manager.
	c.connManager = newMeshConnectionManager(&meshConnectionManagerConfig{
		host:           c.host,
		log:            c.log,
		connFactory:    c.connFactory,
		bootstrapPeers: bootstrapPeers,
	})

	c.bondInfo.AddBonds(bonds, time.Now())

	go c.encryptionManager.run(ctx)

	c.connManager.run(ctx)

	return nil
}
