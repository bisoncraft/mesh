package tatanka

import (
	"bufio"
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/codec"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
)

// libp2pPeerInfoToPb converts a peer.AddrInfo to a protocolsPb.PeerInfo.
func libp2pPeerInfoToPb(peerInfo peer.AddrInfo) *protocolsPb.PeerInfo {
	addrBytes := make([][]byte, len(peerInfo.Addrs))
	for i, addr := range peerInfo.Addrs {
		addrBytes[i] = addr.Bytes()
	}

	return &protocolsPb.PeerInfo{
		Id:    []byte(peerInfo.ID),
		Addrs: addrBytes,
	}
}

// pbPeerInfoToLibp2p converts a protocolsPb.PeerInfo to a peer.AddrInfo.
func pbPeerInfoToLibp2p(pbPeer *protocolsPb.PeerInfo) (peer.AddrInfo, error) {
	peerID, err := peer.IDFromBytes(pbPeer.Id)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to parse peer ID: %w", err)
	}

	addrs := make([]ma.Multiaddr, 0, len(pbPeer.Addrs))
	for _, addrBytes := range pbPeer.Addrs {
		addr, err := ma.NewMultiaddrBytes(addrBytes)
		if err != nil {
			return peer.AddrInfo{}, fmt.Errorf("failed to parse multiaddr: %w", err)
		}
		addrs = append(addrs, addr)
	}

	return peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}, nil
}

// handleDiscovery handles a discovery request. All tatanka nodes that the node
// is connected to are shared with the requesting node.
func (t *TatankaNode) handleDiscovery(s network.Stream) {
	t.log.Infof("handleDiscovery called for stream %s", s.Conn().RemotePeer().ShortString())
	defer func() { _ = s.Close() }()

	peerStore := t.node.Network().Peerstore()
	manifestPeerIDs := t.manifest.allPeerIDs()
	pbPeers := make([]*protocolsPb.PeerInfo, 0, len(manifestPeerIDs))

	for p := range manifestPeerIDs {
		if p == t.node.ID() {
			continue
		}
		if t.node.Network().Connectedness(p) != network.Connected {
			continue
		}

		peerInfo := peerStore.PeerInfo(p)
		if len(peerInfo.Addrs) == 0 {
			continue
		}

		pbPeers = append(pbPeers, libp2pPeerInfoToPb(peerInfo))
	}

	response := pbDiscoveryResponse(pbPeers)
	t.log.Infof("Writing discovery response to stream %s", s.Conn().RemotePeer().ShortString())
	if err := codec.WriteLengthPrefixedMessage(s, response); err != nil {
		t.log.Errorf("Failed to write discovery response: %v", err)
		return
	}
}

// handleClientPush is called when the client opens a push stream to the node.
func (t *TatankaNode) handleClientPush(s network.Stream) {
	if err := codec.WriteLengthPrefixedMessage(s, pbResponseSuccess()); err != nil {
		t.log.Errorf("Failed to write success response: %v", err)
		return
	}

	t.pushStreamManager.newPushStream(s)
}

func (t *TatankaNode) publishClientSubscriptionEvent(client peer.ID, topic string, subscribed bool) {
	message := pbPushMessageSubscription(topic, client, subscribed)
	err := t.gossipSub.publishClientMessage(context.Background(), message)
	if err != nil {
		t.log.Errorf("Failed to publish subscription event: %v", err)
	}
}

// handleClientSubscribe handles a client subscribe request.
func (t *TatankaNode) handleClientSubscribe(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()
	if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
		t.log.Errorf("Failed to set read deadline for client %s: %v.", client.ShortString(), err)
		return
	}

	buf := bufio.NewReader(s)

	subscribeMessage := &protocolsPb.SubscribeRequest{}
	if err := codec.ReadLengthPrefixedMessage(buf, subscribeMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal subscribe message from client %s: %v.", client.ShortString(), err)
		// TODO: client sent invalid message, remove client?
		return
	}

	if subscribeMessage.Subscribe {
		subscribed := t.subscriptionManager.subscribeClient(client, subscribeMessage.Topic)
		if subscribed {
			t.publishClientSubscriptionEvent(client, subscribeMessage.Topic, true)
		}
	} else {
		unsubscribed := t.subscriptionManager.unsubscribeClient(client, subscribeMessage.Topic)
		if unsubscribed {
			t.publishClientSubscriptionEvent(client, subscribeMessage.Topic, false)
		}
	}
}

// handleClientPublish handles a request by a client the publish a message to a
// topic.
func (t *TatankaNode) handleClientPublish(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
		t.log.Errorf("Failed to set read deadline for client %s: %v.", client.ShortString(), err)
		return
	}

	buf := bufio.NewReader(s)

	publishMessage := &protocolsPb.PublishRequest{}
	if err := codec.ReadLengthPrefixedMessage(buf, publishMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal publish message from client %s: %v.", client.ShortString(), err)
		// TODO: remove client?
		return
	}

	message := pbPushMessageBroadcast(publishMessage.Topic, publishMessage.Data, client)
	err := t.gossipSub.publishClientMessage(context.Background(), message)
	if err != nil {
		t.log.Errorf("Failed to publish client message: %w", err)
	}
}

// handleClientAddr handles a request by a client to get the addresses of another
// client. This will allow them to initiate p2p communication with that client.
func (t *TatankaNode) handleClientAddr(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
		t.log.Errorf("Failed to set read deadline for client %s: %v.", client.ShortString(), err)
		return
	}

	sendErrorResponse := func(err error) {
		responseMessage := pbResponseError(err)
		if werr := codec.WriteLengthPrefixedMessage(s, responseMessage); werr != nil {
			t.log.Errorf("Failed to write length prefixed message: %v.", werr)
		}
	}

	buf := bufio.NewReader(s)

	requestMessage := &protocolsPb.ClientAddrRequest{}
	if err := codec.ReadLengthPrefixedMessage(buf, requestMessage); err != nil {
		sendErrorResponse(fmt.Errorf("failed to read/unmarshal addr request message: %w", err))
		return
	}

	requestedPeerID, err := peer.IDFromBytes(requestMessage.Id)
	if err != nil {
		sendErrorResponse(fmt.Errorf("failed to parse peer ID from request: %w", err))
		return
	}

	addr := t.clientConnectionManager.getAddrForClient(requestedPeerID)
	if addr == nil {
		sendErrorResponse(fmt.Errorf("peer not found"))
		return
	}

	addrBytes := make([][]byte, len(addr))
	for i, a := range addr {
		addrBytes[i] = a.Bytes()
	}

	responseMessage := pbResponseClientAddr(addrBytes)
	if err := codec.WriteLengthPrefixedMessage(s, responseMessage); err != nil {
		t.log.Errorf("Failed to write addr response message to client %s: %v.", client.ShortString(), err)
	}
}

func (t *TatankaNode) handlePostBonds(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	if err := codec.SetReadDeadline(codec.ReadTimeout, s); err != nil {
		t.log.Errorf("Failed to set read deadline for client %s: %v.", client.ShortString(), err)
		return
	}

	buf := bufio.NewReader(s)

	postBondMessage := &protocolsPb.PostBondRequest{}
	if err := codec.ReadLengthPrefixedMessage(buf, postBondMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal post bond message from client %s: %v.", client.ShortString(), err)
		// TODO: remove client?
		return
	}

	sendInvalidBondIndex := func(index uint32) {
		responseMessage := pbResponsePostBondError(index)
		if err := codec.WriteLengthPrefixedMessage(s, responseMessage); err != nil {
			t.log.Errorf("Failed to write invalid bond index response: %v.", err)
		}
	}

	sendErrorResponse := func(err error) {
		responseMessage := pbResponseError(err)
		if err := codec.WriteLengthPrefixedMessage(s, responseMessage); err != nil {
			t.log.Errorf("Failed to write post bond error response: %v.", err)
		}
	}

	bondsParams := make([]*bondParams, 0, len(postBondMessage.Bonds))
	for i, bond := range postBondMessage.Bonds {
		valid, expiry, strength, err := t.bondVerifier.verifyBond(bond.AssetID, bond.BondID, client)
		if err != nil {
			sendErrorResponse(err)
			return
		}
		if !valid {
			sendInvalidBondIndex(uint32(i))
			return
		}
		bondsParams = append(bondsParams, &bondParams{
			id:       string(bond.BondID),
			expiry:   expiry,
			strength: strength,
		})
	}

	totalStrength := t.bondStorage.addBonds(client, bondsParams)
	successResponse := pbResponsePostBond(totalStrength)
	if err := codec.WriteLengthPrefixedMessage(s, successResponse); err != nil {
		t.log.Errorf("Failed to write post bond success response: %v.", err)
	}
}

// --- Protobuf Helper Functions ---

func pbDiscoveryResponse(peers []*protocolsPb.PeerInfo) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_DiscoveryResponse{
			DiscoveryResponse: &protocolsPb.DiscoveryResponse{
				Peers: peers,
			},
		},
	}
}

func pbPushMessageSubscription(topic string, client peer.ID, subscribed bool) *protocolsPb.PushMessage {
	messageType := protocolsPb.PushMessage_SUBSCRIBE
	if !subscribed {
		messageType = protocolsPb.PushMessage_UNSUBSCRIBE
	}
	return &protocolsPb.PushMessage{
		MessageType: messageType,
		Topic:       topic,
		Sender:      []byte(client),
	}
}

func pbPushMessageBroadcast(topic string, data []byte, sender peer.ID) *protocolsPb.PushMessage {
	return &protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_BROADCAST,
		Topic:       topic,
		Data:        data,
		Sender:      []byte(sender),
	}
}

func pbResponseError(err error) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Error{
			Error: &protocolsPb.Error{
				Error: &protocolsPb.Error_Message{
					Message: err.Error(),
				},
			},
		},
	}
}

func pbResponseUnauthorizedError() *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Error{
			Error: &protocolsPb.Error{
				Error: &protocolsPb.Error_Unauthorized{
					Unauthorized: &protocolsPb.UnauthorizedError{},
				},
			},
		},
	}
}

func pbResponseClientAddr(addrs [][]byte) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_AddrResponse{
			AddrResponse: &protocolsPb.ClientAddrResponse{
				Addrs: addrs,
			},
		},
	}
}

func pbResponsePostBondError(index uint32) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Error{
			Error: &protocolsPb.Error{
				Error: &protocolsPb.Error_PostBondError{
					PostBondError: &protocolsPb.PostBondError{
						InvalidBondIndex: index,
					},
				},
			},
		},
	}
}

func pbResponsePostBond(bondStrength uint32) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_PostBondResponse{
			PostBondResponse: &protocolsPb.PostBondResponse{
				BondStrength: bondStrength,
			},
		},
	}
}

func pbResponseSuccess() *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Success{
			Success: &protocolsPb.Success{},
		},
	}
}
