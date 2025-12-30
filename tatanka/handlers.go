package tatanka

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/bond"
	"github.com/martonp/tatanka-mesh/codec"
	"github.com/martonp/tatanka-mesh/protocols"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	"github.com/martonp/tatanka-mesh/tatanka/pb"
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

	subscribeMessage := &protocolsPb.SubscribeRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, subscribeMessage); err != nil {
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

	publishMessage := &protocolsPb.PublishRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, publishMessage); err != nil {
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

func (t *TatankaNode) handlePostBonds(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	postBondMessage := &protocolsPb.PostBondRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, postBondMessage); err != nil {
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

	bondsParams := make([]*bond.BondParams, 0, len(postBondMessage.Bonds))
	for i, bp := range postBondMessage.Bonds {
		valid, expiry, strength, err := t.bondVerifier.verifyBond(bp.AssetID, bp.BondID, client)
		if err != nil {
			sendErrorResponse(err)
			return
		}
		if !valid {
			sendInvalidBondIndex(uint32(i))
			return
		}
		bondsParams = append(bondsParams, &bond.BondParams{
			ID:       string(bp.BondID),
			Expiry:   expiry,
			Strength: strength,
		})
	}

	totalStrength := t.bondStorage.addBonds(client, bondsParams)
	successResponse := pbResponsePostBond(totalStrength)
	if err := codec.WriteLengthPrefixedMessage(s, successResponse); err != nil {
		t.log.Errorf("Failed to write post bond success response: %v.", err)
	}
}

// relayMessageToCounterparty sends a relay message to a counterparty client and
// returns the response payload or a protocol error.
func (t *TatankaNode) relayMessageToCounterparty(counterpartyID, initiatorID peer.ID, message []byte) (respData []byte, protoErr *protocolsPb.Error, err error) {
	counterpartyStream, err := t.node.NewStream(context.Background(), counterpartyID, protocols.TatankaRelayMessageProtocol)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = counterpartyStream.Close() }()

	req := &protocolsPb.TatankaRelayMessageRequest{
		PeerID:  []byte(initiatorID),
		Message: message,
	}
	if err := codec.WriteLengthPrefixedMessage(counterpartyStream, req); err != nil {
		return nil, nil, err
	}

	counterpartyResp := &protocolsPb.TatankaRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(counterpartyStream, counterpartyResp); err != nil {
		return nil, nil, err
	}

	if errResp := counterpartyResp.GetError(); errResp != nil {
		return nil, errResp, nil
	}

	return counterpartyResp.GetMessage(), nil, nil
}

// handleClientRelayMessage processes a client relay message request and returns
// the counterparty's response payload or an error, then closes the stream.
func (t *TatankaNode) handleClientRelayMessage(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	requestMessage := &protocolsPb.ClientRelayMessageRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, requestMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal relay message request from client %s: %v.", client.ShortString(), err)
		return
	}

	counterpartyID, err := peer.IDFromBytes(requestMessage.PeerID)
	if err != nil {
		t.log.Debugf("Failed to parse counterparty ID from request: %v.", err)
		return
	}

	writeResponse := func(resp *protocolsPb.ClientRelayMessageResponse) {
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.log.Warnf("Failed to write relay message response to client %s: %v.", client.ShortString(), err)
		}
	}

	if t.node.Network().Connectedness(counterpartyID) == network.Connected {
		respData, protoErr, err := t.relayMessageToCounterparty(counterpartyID, client, requestMessage.Message)
		if err != nil {
			writeResponse(pbClientRelayMessageErrorMessage("failed to contact counterparty client"))
			return
		}
		if protoErr != nil {
			switch {
			case protoErr.GetCpRejectedError() != nil:
				writeResponse(pbClientRelayMessageCounterpartyRejected())
			case protoErr.GetCpNotFoundError() != nil:
				writeResponse(pbClientRelayMessageCounterpartyNotFound())
			case protoErr.GetMessage() != "":
				writeResponse(pbClientRelayMessageErrorMessage(protoErr.GetMessage()))
			default:
				writeResponse(pbClientRelayMessageErrorMessage("counterparty relay failed"))
			}
			return
		}

		writeResponse(pbClientRelayMessageSuccess(respData))
		return
	}

	// Counterparty is not directly connected; forward the request to a tatanka node that has the counterparty connected.
	tatankaPeers := t.clientConnectionManager.getTatankaPeersForClient(counterpartyID)
	if len(tatankaPeers) == 0 {
		t.log.Debugf("No tatanka peers found for counterparty %s.", counterpartyID.ShortString())
		writeResponse(pbClientRelayMessageCounterpartyNotFound())
		return
	}

	tatankaPeer := tatankaPeers[0]
	forwardStream, err := t.node.NewStream(context.Background(), tatankaPeer, forwardRelayProtocol)
	if err != nil {
		t.log.Warnf("Failed to open forward relay stream to tatanka peer %s: %v", tatankaPeer.ShortString(), err)
		writeResponse(pbClientRelayMessageErrorMessage("failed to contact counterparty node"))
		return
	}
	defer func() { _ = forwardStream.Close() }()

	forwardReq := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(client),
		CounterpartyId: []byte(counterpartyID),
		Message:        requestMessage.Message,
	}
	if err := codec.WriteLengthPrefixedMessage(forwardStream, forwardReq); err != nil {
		t.log.Warnf("Failed to write forward relay request to tatanka peer %s: %v", tatankaPeer.ShortString(), err)
		writeResponse(pbClientRelayMessageErrorMessage("failed to forward relay request"))
		return
	}

	forwardResp := &pb.TatankaForwardRelayResponse{}
	if err := codec.ReadLengthPrefixedMessage(forwardStream, forwardResp); err != nil {
		t.log.Warnf("Failed to read forward relay response from tatanka peer %s: %v", tatankaPeer.ShortString(), err)
		writeResponse(pbClientRelayMessageErrorMessage("failed to receive response from counterparty node"))
		return
	}

	if errMsg := forwardResp.GetError(); errMsg != "" {
		writeResponse(pbClientRelayMessageErrorMessage(errMsg))
		return
	}
	if forwardResp.GetClientNotFound() != nil {
		writeResponse(pbClientRelayMessageCounterpartyNotFound())
		return
	}
	if forwardResp.GetClientRejected() != nil {
		writeResponse(pbClientRelayMessageCounterpartyRejected())
		return
	}

	writeResponse(pbClientRelayMessageSuccess(forwardResp.GetSuccess()))
}

// handleForwardRelay handles a request from another tatanka node to forward a relay message to a client.
func (t *TatankaNode) handleForwardRelay(s network.Stream) {
	defer func() { _ = s.Close() }()

	peerID := s.Conn().RemotePeer()

	requestMessage := &pb.TatankaForwardRelayRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, requestMessage); err != nil {
		t.log.Warnf("Failed to read/unmarshal forward relay request message from peer %s: %v.", peerID.ShortString(), err)
		return
	}

	counterpartyID, err := peer.IDFromBytes(requestMessage.CounterpartyId)
	if err != nil {
		t.log.Warnf("Failed to parse counterparty ID from request: %v.", err)
		return
	}

	writeResponse := func(resp *pb.TatankaForwardRelayResponse) {
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.log.Warnf("Failed to write forward relay response to peer %s: %v", peerID.ShortString(), err)
		}
	}

	// If the counterparty is not connected to us, return an error. Currently only one hop forwarding
	// is supported.
	if t.node.Network().Connectedness(counterpartyID) != network.Connected {
		t.log.Warnf("Unable to forward relay request to counterparty %s because they are not connected to us.", counterpartyID.ShortString())
		writeResponse(pbTatankaForwardRelayClientNotFound())
		return
	}

	respData, protoErr, err := t.relayMessageToCounterparty(counterpartyID, peer.ID(requestMessage.InitiatorId), requestMessage.Message)
	if err != nil {
		writeResponse(pbTatankaForwardRelayError("failed to contact counterparty client"))
		return
	}
	if protoErr != nil {
		switch {
		case protoErr.GetCpRejectedError() != nil:
			writeResponse(pbTatankaForwardRelayClientRejected())
		case protoErr.GetCpNotFoundError() != nil:
			writeResponse(pbTatankaForwardRelayClientNotFound())
		case protoErr.GetMessage() != "":
			writeResponse(pbTatankaForwardRelayError(protoErr.GetMessage()))
		default:
			writeResponse(pbTatankaForwardRelayError("counterparty relay failed"))
		}
		return
	}

	writeResponse(pbTatankaForwardRelaySuccess(respData))
}

// handleDiscovery handles a request from another tatanka node to discover
// addresses for a target peer. If the target peer is not connected to us, we
// send a not found response, otherwise we send the addresses of the target peer.
func (t *TatankaNode) handleDiscovery(s network.Stream) {
	defer func() { _ = s.Close() }()

	remotePeerID := s.Conn().RemotePeer()
	requestMessage := &pb.DiscoveryRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, requestMessage); err != nil {
		t.log.Warnf("Failed to read/unmarshal discovery request message from peer %s: %v.", remotePeerID.ShortString(), err)
		return
	}

	targetPeerID, err := peer.IDFromBytes(requestMessage.Id)
	if err != nil {
		t.log.Warnf("Failed to parse target peer ID from request: %v.", err)
		return
	}

	// Only share addresses for whitelist peers.
	if _, ok := t.getWhitelist().allPeerIDs()[targetPeerID]; !ok {
		t.log.Warnf("Tatanka peer %s attempted to discover addresses for non-whitelist peer %s.", remotePeerID.ShortString(), targetPeerID.ShortString())
		if err := codec.WriteLengthPrefixedMessage(s, pbDiscoveryResponseNotFound()); err != nil {
			t.log.Warnf("Failed to write discovery response to peer %s: %v.", remotePeerID.ShortString(), err)
		}
		return
	}

	if t.node.Network().Connectedness(targetPeerID) != network.Connected {
		if err := codec.WriteLengthPrefixedMessage(s, pbDiscoveryResponseNotFound()); err != nil {
			t.log.Warnf("Failed to write discovery response to peer %s: %v.", remotePeerID.ShortString(), err)
		}
		return
	}

	addrs := t.node.Peerstore().Addrs(targetPeerID)
	if err := codec.WriteLengthPrefixedMessage(s, pbDiscoveryResponseSuccess(addrs)); err != nil {
		t.log.Warnf("Failed to write discovery response to peer %s: %v.", remotePeerID.ShortString(), err)
	}
}

// handleWhitelist handles a request from another tatanka node to verify the
// whitelist alignment. The counterparty sends the list of peer IDs in their
// whitelist. If they match with ours, we send a success response, otherwise
// we send our whitelist peer IDs so the counterparty can see the difference.
func (t *TatankaNode) handleWhitelist(s network.Stream) {
	defer func() { _ = s.Close() }()

	remotePeerID := s.Conn().RemotePeer()

	req := &pb.WhitelistRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
		t.log.Warnf("Failed to read whitelist request from %s: %v", remotePeerID.ShortString(), err)
		return
	}

	whitelist := t.getWhitelist()
	localPeerIDs := whitelist.allPeerIDs()
	var mismatch bool

	// Check if the incoming peer IDs are in the local whitelist.
	for _, idBytes := range req.PeerIDs {
		id, err := peer.IDFromBytes(idBytes)
		if err != nil {
			mismatch = true
			break
		}
		if _, ok := localPeerIDs[id]; !ok {
			mismatch = true
			break
		}
	}

	// Make sure there aren't additional peer IDs in the local whitelist.
	mismatch = mismatch || len(req.PeerIDs) != len(localPeerIDs)

	var resp *pb.WhitelistResponse
	if mismatch {
		resp = pbWhitelistResponseMismatch(whitelist.peerIDsBytes())
	} else {
		resp = pbWhitelistResponseSuccess()
	}

	if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
		t.log.Warnf("Failed to write whitelist response to %s: %v", remotePeerID.ShortString(), err)
	}
}

// --- Protobuf Helper Functions ---

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

func pbClientRelayMessageSuccess(message []byte) *protocolsPb.ClientRelayMessageResponse {
	return &protocolsPb.ClientRelayMessageResponse{
		Response: &protocolsPb.ClientRelayMessageResponse_Message{
			Message: message,
		},
	}
}

func pbClientRelayMessageError(err *protocolsPb.Error) *protocolsPb.ClientRelayMessageResponse {
	return &protocolsPb.ClientRelayMessageResponse{
		Response: &protocolsPb.ClientRelayMessageResponse_Error{
			Error: err,
		},
	}
}

func pbClientRelayMessageErrorMessage(message string) *protocolsPb.ClientRelayMessageResponse {
	return pbClientRelayMessageError(&protocolsPb.Error{
		Error: &protocolsPb.Error_Message{
			Message: message,
		},
	})
}

func pbClientRelayMessageCounterpartyNotFound() *protocolsPb.ClientRelayMessageResponse {
	return pbClientRelayMessageError(&protocolsPb.Error{
		Error: &protocolsPb.Error_CpNotFoundError{
			CpNotFoundError: &protocolsPb.CounterpartyNotFoundError{},
		},
	})
}

func pbClientRelayMessageCounterpartyRejected() *protocolsPb.ClientRelayMessageResponse {
	return pbClientRelayMessageError(&protocolsPb.Error{
		Error: &protocolsPb.Error_CpRejectedError{
			CpRejectedError: &protocolsPb.CounterpartyRejectedError{},
		},
	})
}

func pbTatankaForwardRelaySuccess(message []byte) *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_Success{
			Success: message,
		},
	}
}

func pbTatankaForwardRelayClientNotFound() *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_ClientNotFound_{
			ClientNotFound: &pb.TatankaForwardRelayResponse_ClientNotFound{},
		},
	}
}

func pbTatankaForwardRelayClientRejected() *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_ClientRejected_{
			ClientRejected: &pb.TatankaForwardRelayResponse_ClientRejected{},
		},
	}
}

func pbTatankaForwardRelayError(message string) *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_Error{
			Error: message,
		},
	}
}

func pbWhitelistResponseSuccess() *pb.WhitelistResponse {
	return &pb.WhitelistResponse{
		Response: &pb.WhitelistResponse_Success_{
			Success: &pb.WhitelistResponse_Success{},
		},
	}
}

func pbWhitelistResponseMismatch(mismatchedPeerIDs [][]byte) *pb.WhitelistResponse {
	return &pb.WhitelistResponse{
		Response: &pb.WhitelistResponse_Mismatch_{
			Mismatch: &pb.WhitelistResponse_Mismatch{
				PeerIDs: mismatchedPeerIDs,
			},
		},
	}
}

func pbDiscoveryResponseNotFound() *pb.DiscoveryResponse {
	return &pb.DiscoveryResponse{
		Response: &pb.DiscoveryResponse_NotFound_{
			NotFound: &pb.DiscoveryResponse_NotFound{},
		},
	}
}

func pbDiscoveryResponseSuccess(addrs []ma.Multiaddr) *pb.DiscoveryResponse {
	addrBytes := make([][]byte, 0, len(addrs))
	for _, addr := range addrs {
		addrBytes = append(addrBytes, addr.Bytes())
	}

	return &pb.DiscoveryResponse{
		Response: &pb.DiscoveryResponse_Success_{
			Success: &pb.DiscoveryResponse_Success{
				Addrs: addrBytes,
			},
		},
	}
}
