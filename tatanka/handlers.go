package tatanka

import (
	"bufio"
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/martonp/tatanka-mesh/codec"
	protocolsPb "github.com/martonp/tatanka-mesh/protocols/pb"
	pb "github.com/martonp/tatanka-mesh/tatanka/pb"
	ma "github.com/multiformats/go-multiaddr"
)

// libp2pPeerInfoToPb converts a peer.AddrInfo to a pb.PeerInfo.
func libp2pPeerInfoToPb(peerInfo peer.AddrInfo) *pb.PeerInfo {
	addrBytes := make([][]byte, len(peerInfo.Addrs))
	for i, addr := range peerInfo.Addrs {
		addrBytes[i] = addr.Bytes()
	}

	return &pb.PeerInfo{
		Id:    []byte(peerInfo.ID),
		Addrs: addrBytes,
	}
}

// pbPeerInfoToLibp2p converts a pb.PeerInfo to a peer.AddrInfo.
func pbPeerInfoToLibp2p(pbPeer *pb.PeerInfo) (peer.AddrInfo, error) {
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
	defer func() { _ = s.Close() }()

	peerStore := t.node.Network().Peerstore()
	manifestPeerIDs := t.manifest.allPeerIDs()
	pbPeers := make([]*pb.PeerInfo, 0, len(manifestPeerIDs))

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

	response := &pb.DiscoveryResponse{
		Peers: pbPeers,
	}

	if err := codec.WriteLengthPrefixedMessage(s, response); err != nil {
		t.log.Errorf("Failed to write discovery response: %v", err)
		return
	}
}

// handleClientPush is called when the client opens a push stream to the node.
func (t *TatankaNode) handleClientPush(s network.Stream) {
	t.pushStreamManager.newPushStream(s)
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

	subscribeMessage := &protocolsPb.ClientSubscribeMessage{}
	if err := codec.ReadLengthPrefixedMessage(buf, subscribeMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal subscribe message from client %s: %v.", client.ShortString(), err)
		// TODO: client sent invalid message, remove client?
		return
	}

	if subscribeMessage.Subscribe {
		t.subscriptionManager.subscribeClient(client, subscribeMessage.Topic)
	} else {
		t.subscriptionManager.unsubscribeClient(client, subscribeMessage.Topic)
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

	publishMessage := &protocolsPb.ClientPublishMessage{}
	if err := codec.ReadLengthPrefixedMessage(buf, publishMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal publish message from client %s: %v.", client.ShortString(), err)
		// TODO: remove client?
		return
	}

	err := t.gossipSub.publishClientMessage(context.Background(), &protocolsPb.ClientPushMessage{
		Topic:  publishMessage.Topic,
		Data:   publishMessage.Data,
		Sender: []byte(client),
	})
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
		responseMessage := &protocolsPb.ClientAddrResponseMessage{
			Result: &protocolsPb.ClientAddrResponseMessage_Error{
				Error: err.Error(),
			},
		}
		werr := codec.WriteLengthPrefixedMessage(s, responseMessage)
		t.log.Errorf("Failed to write length prefixed message: %v.", werr)
	}

	buf := bufio.NewReader(s)

	requestMessage := &protocolsPb.ClientAddrRequestMessage{}
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

	responseMessage := &protocolsPb.ClientAddrResponseMessage{
		Result: &protocolsPb.ClientAddrResponseMessage_Success_{
			Success: &protocolsPb.ClientAddrResponseMessage_Success{
				Addrs: addrBytes,
			},
		},
	}

	if err := codec.WriteLengthPrefixedMessage(s, responseMessage); err != nil {
		t.log.Errorf("Failed to write addr response message to client %s: %v.", client.ShortString(), err)
	}
}
