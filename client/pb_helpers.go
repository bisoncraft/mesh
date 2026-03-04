package client

import (
	clientpb "github.com/bisoncraft/mesh/client/pb"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
)

func pbPeerMessageSuccess(payload []byte) *protocolsPb.TatankaRelayMessageResponse {
	return &protocolsPb.TatankaRelayMessageResponse{
		Response: &protocolsPb.TatankaRelayMessageResponse_Message{
			Message: payload,
		},
	}
}

func pbMessageResponseError(msg string) *clientpb.MessageResponse {
	return &clientpb.MessageResponse{
		Response: &clientpb.MessageResponse_Error{
			Error: &clientpb.Error{
				Error: &clientpb.Error_Message{Message: msg},
			},
		},
	}
}

func pbMessageResponseSuccess(payload []byte) *clientpb.MessageResponse {
	return &clientpb.MessageResponse{
		Response: &clientpb.MessageResponse_Message{
			Message: payload,
		},
	}
}

func pbMessageResponseNoEncryptionKey() *clientpb.MessageResponse {
	return &clientpb.MessageResponse{
		Response: &clientpb.MessageResponse_Error{
			Error: &clientpb.Error{
				Error: &clientpb.Error_NoEncryptionKeyError{
					NoEncryptionKeyError: &clientpb.NoEncryptionKeyError{},
				},
			},
		},
	}
}

func pbHandshakeResponseSuccess(publicKey, nonce []byte) *clientpb.HandshakeResponse {
	return &clientpb.HandshakeResponse{
		Nonce: nonce,
		Result: &clientpb.HandshakeResponse_PublicKey{
			PublicKey: publicKey,
		},
	}
}

func pbHandshakeResponseError(msg string, nonce []byte) *clientpb.HandshakeResponse {
	return &clientpb.HandshakeResponse{
		Nonce: nonce,
		Result: &clientpb.HandshakeResponse_Error{
			Error: &clientpb.Error{
				Error: &clientpb.Error_Message{Message: msg},
			},
		},
	}
}

func pbClientRequestHandshake(hsReq *clientpb.HandshakeRequest) *clientpb.ClientRequest {
	return &clientpb.ClientRequest{
		Request: &clientpb.ClientRequest_HandshakeRequest{
			HandshakeRequest: hsReq,
		},
	}
}

func pbClientRequestMessage(encrypted []byte) *clientpb.ClientRequest {
	return &clientpb.ClientRequest{
		Request: &clientpb.ClientRequest_MessageRequest{
			MessageRequest: &clientpb.MessageRequest{
				Message: encrypted,
			},
		},
	}
}

func pbClientResponseHandshake(hsResp *clientpb.HandshakeResponse) *clientpb.ClientResponse {
	return &clientpb.ClientResponse{
		Response: &clientpb.ClientResponse_HandshakeResponse{
			HandshakeResponse: hsResp,
		},
	}
}

func pbClientResponseMessage(msgResp *clientpb.MessageResponse) *clientpb.ClientResponse {
	return &clientpb.ClientResponse{
		Response: &clientpb.ClientResponse_MessageResponse{
			MessageResponse: msgResp,
		},
	}
}
