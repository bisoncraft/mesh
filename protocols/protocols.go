package protocols

const (
	// ClientSubscribeProtocol is used by a client to subscribe to a topic.
	// Client opens stream -> writes topic string -> server ACK/closes.
	ClientSubscribeProtocol = "/tatanka/subscribe/1.0.0"

	// ClientSubscriptionsProtocol is used by a client to get the list of topics
	// they are subscribed to.
	ClientSubscriptionsProtocol = "/tatanka/subscriptions/1.0.0"

	// ClientPublishProtocol is used by a client to publish a message.
	// Client opens stream -> writes topic string -> writes data -> server closes.
	ClientPublishProtocol = "/tatanka/publish/1.0.0"

	// ClientPushProtocol is the persistent stream a client opens
	// to *receive* messages from the server.
	ClientPushProtocol = "/tatanka/push/1.0.0"

	// ClientAddrProtocol is used by the client to get the address of another
	// client.
	ClientAddrProtocol = "/tatanka/addrs/1.0.0"

	// DiscoveryProtocol is used by clients and other tatanka nodes to query the
	// other tatanka nodes that the node is connected to.
	DiscoveryProtocol = "/tatanka/discovery/1.0.0"
)
