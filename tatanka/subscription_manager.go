package tatanka

import (
	"maps"
	"slices"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// subscriptionManager handles management of client subscriptions to topics.
type subscriptionManager struct {
	mtx                 sync.RWMutex
	topicSubscriptions  map[string]map[peer.ID]struct{}
	clientSubscriptions map[peer.ID]map[string]struct{}
}

func newSubscriptionManager() *subscriptionManager {
	return &subscriptionManager{
		topicSubscriptions:  make(map[string]map[peer.ID]struct{}),
		clientSubscriptions: make(map[peer.ID]map[string]struct{}),
	}
}

// subscribeClient returns true if the client was previously not subscribed to
// the topic.
func (sm *subscriptionManager) subscribeClient(client peer.ID, topic string) bool {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	if _, ok := sm.topicSubscriptions[topic]; !ok {
		sm.topicSubscriptions[topic] = make(map[peer.ID]struct{})
	} else if _, alreadySubbed := sm.topicSubscriptions[topic][client]; alreadySubbed {
		return false
	}

	sm.topicSubscriptions[topic][client] = struct{}{}

	if _, ok := sm.clientSubscriptions[client]; !ok {
		sm.clientSubscriptions[client] = make(map[string]struct{})
	}
	sm.clientSubscriptions[client][topic] = struct{}{}

	return true
}

// unsubscribeClient returns true if the client was previously subscribed to
// the topic.
func (sm *subscriptionManager) unsubscribeClient(client peer.ID, topic string) bool {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	topicMap, topicExists := sm.topicSubscriptions[topic]
	if !topicExists {
		return false
	}

	if _, clientSubbed := topicMap[client]; !clientSubbed {
		return false
	}

	delete(topicMap, client)
	if len(topicMap) == 0 {
		delete(sm.topicSubscriptions, topic)
	}

	if clientMap, clientExists := sm.clientSubscriptions[client]; clientExists {
		delete(clientMap, topic)
		if len(clientMap) == 0 {
			delete(sm.clientSubscriptions, client)
		}
	}

	return true
}

func (sm *subscriptionManager) clientsForTopic(topic string) []peer.ID {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	subscriptions, ok := sm.topicSubscriptions[topic]
	if !ok || len(subscriptions) == 0 {
		return nil
	}

	return slices.Collect(maps.Keys(subscriptions))
}

func (sm *subscriptionManager) topicsForClient(client peer.ID) []string {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	topics, ok := sm.clientSubscriptions[client]
	if !ok {
		return nil
	}

	return slices.Collect(maps.Keys(topics))
}

func (sm *subscriptionManager) removeClient(client peer.ID) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	topics, ok := sm.clientSubscriptions[client]
	if !ok {
		return
	}

	for topic := range topics {
		if topicMap, ok := sm.topicSubscriptions[topic]; ok {
			delete(topicMap, client)
			if len(topicMap) == 0 {
				delete(sm.topicSubscriptions, topic)
			}
		}
	}

	delete(sm.clientSubscriptions, client)
}
