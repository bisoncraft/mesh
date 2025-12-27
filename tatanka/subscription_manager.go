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

// subscriptionChanges holds the result of a bulk subscription update.
type subscriptionChanges struct {
	subscribed   []string
	unsubscribed []string
}

// bulkSubscribe replaces a client's subscriptions with the given topics.
// It returns the list of newly subscribed topics and unsubscribed topics.
func (sm *subscriptionManager) bulkSubscribe(client peer.ID, topics []string) subscriptionChanges {
	newTopics := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		newTopics[t] = struct{}{}
	}

	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	var changes subscriptionChanges

	if currentTopics, ok := sm.clientSubscriptions[client]; ok {
		for topic := range currentTopics {
			if _, stillSubscribed := newTopics[topic]; !stillSubscribed {
				if topicMap, ok := sm.topicSubscriptions[topic]; ok {
					delete(topicMap, client)
					if len(topicMap) == 0 {
						delete(sm.topicSubscriptions, topic)
					}
				}
				changes.unsubscribed = append(changes.unsubscribed, topic)
			}
		}
	}

	if _, ok := sm.clientSubscriptions[client]; !ok {
		sm.clientSubscriptions[client] = make(map[string]struct{})
	}

	for _, topic := range topics {
		if _, alreadySubscribed := sm.clientSubscriptions[client][topic]; !alreadySubscribed {
			if _, ok := sm.topicSubscriptions[topic]; !ok {
				sm.topicSubscriptions[topic] = make(map[peer.ID]struct{})
			}
			sm.topicSubscriptions[topic][client] = struct{}{}
			changes.subscribed = append(changes.subscribed, topic)
		}
	}

	sm.clientSubscriptions[client] = newTopics

	return changes
}
