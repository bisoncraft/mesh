package client

import (
	"fmt"
	"sync"
)

// topicRegistry represents a registry of topics and their corresponding handlers.
type topicRegistry struct {
	mtx    sync.RWMutex
	topics map[string]TopicHandler
}

func newTopicRegistry() *topicRegistry {
	return &topicRegistry{
		topics: make(map[string]TopicHandler),
	}
}

// register tracks the provided topic and its corresponding handler.
// Returns true if registration succeeded, false if the topic was already registered.
func (t *topicRegistry) register(topic string, handlerFunc TopicHandler) bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if _, exists := t.topics[topic]; exists {
		return false
	}

	t.topics[topic] = handlerFunc
	return true
}

// unregister removes the provided topic from the registry.
// Returns true if the topic was unregistered, false if it was not registered.
func (t *topicRegistry) unregister(topic string) bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if _, exists := t.topics[topic]; !exists {
		return false
	}

	delete(t.topics, topic)
	return true
}

// fetchHandler returns the handler associated with the provided topic or an error if there is none.
func (t *topicRegistry) fetchHandler(topic string) (TopicHandler, error) {
	t.mtx.RLock()
	f, exists := t.topics[topic]
	t.mtx.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no handler found for topic %s", topic)
	}

	return f, nil
}

// topics returns a list of all subscribed topics.
func (t *topicRegistry) fetchTopics() []string {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	topics := make([]string, 0, len(t.topics))
	for k := range t.topics {
		topics = append(topics, k)
	}

	return topics
}
