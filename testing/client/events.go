package client

import (
	"encoding/json"
	"sync"
	"time"
)

// EventType describes a client event delivered to the UI.
type EventType string

const (
	EventTypeData             EventType = "data"              // message received on topic
	EventTypeBroadcast        EventType = "broadcast"         // message broadcast requested by UI
	EventTypeSubscribed       EventType = "subscribed"        // local client subscribed to a topic
	EventTypeUnsubscribed     EventType = "unsubscribed"      // local client unsubscribed from a topic
	EventTypePeerSubscribed   EventType = "peer_subscribed"   // remote peer subscribed
	EventTypePeerUnsubscribed EventType = "peer_unsubscribed" // remote peer unsubscribed
)

// Event is a typed, serializable event used for streaming and persistence.
type Event struct {
	ID      uint64    `json:"id"`
	At      time.Time `json:"at"`
	Type    EventType `json:"type"`
	Topic   string    `json:"topic,omitempty"`
	Peer    string    `json:"peer,omitempty"`
	Message string    `json:"message,omitempty"`
}

// eventStore holds all recorded events and broadcasts them to subscribers.
type eventStore struct {
	mtx         sync.RWMutex
	nextID      uint64
	events      []Event
	subscribers map[chan Event]struct{}
}

func newEventStore() *eventStore {
	return &eventStore{
		nextID:      1,
		events:      make([]Event, 0, 1024),
		subscribers: make(map[chan Event]struct{}),
	}
}

func (s *eventStore) add(e Event) Event {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	e.ID = s.nextID
	s.nextID++
	if e.At.IsZero() {
		e.At = time.Now()
	}

	s.events = append(s.events, e)
	for ch := range s.subscribers {
		select {
		case ch <- e:
		default:
			// Drop to avoid blocking the producer.
		}
	}

	return e
}

func (s *eventStore) listSince(afterID uint64) []Event {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if afterID == 0 {
		cp := make([]Event, len(s.events))
		copy(cp, s.events)
		return cp
	}

	i := 0
	for ; i < len(s.events); i++ {
		if s.events[i].ID > afterID {
			break
		}
	}
	cp := make([]Event, len(s.events)-i)
	copy(cp, s.events[i:])
	return cp
}

func (s *eventStore) subscribe() (ch <-chan Event, cancel func()) {
	c := make(chan Event, 256)

	s.mtx.Lock()
	s.subscribers[c] = struct{}{}
	s.mtx.Unlock()

	return c, func() {
		s.mtx.Lock()
		if _, ok := s.subscribers[c]; ok {
			delete(s.subscribers, c)
			close(c)
		}
		s.mtx.Unlock()
	}
}

func encodeEventJSON(e Event) (string, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
