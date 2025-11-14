package tatanka

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSubscriptionManager(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, sm *subscriptionManager)
	}{
		{
			name: "basic subscribe and unsubscribe",
			run: func(t *testing.T, sm *subscriptionManager) {
				client1, _ := peer.Decode("12D3KooW9rDqyS5S8F3kJxdm1EhAeC36jV6Gq3hVyHKn2FSN2d7Z")
				client2, _ := peer.Decode("12D3KooWQYh9X5fB3jW2Uj4k3eHQ3c5j2b2f5k7m3n9p8r4q6t8")

				// Test subscribe return values
				if subbed := sm.subscribeClient(client1, "topic1"); !subbed {
					t.Errorf("subscribeClient(client1, topic1) returned false, expected true for new subscription")
				}
				if subbed := sm.subscribeClient(client1, "topic2"); !subbed {
					t.Errorf("subscribeClient(client1, topic2) returned false, expected true for new subscription")
				}
				if subbed := sm.subscribeClient(client2, "topic1"); !subbed {
					t.Errorf("subscribeClient(client2, topic1) returned false, expected true for new subscription")
				}

				gotClients := sm.clientsForTopic("topic1")
				expectedClients := []peer.ID{client1, client2}
				if len(gotClients) != len(expectedClients) || !reflect.DeepEqual(sortPeers(gotClients), sortPeers(expectedClients)) {
					t.Errorf("clientsForTopic(topic1) = %v, want %v", gotClients, expectedClients)
				}

				gotTopics := sm.topicsForClient(client1)
				expectedTopics := []string{"topic1", "topic2"}
				if len(gotTopics) != len(expectedTopics) || !reflect.DeepEqual(sortStrings(gotTopics), sortStrings(expectedTopics)) {
					t.Errorf("topicsForClient(client1) = %v, want %v", gotTopics, expectedTopics)
				}

				// Test unsubscribe return values
				if unsubbed := sm.unsubscribeClient(client1, "topic1"); !unsubbed {
					t.Errorf("unsubscribeClient(client1, topic1) returned false, expected true for existing subscription")
				}
				gotClients = sm.clientsForTopic("topic1")
				expectedClients = []peer.ID{client2}
				if len(gotClients) != len(expectedClients) || !reflect.DeepEqual(sortPeers(gotClients), sortPeers(expectedClients)) {
					t.Errorf("clientsForTopic(topic1) after unsubscribe = %v, want %v", gotClients, expectedClients)
				}

				gotTopics = sm.topicsForClient(client1)
				expectedTopics = []string{"topic2"}
				if len(gotTopics) != len(expectedTopics) || !reflect.DeepEqual(sortStrings(gotTopics), sortStrings(expectedTopics)) {
					t.Errorf("topicsForClient(client1) after unsubscribe = %v, want %v", gotTopics, expectedTopics)
				}

				if unsubbed := sm.unsubscribeClient(client2, "topic1"); !unsubbed {
					t.Errorf("unsubscribeClient(client2, topic1) returned false, expected true for existing subscription")
				}
				gotClients = sm.clientsForTopic("topic1")
				if len(gotClients) != 0 {
					t.Errorf("clientsForTopic(topic1) after full unsubscribe = %v, want []", gotClients)
				}

				gotTopics = sm.topicsForClient(client2)
				if len(gotTopics) != 0 {
					t.Errorf("topicsForClient(client2) after full unsubscribe = %v, want []", gotTopics)
				}
			},
		},
		{
			name: "duplicate subscribe",
			run: func(t *testing.T, sm *subscriptionManager) {
				client, _ := peer.Decode("12D3KooW9rDqyS5S8F3kJxdm1EhAeC36jV6Gq3hVyHKn2FSN2d7Z")

				// First subscribe should return true
				if subbed := sm.subscribeClient(client, "topic"); !subbed {
					t.Errorf("first subscribeClient returned false, expected true")
				}
				// Second subscribe should return false
				if subbed := sm.subscribeClient(client, "topic"); subbed {
					t.Errorf("duplicate subscribeClient returned true, expected false")
				}

				gotClients := sm.clientsForTopic("topic")
				expectedClients := []peer.ID{client}
				if len(gotClients) != 1 || gotClients[0] != client {
					t.Errorf("clientsForTopic after duplicate subscribe = %v, want %v", gotClients, expectedClients)
				}

				gotTopics := sm.topicsForClient(client)
				expectedTopics := []string{"topic"}
				if len(gotTopics) != 1 || gotTopics[0] != "topic" {
					t.Errorf("topicsForClient after duplicate subscribe = %v, want %v", gotTopics, expectedTopics)
				}
			},
		},
		{
			name: "unsubscribe non-existent",
			run: func(t *testing.T, sm *subscriptionManager) {
				client, _ := peer.Decode("12D3KooW9rDqyS5S8F3kJxdm1EhAeC36jV6Gq3hVyHKn2FSN2d7Z")

				// Unsubscribe from a topic that doesn't exist
				if unsubbed := sm.unsubscribeClient(client, "nonexistent"); unsubbed {
					t.Errorf("unsubscribeClient(nonexistent topic) returned true, expected false")
				}

				// Subscribe, then unsubscribe from a different topic
				sm.subscribeClient(client, "topic1")
				if unsubbed := sm.unsubscribeClient(client, "topic2"); unsubbed {
					t.Errorf("unsubscribeClient(different topic) returned true, expected false")
				}

				if len(sm.clientsForTopic("nonexistent")) != 0 {
					t.Error("Expected no clients for nonexistent topic")
				}

				if len(sm.topicsForClient(client)) != 1 {
					t.Error("Expected 1 topic for client")
				}
			},
		},
		{
			name: "remove client",
			run: func(t *testing.T, sm *subscriptionManager) {
				client1, _ := peer.Decode("12D3KooW9rDqyS5S8F3kJxdm1EhAeC36jV6Gq3hVyHKn2FSN2d7Z")
				client2, _ := peer.Decode("12D3KooWQYh9X5fB3jW2Uj4k3eHQ3c5j2b2f5k7m3n9p8r4q6t8")

				sm.subscribeClient(client1, "topic1")
				sm.subscribeClient(client1, "topic2")
				sm.subscribeClient(client2, "topic1")

				sm.removeClient(client1)

				gotClients := sm.clientsForTopic("topic1")
				expectedClients := []peer.ID{client2}
				if len(gotClients) != len(expectedClients) || !reflect.DeepEqual(sortPeers(gotClients), sortPeers(expectedClients)) {
					t.Errorf("clientsForTopic(topic1) after remove = %v, want %v", gotClients, expectedClients)
				}

				gotClients = sm.clientsForTopic("topic2")
				if len(gotClients) != 0 {
					t.Errorf("clientsForTopic(topic2) after remove = %v, want []", gotClients)
				}

				gotTopics := sm.topicsForClient(client1)
				if len(gotTopics) != 0 {
					t.Errorf("topicsForClient(client1) after remove = %v, want []", gotTopics)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newSubscriptionManager()
			tt.run(t, sm)
		})
	}
}

func sortPeers(peers []peer.ID) []peer.ID {
	sorted := make([]peer.ID, len(peers))
	copy(sorted, peers)
	slices.SortFunc(sorted, func(a, b peer.ID) int {
		return strings.Compare(string(a), string(b))
	})
	return sorted
}

func sortStrings(strings []string) []string {
	sorted := make([]string, len(strings))
	copy(sorted, strings)
	slices.SortFunc(sorted, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	return sorted
}
