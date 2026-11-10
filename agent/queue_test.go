//go:build !legacy
// +build !legacy

package main

import "testing"

func TestEventQueueIsBoundedAndAcknowledged(t *testing.T) {
	queue := newEventQueue(2)
	queue.Push(Event{EventID: "first-event"})
	queue.Push(Event{EventID: "second-event"})
	queue.Push(Event{EventID: "third-event"})
	depth, dropped := queue.Stats()
	if depth != 2 || dropped != 1 {
		t.Fatalf("depth=%d dropped=%d", depth, dropped)
	}
	items := queue.Peek(10)
	if items[0].EventID != "second-event" || items[1].EventID != "third-event" {
		t.Fatalf("unexpected queue contents: %#v", items)
	}
	queue.Ack(1)
	items = queue.Peek(10)
	if len(items) != 1 || items[0].EventID != "third-event" {
		t.Fatalf("unexpected acknowledged queue: %#v", items)
	}
}

func TestScopeDefaultsToDeny(t *testing.T) {
	scope := newScopeState()
	if scope.Allows("production") {
		t.Fatal("new scope must deny all namespaces")
	}
	scope.Set([]string{"production"})
	if !scope.Allows("production") || scope.Allows("staging") {
		t.Fatal("scope did not apply selected namespace")
	}
}
