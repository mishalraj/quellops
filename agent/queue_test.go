//go:build !legacy
// +build !legacy

package main

import (
	"path/filepath"
	"testing"
)

func TestEventQueueIsBoundedAndAcknowledged(t *testing.T) {
	queue := newEventQueue(2)
	_ = queue.Push(Event{EventID: "first-event"})
	_ = queue.Push(Event{EventID: "second-event"})
	_ = queue.Push(Event{EventID: "third-event"})
	depth, dropped := queue.Stats()
	if depth != 2 || dropped != 1 {
		t.Fatalf("depth=%d dropped=%d", depth, dropped)
	}
	items := queue.Peek(10)
	if items[0].EventID != "second-event" || items[1].EventID != "third-event" {
		t.Fatalf("unexpected queue contents: %#v", items)
	}
	_ = queue.Ack(1)
	items = queue.Peek(10)
	if len(items) != 1 || items[0].EventID != "third-event" {
		t.Fatalf("unexpected acknowledged queue: %#v", items)
	}
}

func TestEventQueueSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.json")
	queue, err := openEventQueue(3, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(Event{EventID: "persisted-event"}); err != nil {
		t.Fatal(err)
	}
	restarted, err := openEventQueue(3, path)
	if err != nil {
		t.Fatal(err)
	}
	items := restarted.Peek(3)
	if len(items) != 1 || items[0].EventID != "persisted-event" {
		t.Fatalf("event was not restored: %#v", items)
	}
	if err := restarted.Ack(1); err != nil {
		t.Fatal(err)
	}
	again, err := openEventQueue(3, path)
	if err != nil || len(again.Peek(3)) != 0 {
		t.Fatalf("acknowledgement was not persisted: %#v %v", again, err)
	}
}

func TestAgentTokenSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.token")
	if err := saveAgentToken(path, "cluster-token"); err != nil {
		t.Fatal(err)
	}
	token, err := loadAgentToken(path)
	if err != nil || token != "cluster-token" {
		t.Fatalf("token=%q err=%v", token, err)
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
