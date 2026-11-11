package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type eventQueue struct {
	mu      sync.Mutex
	items   []Event
	limit   int
	dropped uint64
	path    string
	lastErr string
}

type queueFile struct {
	Version int     `json:"version"`
	Items   []Event `json:"items"`
	Dropped uint64  `json:"dropped"`
}

func newEventQueue(limit int) *eventQueue {
	if limit < 1 {
		limit = 1
	}
	return &eventQueue{limit: limit}
}

func openEventQueue(limit int, path string) (*eventQueue, error) {
	q := newEventQueue(limit)
	q.path = path
	if path == "" {
		return q, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return q, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read event queue: %w", err)
	}
	var saved queueFile
	if err := json.Unmarshal(raw, &saved); err != nil || saved.Version != 1 {
		return nil, fmt.Errorf("event queue file is invalid")
	}
	if len(saved.Items) > q.limit {
		overflow := len(saved.Items) - q.limit
		saved.Items = saved.Items[overflow:]
		saved.Dropped += uint64(overflow)
	}
	q.items = saved.Items
	q.dropped = saved.Dropped
	return q, nil
}

func (q *eventQueue) persistLocked() error {
	if q.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(q.path), 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(queueFile{Version: 1, Items: q.items, Dropped: q.dropped})
	if err != nil {
		return err
	}
	temporary := q.path + ".tmp"
	if err := os.WriteFile(temporary, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, q.path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func (q *eventQueue) Push(event Event) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.limit {
		copy(q.items, q.items[1:])
		q.items = q.items[:len(q.items)-1]
		q.dropped++
	}
	q.items = append(q.items, event)
	err := q.persistLocked()
	if err != nil {
		q.lastErr = err.Error()
	} else {
		q.lastErr = ""
	}
	return err
}

func (q *eventQueue) Peek(limit int) []Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit > len(q.items) {
		limit = len(q.items)
	}
	result := make([]Event, limit)
	copy(result, q.items[:limit])
	return result
}

func (q *eventQueue) Ack(count int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if count > len(q.items) {
		count = len(q.items)
	}
	q.items = append(q.items[:0], q.items[count:]...)
	err := q.persistLocked()
	if err != nil {
		q.lastErr = err.Error()
	} else {
		q.lastErr = ""
	}
	return err
}

func (q *eventQueue) PersistenceError() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lastErr
}

func saveAgentToken(path, token string) error {
	if path == "" || token == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, []byte(token), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func loadAgentToken(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (q *eventQueue) Stats() (int, uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items), q.dropped
}
