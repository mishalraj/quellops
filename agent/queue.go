package main

import "sync"

type eventQueue struct {
	mu      sync.Mutex
	items   []Event
	limit   int
	dropped uint64
}

func newEventQueue(limit int) *eventQueue {
	if limit < 1 {
		limit = 1
	}
	return &eventQueue{limit: limit}
}

func (q *eventQueue) Push(event Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.limit {
		copy(q.items, q.items[1:])
		q.items = q.items[:len(q.items)-1]
		q.dropped++
	}
	q.items = append(q.items, event)
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

func (q *eventQueue) Ack(count int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if count > len(q.items) {
		count = len(q.items)
	}
	q.items = append(q.items[:0], q.items[count:]...)
}

func (q *eventQueue) Stats() (int, uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items), q.dropped
}
