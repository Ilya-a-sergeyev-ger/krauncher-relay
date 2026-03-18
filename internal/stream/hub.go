// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package stream

import (
	"sync"
	"time"
)

// taskEntry wraps a TaskStream with metadata used by the GC.
type taskEntry struct {
	stream      *TaskStream
	lastUpdated time.Time
}

// Hub is the central registry of TaskStreams.
// A single Hub instance is shared by all handlers.
type Hub struct {
	mu      sync.RWMutex
	streams map[string]*taskEntry
	ringSize int
}

// NewHub creates a Hub where each TaskStream uses a ring buffer of ringSize.
func NewHub(ringSize int) *Hub {
	return &Hub{
		streams:  make(map[string]*taskEntry),
		ringSize: ringSize,
	}
}

// GetOrCreate returns the existing TaskStream for taskID, or creates a new one.
func (h *Hub) GetOrCreate(taskID string) *TaskStream {
	// Fast path.
	h.mu.RLock()
	if e, ok := h.streams[taskID]; ok {
		h.mu.RUnlock()
		return e.stream
	}
	h.mu.RUnlock()

	// Slow path — write lock with double-check.
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.streams[taskID]; ok {
		return e.stream
	}
	ts := NewTaskStream(taskID, h.ringSize)
	h.streams[taskID] = &taskEntry{stream: ts, lastUpdated: time.Now()}
	return ts
}

// Get returns the TaskStream for taskID and true, or nil and false if not found.
func (h *Hub) Get(taskID string) (*TaskStream, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if e, ok := h.streams[taskID]; ok {
		return e.stream, true
	}
	return nil, false
}

// Touch updates the last-activity timestamp for taskID (called on each
// incoming worker message to reset the GC TTL).
func (h *Hub) Touch(taskID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.streams[taskID]; ok {
		e.lastUpdated = time.Now()
	}
}

// Close closes and removes the stream for taskID.
// No-op if the stream does not exist.
func (h *Hub) Close(taskID string) {
	h.mu.Lock()
	e, ok := h.streams[taskID]
	if ok {
		delete(h.streams, taskID)
	}
	h.mu.Unlock()

	if ok {
		e.stream.Close()
	}
}

// GC removes streams that are either already closed or have had no activity
// for longer than ttl (e.g. worker disconnected without sending stream_ended).
// Stale-but-open streams are closed before removal.
// Returns the number of streams removed.
func (h *Hub) GC(ttl time.Duration) int {
	now := time.Now()

	// Collect candidates under a read lock to minimise contention.
	h.mu.RLock()
	var stale []string
	for id, e := range h.streams {
		if e.stream.IsClosed() || now.Sub(e.lastUpdated) > ttl {
			stale = append(stale, id)
		}
	}
	h.mu.RUnlock()

	if len(stale) == 0 {
		return 0
	}

	// Confirm under write lock (state may have changed) and remove.
	var toClose []*TaskStream
	h.mu.Lock()
	for _, id := range stale {
		e, ok := h.streams[id]
		if !ok {
			continue
		}
		if e.stream.IsClosed() || now.Sub(e.lastUpdated) > ttl {
			delete(h.streams, id)
			if !e.stream.IsClosed() {
				toClose = append(toClose, e.stream)
			}
		}
	}
	h.mu.Unlock()

	// Close outside the lock to avoid holding it during fan-out teardown.
	for _, ts := range toClose {
		ts.Close()
	}
	return len(stale)
}

// Size returns the number of streams currently tracked.
func (h *Hub) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.streams)
}
