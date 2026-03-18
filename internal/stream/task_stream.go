// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package stream

import (
	"context"
	"sync"
	"sync/atomic"
)

// clientChanSize is the buffer size of the internal live channel.
// A full live channel causes Publish to drop the message (non-blocking send).
// The subscriber can reconnect to receive backfill and resume.
const clientChanSize = 64

// TaskStream is a fan-out stream for a single task.
// One worker publishes messages; any number of clients subscribe concurrently.
//
// Ordering guarantee: each subscriber receives the full ring-buffer backfill
// before any live messages, in Seq order, without duplicates. This is
// achieved by a two-channel design inside Subscribe.
type TaskStream struct {
	taskID string
	ring   *RingBuffer

	mu      sync.Mutex
	clients map[chan Message]struct{}
	closed  bool

	seq atomic.Uint64
}

// NewTaskStream creates a TaskStream with a ring buffer of ringSize messages.
func NewTaskStream(taskID string, ringSize int) *TaskStream {
	return &TaskStream{
		taskID:  taskID,
		ring:    NewRingBuffer(ringSize),
		clients: make(map[chan Message]struct{}),
	}
}

// Publish assigns a monotonic sequence number, stores the message in the ring
// buffer, and fans it out to all active subscribers (non-blocking).
// Returns false if the stream is already closed.
func (ts *TaskStream) Publish(msg Message) bool {
	msg.Seq = ts.seq.Add(1)
	ts.ring.Push(msg)

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.closed {
		return false
	}
	for ch := range ts.clients {
		select {
		case ch <- msg:
		default: // slow subscriber — drop; backfill restores history on reconnect
		}
	}
	return true
}

// Subscribe returns a read-only channel that delivers the backfill snapshot
// followed by live messages. The returned channel is closed when:
//   - ctx is cancelled (client disconnects), or
//   - Close() is called on the stream (stream_ended / worker gone).
//
// The caller must ensure that ctx is eventually cancelled to release the
// goroutine and channel resources.
//
// Two-channel design (ordering proof):
//
//  1. Under the mutex: (a) add `live` to the fan-out set, then (b) take
//     the ring snapshot.  No Publish can run between (a) and (b) because
//     Publish also locks the mutex.  Therefore:
//     – messages published BEFORE this call are in the snapshot only.
//     – messages published AFTER this call go to `live` only.
//     → no gap, no duplicate.
//
//  2. The forwarder goroutine drains the snapshot into `out` first, then
//     forwards `live` into `out` → subscriber sees strictly ordered Seq.
func (ts *TaskStream) Subscribe(ctx context.Context) <-chan Message {
	live := make(chan Message, clientChanSize)
	out := make(chan Message, clientChanSize)

	ts.mu.Lock()
	if ts.closed {
		ts.mu.Unlock()
		close(out)
		return out
	}
	snapshot := ts.ring.Snapshot()
	ts.clients[live] = struct{}{}
	ts.mu.Unlock()

	go func() {
		defer func() {
			// Remove `live` from the fan-out set and close it (if not already
			// done by Close()). Then close `out` to signal the subscriber.
			ts.mu.Lock()
			if _, ok := ts.clients[live]; ok {
				delete(ts.clients, live)
				close(live)
			}
			ts.mu.Unlock()
			close(out)
		}()

		// Phase 1: backfill.
		for _, msg := range snapshot {
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}

		// Phase 2: live messages.
		for {
			select {
			case msg, ok := <-live:
				if !ok {
					// Stream was closed by Close().
					return
				}
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

// Close closes the stream and all subscriber channels.
// In-flight subscribers will drain their live channels and exit cleanly.
// Safe to call multiple times.
func (ts *TaskStream) Close() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.closed {
		return
	}
	ts.closed = true
	for ch := range ts.clients {
		close(ch)
	}
	// Clear the map so deferred cleanups in Subscribe goroutines skip re-close.
	ts.clients = make(map[chan Message]struct{})
}

// IsClosed reports whether the stream has been closed.
func (ts *TaskStream) IsClosed() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.closed
}

// ClientCount returns the number of active subscribers.
func (ts *TaskStream) ClientCount() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.clients)
}

// TaskID returns the task identifier this stream belongs to.
func (ts *TaskStream) TaskID() string {
	return ts.taskID
}
