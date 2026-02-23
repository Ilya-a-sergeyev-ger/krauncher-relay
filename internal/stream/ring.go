package stream

import "sync"

// RingBuffer is a thread-safe fixed-capacity circular buffer of Messages.
// When full, Push overwrites the oldest entry (FIFO eviction).
type RingBuffer struct {
	buf  []Message
	head int // index of the oldest element
	size int // number of elements currently stored
	cap  int
	mu   sync.RWMutex
}

// NewRingBuffer creates a RingBuffer with the given capacity.
// Panics if capacity < 1.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity < 1 {
		panic("ring buffer capacity must be >= 1")
	}
	return &RingBuffer{
		buf: make([]Message, capacity),
		cap: capacity,
	}
}

// Push appends msg to the buffer.
// If the buffer is full, the oldest message is overwritten.
func (r *RingBuffer) Push(msg Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.size < r.cap {
		// Buffer not yet full: write to the next free slot.
		r.buf[(r.head+r.size)%r.cap] = msg
		r.size++
	} else {
		// Buffer full: overwrite the oldest slot and advance head.
		r.buf[r.head] = msg
		r.head = (r.head + 1) % r.cap
	}
}

// Snapshot returns a copy of all messages in insertion order (oldest first).
// Returns nil when the buffer is empty.
func (r *RingBuffer) Snapshot() []Message {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.size == 0 {
		return nil
	}
	out := make([]Message, r.size)
	for i := range r.size {
		out[i] = r.buf[(r.head+i)%r.cap]
	}
	return out
}

// Len returns the number of messages currently stored.
func (r *RingBuffer) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}

// Cap returns the maximum capacity of the buffer.
func (r *RingBuffer) Cap() int {
	return r.cap
}
