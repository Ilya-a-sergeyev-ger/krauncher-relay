package stream_test

import (
	"testing"

	"cas-relay/internal/stream"
)

func TestRingBuffer_EmptySnapshot(t *testing.T) {
	r := stream.NewRingBuffer(4)
	if snap := r.Snapshot(); snap != nil {
		t.Fatalf("expected nil snapshot for empty buffer, got %v", snap)
	}
	if r.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", r.Len())
	}
}

func TestRingBuffer_PartialFill(t *testing.T) {
	r := stream.NewRingBuffer(4)
	r.Push(msg(1))
	r.Push(msg(2))

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(snap))
	}
	if snap[0].Seq != 1 || snap[1].Seq != 2 {
		t.Fatalf("unexpected order: %v", seqs(snap))
	}
}

func TestRingBuffer_ExactFill(t *testing.T) {
	r := stream.NewRingBuffer(3)
	r.Push(msg(1))
	r.Push(msg(2))
	r.Push(msg(3))

	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(snap))
	}
	if snap[0].Seq != 1 || snap[2].Seq != 3 {
		t.Fatalf("unexpected order: %v", seqs(snap))
	}
}

func TestRingBuffer_Wrap(t *testing.T) {
	r := stream.NewRingBuffer(3)
	// Fill and overflow by 2.
	for i := uint64(1); i <= 5; i++ {
		r.Push(msg(i))
	}
	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 messages after wrap, got %d", len(snap))
	}
	// Should contain seq 3, 4, 5 (oldest 1 and 2 evicted).
	if snap[0].Seq != 3 || snap[1].Seq != 4 || snap[2].Seq != 5 {
		t.Fatalf("unexpected wrap order: %v", seqs(snap))
	}
}

func TestRingBuffer_WrapManyTimes(t *testing.T) {
	r := stream.NewRingBuffer(4)
	const total = 100
	for i := uint64(1); i <= total; i++ {
		r.Push(msg(i))
	}
	snap := r.Snapshot()
	if len(snap) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(snap))
	}
	// Last 4 messages: 97, 98, 99, 100.
	for i, want := range []uint64{97, 98, 99, 100} {
		if snap[i].Seq != want {
			t.Fatalf("pos %d: expected seq %d, got %d", i, want, snap[i].Seq)
		}
	}
}

func TestRingBuffer_PanicOnZeroCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for capacity=0")
		}
	}()
	stream.NewRingBuffer(0)
}

func TestRingBuffer_CapLen(t *testing.T) {
	r := stream.NewRingBuffer(5)
	if r.Cap() != 5 {
		t.Fatalf("expected Cap=5, got %d", r.Cap())
	}
	r.Push(msg(1))
	r.Push(msg(2))
	if r.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", r.Len())
	}
}

// helpers

func msg(seq uint64) stream.Message {
	return stream.Message{TaskID: "t", Seq: seq}
}

func seqs(msgs []stream.Message) []uint64 {
	out := make([]uint64, len(msgs))
	for i, m := range msgs {
		out[i] = m.Seq
	}
	return out
}
