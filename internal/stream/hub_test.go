package stream_test

import (
	"testing"
	"time"

	"cas-relay/internal/stream"
)

func TestHub_GetOrCreate_ReturnsSameInstance(t *testing.T) {
	h := stream.NewHub(10)
	ts1 := h.GetOrCreate("task-1")
	ts2 := h.GetOrCreate("task-1")
	if ts1 != ts2 {
		t.Fatal("GetOrCreate must return the same instance for the same taskID")
	}
}

func TestHub_GetOrCreate_DifferentIDs(t *testing.T) {
	h := stream.NewHub(10)
	ts1 := h.GetOrCreate("task-a")
	ts2 := h.GetOrCreate("task-b")
	if ts1 == ts2 {
		t.Fatal("different taskIDs must return different streams")
	}
}

func TestHub_Get_Missing(t *testing.T) {
	h := stream.NewHub(10)
	_, ok := h.Get("nonexistent")
	if ok {
		t.Fatal("Get on missing taskID should return false")
	}
}

func TestHub_Get_AfterCreate(t *testing.T) {
	h := stream.NewHub(10)
	h.GetOrCreate("task-1")
	ts, ok := h.Get("task-1")
	if !ok || ts == nil {
		t.Fatal("Get should return the stream after GetOrCreate")
	}
}

func TestHub_Close_ClosesStream(t *testing.T) {
	h := stream.NewHub(10)
	ts := h.GetOrCreate("task-1")
	h.Close("task-1")

	if !ts.IsClosed() {
		t.Fatal("stream should be closed after Hub.Close")
	}
	if _, ok := h.Get("task-1"); ok {
		t.Fatal("hub should not return a stream after Close")
	}
}

func TestHub_Close_NoOp_Missing(t *testing.T) {
	h := stream.NewHub(10)
	// Should not panic.
	h.Close("does-not-exist")
}

func TestHub_Size(t *testing.T) {
	h := stream.NewHub(10)
	if h.Size() != 0 {
		t.Fatalf("expected size 0, got %d", h.Size())
	}
	h.GetOrCreate("a")
	h.GetOrCreate("b")
	if h.Size() != 2 {
		t.Fatalf("expected size 2, got %d", h.Size())
	}
	h.Close("a")
	if h.Size() != 1 {
		t.Fatalf("expected size 1 after Close, got %d", h.Size())
	}
}

func TestHub_GC_RemovesClosedStreams(t *testing.T) {
	h := stream.NewHub(10)
	ts := h.GetOrCreate("task-gc")
	ts.Close() // mark as closed without going through Hub

	removed := h.GC(time.Hour) // large TTL so only closed streams are affected
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if h.Size() != 0 {
		t.Fatalf("expected hub empty after GC, size=%d", h.Size())
	}
}

func TestHub_GC_RemovesStaleStreams(t *testing.T) {
	h := stream.NewHub(10)
	h.GetOrCreate("task-stale")

	// GC with a TTL of zero: every stream is immediately stale.
	removed := h.GC(0)
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if h.Size() != 0 {
		t.Fatalf("expected hub empty after GC, size=%d", h.Size())
	}
}

func TestHub_GC_ClosesStaleOpenStreams(t *testing.T) {
	h := stream.NewHub(10)
	ts := h.GetOrCreate("task-open-stale")

	h.GC(0) // TTL=0 → stale immediately

	if !ts.IsClosed() {
		t.Fatal("stale open stream should be closed by GC")
	}
}

func TestHub_Touch_ResetsGCTimer(t *testing.T) {
	h := stream.NewHub(10)
	h.GetOrCreate("task-touched")

	// Touch to reset the timer.
	h.Touch("task-touched")

	// GC with a 10-second TTL: recently touched stream should survive.
	removed := h.GC(10 * time.Second)
	if removed != 0 {
		t.Fatalf("recently touched stream should not be GC'd, removed=%d", removed)
	}
	if h.Size() != 1 {
		t.Fatalf("expected stream to survive GC, size=%d", h.Size())
	}
}
