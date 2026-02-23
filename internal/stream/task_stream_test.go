package stream_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cas-relay/internal/stream"
)

// recvTimeout reads one message from ch within 1 second or fails the test.
func recvTimeout(t *testing.T, ch <-chan stream.Message) stream.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		return m
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
		return stream.Message{}
	}
}

// drainTimeout reads all messages currently buffered in ch (non-blocking).
func drainTimeout(t *testing.T, ch <-chan stream.Message, max int) []stream.Message {
	t.Helper()
	var out []stream.Message
	deadline := time.After(time.Second)
	for len(out) < max {
		select {
		case m, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, m)
		case <-deadline:
			return out
		}
	}
	return out
}

func TestTaskStream_Publish_AssignsSeq(t *testing.T) {
	ts := stream.NewTaskStream("t1", 10)
	ts.Publish(stream.Message{})
	ts.Publish(stream.Message{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := ts.Subscribe(ctx)

	m1 := recvTimeout(t, ch)
	m2 := recvTimeout(t, ch)

	if m1.Seq != 1 || m2.Seq != 2 {
		t.Fatalf("expected seq 1,2 got %d,%d", m1.Seq, m2.Seq)
	}
}

func TestTaskStream_Backfill_BeforeLive(t *testing.T) {
	ts := stream.NewTaskStream("t2", 100)

	// Publish two messages before any subscriber.
	ts.Publish(stream.Message{Type: stream.TypeStdout, Data: json.RawMessage(`"line1"`)})
	ts.Publish(stream.Message{Type: stream.TypeStdout, Data: json.RawMessage(`"line2"`)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := ts.Subscribe(ctx)

	// Receive backfill.
	m1 := recvTimeout(t, ch)
	m2 := recvTimeout(t, ch)
	if string(m1.Data) != `"line1"` || string(m2.Data) != `"line2"` {
		t.Fatalf("unexpected backfill: %q %q", m1.Data, m2.Data)
	}

	// Publish live message after subscription.
	ts.Publish(stream.Message{Type: stream.TypeStdout, Data: json.RawMessage(`"line3"`)})
	m3 := recvTimeout(t, ch)
	if string(m3.Data) != `"line3"` {
		t.Fatalf("unexpected live: %q", m3.Data)
	}
	// Seq must be strictly increasing.
	if m3.Seq <= m2.Seq {
		t.Fatalf("live seq %d not greater than backfill seq %d", m3.Seq, m2.Seq)
	}
}

func TestTaskStream_MultipleSubscribers(t *testing.T) {
	ts := stream.NewTaskStream("t3", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch1 := ts.Subscribe(ctx)
	ch2 := ts.Subscribe(ctx)

	ts.Publish(stream.Message{Data: json.RawMessage(`"hello"`)})

	m1 := recvTimeout(t, ch1)
	m2 := recvTimeout(t, ch2)

	if string(m1.Data) != `"hello"` || string(m2.Data) != `"hello"` {
		t.Fatalf("fan-out failed: ch1=%q ch2=%q", m1.Data, m2.Data)
	}
	if m1.Seq != m2.Seq {
		t.Fatalf("seq mismatch: %d vs %d", m1.Seq, m2.Seq)
	}
}

func TestTaskStream_ContextCancel_ClosesChannel(t *testing.T) {
	ts := stream.NewTaskStream("t4", 10)

	ctx, cancel := context.WithCancel(context.Background())
	ch := ts.Subscribe(ctx)

	cancel()

	// Channel must close within 1 second.
	select {
	case _, ok := <-ch:
		if ok {
			// Drain remaining messages and wait for close.
			for range ch {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after ctx cancel")
	}
}

func TestTaskStream_Close_ClosesAllSubscribers(t *testing.T) {
	ts := stream.NewTaskStream("t5", 10)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	ch1 := ts.Subscribe(ctx1)
	ch2 := ts.Subscribe(ctx2)

	ts.Close()

	closed := func(ch <-chan stream.Message) bool {
		deadline := time.After(time.Second)
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return true
				}
			case <-deadline:
				return false
			}
		}
	}

	if !closed(ch1) {
		t.Error("ch1 not closed after stream.Close()")
	}
	if !closed(ch2) {
		t.Error("ch2 not closed after stream.Close()")
	}
}

func TestTaskStream_Publish_ReturnsFalseAfterClose(t *testing.T) {
	ts := stream.NewTaskStream("t6", 10)
	ts.Close()

	if ts.Publish(stream.Message{}) {
		t.Fatal("Publish should return false after Close")
	}
}

func TestTaskStream_Subscribe_AfterClose_ImmediatelyClosedChannel(t *testing.T) {
	ts := stream.NewTaskStream("t7", 10)
	ts.Publish(stream.Message{Data: json.RawMessage(`"x"`)})
	ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := ts.Subscribe(ctx)

	// Channel should be closed immediately (no data, since stream is closed).
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel, got a message")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout: channel not closed")
	}
}

func TestTaskStream_NoOrdering_Violation(t *testing.T) {
	// Publishes N messages before and after subscription; verifies that
	// the subscriber sees them in strictly ascending Seq order.
	ts := stream.NewTaskStream("t8", 200)

	const prePublish = 50
	for i := 0; i < prePublish; i++ {
		ts.Publish(stream.Message{})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := ts.Subscribe(ctx)

	const postPublish = 50
	for i := 0; i < postPublish; i++ {
		ts.Publish(stream.Message{})
	}

	total := prePublish + postPublish
	msgs := drainTimeout(t, ch, total)
	if len(msgs) != total {
		t.Fatalf("expected %d messages, got %d", total, len(msgs))
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Seq <= msgs[i-1].Seq {
			t.Fatalf("ordering violation at index %d: seq %d after seq %d",
				i, msgs[i].Seq, msgs[i-1].Seq)
		}
	}
}
