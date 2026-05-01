// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package handler_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"cas-relay/internal/auth"
	"cas-relay/internal/config"
	"cas-relay/internal/handler"
	"cas-relay/internal/relayv1"
	"cas-relay/internal/stream"
)

const bufSize = 1 << 20 // 1 MB in-memory gRPC transport

// testEnv holds a running in-process gRPC server for handler tests.
type testEnv struct {
	hub        *stream.Hub
	payloadHub *stream.PayloadHub
	cfg        *config.Config
	conn       *grpc.ClientConn
	client     relayv1.RelayClient
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	cfg := &config.Config{
		WorkerSecret:   "worker-secret",
		TokenSecret:    "token-secret",
		RingBufferSize: 100,
	}
	hub := stream.NewHub(cfg.RingBufferSize)
	payloadHub := stream.NewPayloadHub()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	relayv1.RegisterRelayServer(srv, handler.NewRelayServer(context.Background(), hub, payloadHub, cfg))
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		hub:        hub,
		payloadHub: payloadHub,
		cfg:        cfg,
		conn:       conn,
		client:     relayv1.NewRelayClient(conn),
	}
}

// workerCtx returns an outgoing context with valid HMAC metadata for workerID.
func (e *testEnv) workerCtx(workerID string) context.Context {
	id, ts, sig := auth.WorkerAuthMetadata(workerID, e.cfg.WorkerSecret)
	md := metadata.Pairs("x-worker-id", id, "x-worker-ts", ts, "x-worker-sig", sig)
	return metadata.NewOutgoingContext(context.Background(), md)
}

// recvFromCh reads one message within 2 s or fails.
func recvFromCh(t *testing.T, ch <-chan stream.Message) stream.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before message arrived")
		}
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message from stream")
		return stream.Message{}
	}
}

// --- WorkerStream auth ---

func TestWorkerStream_Unauthorized(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// No metadata — expect Unauthenticated on recv.
	ws, err := e.client.WorkerStream(ctx)
	if err != nil {
		t.Fatalf("WorkerStream open: %v", err)
	}
	// Server checks auth and returns error; client sees it on Recv.
	_, err = ws.Recv()
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v (%v)", code, err)
	}
}

func TestWorkerStream_WrongSecret(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	id, ts, sig := auth.WorkerAuthMetadata("worker-1", "wrong-secret")
	md := metadata.Pairs("x-worker-id", id, "x-worker-ts", ts, "x-worker-sig", sig)
	ws, _ := e.client.WorkerStream(metadata.NewOutgoingContext(ctx, md))

	_, err := ws.Recv()
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v (%v)", code, err)
	}
}

// --- WorkerStream delivery ---

func TestWorkerStream_MessageDelivery(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const taskID = "task-delivery"
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	ch := e.hub.GetOrCreate(taskID).Subscribe(subCtx)

	ws, err := e.client.WorkerStream(e.workerCtx("worker-1"))
	if err != nil {
		t.Fatalf("WorkerStream: %v", err)
	}
	defer ws.CloseSend()

	if err := ws.Send(&relayv1.WorkerMessage{
		TaskId: taskID,
		Type:   string(stream.TypeStdout),
		Ts:     float64(time.Now().Unix()),
		Data:   []byte(`"hello from worker"`),
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	_ = ctx
	m := recvFromCh(t, ch)
	if string(m.Data) != `"hello from worker"` {
		t.Fatalf("unexpected data: %q", m.Data)
	}
	if m.Seq == 0 {
		t.Fatal("Seq must be non-zero (assigned by Publish)")
	}
}

func TestWorkerStream_MultipleMessages_MonotoneSeq(t *testing.T) {
	e := newTestEnv(t)

	const taskID = "task-multi-seq"
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	ch := e.hub.GetOrCreate(taskID).Subscribe(subCtx)

	ws, err := e.client.WorkerStream(e.workerCtx("worker-1"))
	if err != nil {
		t.Fatalf("WorkerStream: %v", err)
	}
	defer ws.CloseSend()

	for range 5 {
		ws.Send(&relayv1.WorkerMessage{ //nolint:errcheck
			TaskId: taskID,
			Type:   string(stream.TypeStdout),
			Ts:     float64(time.Now().Unix()),
			Data:   []byte(`"line"`),
		})
	}

	var prev uint64
	for range 5 {
		m := recvFromCh(t, ch)
		if m.Seq <= prev {
			t.Fatalf("seq not monotone: %d after %d", m.Seq, prev)
		}
		prev = m.Seq
	}
}

func TestWorkerStream_MultipleTasks(t *testing.T) {
	e := newTestEnv(t)

	ws, err := e.client.WorkerStream(e.workerCtx("worker-1"))
	if err != nil {
		t.Fatalf("WorkerStream: %v", err)
	}
	defer ws.CloseSend()

	tasks := []string{"task-A", "task-B", "task-C"}
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	channels := make(map[string]<-chan stream.Message, len(tasks))
	for _, id := range tasks {
		channels[id] = e.hub.GetOrCreate(id).Subscribe(subCtx)
	}

	for _, id := range tasks {
		ws.Send(&relayv1.WorkerMessage{ //nolint:errcheck
			TaskId: id,
			Type:   string(stream.TypeEvent),
			Ts:     float64(time.Now().Unix()),
			Data:   []byte(`{"name":"execution_started","details":null}`),
		})
	}

	for _, id := range tasks {
		m := recvFromCh(t, channels[id])
		if m.TaskID != id {
			t.Fatalf("expected TaskID=%q, got %q", id, m.TaskID)
		}
	}
}

func TestWorkerStream_BackfillAfterWorkerSends(t *testing.T) {
	e := newTestEnv(t)

	ws, err := e.client.WorkerStream(e.workerCtx("worker-1"))
	if err != nil {
		t.Fatalf("WorkerStream: %v", err)
	}
	defer ws.CloseSend()

	const taskID = "task-backfill"
	for range 3 {
		ws.Send(&relayv1.WorkerMessage{ //nolint:errcheck
			TaskId: taskID,
			Type:   string(stream.TypeStdout),
			Ts:     float64(time.Now().Unix()),
			Data:   []byte(`"line"`),
		})
	}

	// Wait for handler to publish to hub.
	time.Sleep(50 * time.Millisecond)

	// Late subscriber gets backfill.
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	ch := e.hub.GetOrCreate(taskID).Subscribe(subCtx)

	for range 3 {
		recvFromCh(t, ch)
	}
}

func TestWorkerStream_StreamEnded_GracePeriodPending(t *testing.T) {
	e := newTestEnv(t)

	ws, err := e.client.WorkerStream(e.workerCtx("worker-1"))
	if err != nil {
		t.Fatalf("WorkerStream: %v", err)
	}
	defer ws.CloseSend()

	const taskID = "task-ended"
	ws.Send(&relayv1.WorkerMessage{ //nolint:errcheck
		TaskId: taskID, Type: string(stream.TypeStdout),
		Ts: float64(time.Now().Unix()), Data: []byte(`"before end"`),
	})

	time.Sleep(50 * time.Millisecond)
	ts, ok := e.hub.Get(taskID)
	if !ok {
		t.Fatal("stream must exist after first message")
	}

	endData, _ := json.Marshal(stream.EventData{Name: stream.EventStreamEnded})
	ws.Send(&relayv1.WorkerMessage{ //nolint:errcheck
		TaskId: taskID, Type: string(stream.TypeEvent),
		Ts: float64(time.Now().Unix()), Data: endData,
	})

	time.Sleep(50 * time.Millisecond)

	// Grace period (30s) has not elapsed — stream must still be open.
	if ts.IsClosed() {
		t.Fatal("stream must still be open immediately after stream_ended (grace period)")
	}
	if _, ok := e.hub.Get(taskID); !ok {
		t.Fatal("hub must still hold the stream during grace period")
	}
}
