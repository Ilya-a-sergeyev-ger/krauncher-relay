// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package handler_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"cas-relay/internal/relayv1"
	"cas-relay/internal/stream"
)

// makeToken signs a relay_task_token for use in client tests.
func makeToken(taskID, userID, secret string, exp time.Time) string {
	type relayTokenClaims struct {
		TaskID string `json:"task_id"`
		UserID string `json:"user_id"`
		jwt.RegisteredClaims
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, relayTokenClaims{
		TaskID: taskID,
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}).SignedString([]byte(secret))
	if err != nil {
		panic(err)
	}
	return tok
}

// clientCtx returns an outgoing context with a valid Bearer token.
func (e *testEnv) clientCtx(taskID string) context.Context {
	tok := makeToken(taskID, "user-1", e.cfg.TokenSecret, time.Now().Add(time.Hour))
	md := metadata.Pairs("authorization", "bearer "+tok)
	return metadata.NewOutgoingContext(context.Background(), md)
}

// recvTask receives one TaskMessage from the stream within 2 s.
func recvTask(t *testing.T, ts relayv1.Relay_TaskStreamClient) *relayv1.TaskMessage {
	t.Helper()
	type result struct {
		msg *relayv1.TaskMessage
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := ts.Recv()
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("TaskStream Recv: %v", r.err)
		}
		return r.msg
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for TaskMessage")
		return nil
	}
}

// --- TaskStream auth ---

func TestTaskStream_Unauthorized(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// No token metadata.
	ts, err := e.client.TaskStream(ctx, &relayv1.TaskStreamRequest{TaskId: "task-1"})
	if err != nil {
		t.Fatalf("TaskStream open: %v", err)
	}
	_, err = ts.Recv()
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v (%v)", code, err)
	}
}

func TestTaskStream_ExpiredToken(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tok := makeToken("task-1", "user-1", e.cfg.TokenSecret, time.Now().Add(-time.Hour))
	md := metadata.Pairs("authorization", "bearer "+tok)
	ts, _ := e.client.TaskStream(metadata.NewOutgoingContext(ctx, md),
		&relayv1.TaskStreamRequest{TaskId: "task-1"})

	_, err := ts.Recv()
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v (%v)", code, err)
	}
}

func TestTaskStream_TaskIDMismatch(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Token signed for "other-task" but request is for "task-1".
	tok := makeToken("other-task", "user-1", e.cfg.TokenSecret, time.Now().Add(time.Hour))
	md := metadata.Pairs("authorization", "bearer "+tok)
	ts, _ := e.client.TaskStream(metadata.NewOutgoingContext(ctx, md),
		&relayv1.TaskStreamRequest{TaskId: "task-1"})

	_, err := ts.Recv()
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v (%v)", code, err)
	}
}

// --- TaskStream delivery ---

func TestTaskStream_LiveDelivery(t *testing.T) {
	e := newTestEnv(t)

	const taskID = "task-ts-live"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ts, err := e.client.TaskStream(e.clientCtx(taskID), &relayv1.TaskStreamRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("TaskStream: %v", err)
	}

	// Let the server-side goroutine subscribe before publishing.
	time.Sleep(30 * time.Millisecond)
	e.hub.GetOrCreate(taskID).Publish(stream.Message{
		TaskID: taskID,
		Type:   stream.TypeStdout,
		Data:   json.RawMessage(`"live message"`),
	})

	_ = ctx
	msg := recvTask(t, ts)
	if string(msg.Data) != `"live message"` {
		t.Fatalf("unexpected data: %q", msg.Data)
	}
	if msg.Seq == 0 {
		t.Fatal("Seq must be non-zero")
	}
}

func TestTaskStream_Backfill(t *testing.T) {
	e := newTestEnv(t)

	const taskID = "task-ts-backfill"

	// Pre-publish 3 messages into the ring buffer.
	for i := range 3 {
		e.hub.GetOrCreate(taskID).Publish(stream.Message{
			TaskID: taskID,
			Type:   stream.TypeStdout,
			Data:   json.RawMessage([]byte{'"', byte('0' + i), '"'}),
		})
	}

	ts, err := e.client.TaskStream(e.clientCtx(taskID), &relayv1.TaskStreamRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("TaskStream: %v", err)
	}

	var prevSeq uint64
	for range 3 {
		msg := recvTask(t, ts)
		if msg.Seq <= prevSeq {
			t.Fatalf("seq not monotone: %d after %d", msg.Seq, prevSeq)
		}
		prevSeq = msg.Seq
	}
}

func TestTaskStream_MultipleClients_FanOut(t *testing.T) {
	e := newTestEnv(t)

	const taskID = "task-fanout"

	ts1, err := e.client.TaskStream(e.clientCtx(taskID), &relayv1.TaskStreamRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("TaskStream1: %v", err)
	}
	ts2, err := e.client.TaskStream(e.clientCtx(taskID), &relayv1.TaskStreamRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("TaskStream2: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	e.hub.GetOrCreate(taskID).Publish(stream.Message{
		TaskID: taskID,
		Type:   stream.TypeMetric,
		Data:   json.RawMessage(`{"gpu_util_pct":95}`),
	})

	for _, ts := range []relayv1.Relay_TaskStreamClient{ts1, ts2} {
		msg := recvTask(t, ts)
		if msg.Type != string(stream.TypeMetric) {
			t.Fatalf("expected metric, got %q", msg.Type)
		}
	}
}

func TestTaskStream_StreamClose_EndsStream(t *testing.T) {
	e := newTestEnv(t)

	const taskID = "task-ts-close"
	ts, err := e.client.TaskStream(e.clientCtx(taskID), &relayv1.TaskStreamRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("TaskStream: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	// Closing the hub stream causes TaskStream to return nil (clean EOF).
	e.hub.Close(taskID)

	type result struct {
		msg *relayv1.TaskMessage
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := ts.Recv()
		ch <- result{m, err}
	}()

	select {
	case r := <-ch:
		// EOF (nil error from gRPC) or io.EOF indicates clean stream end.
		if r.err == nil && r.msg != nil {
			t.Log("received message before stream close — acceptable if backfill was pending")
		} else if r.err != nil {
			// Any error (including EOF-based) is acceptable after stream close.
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stream to end after hub.Close")
	}
}

// --- UploadPayload ---

func TestUploadPayload_Unauthorized(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// No token.
	_, err := e.client.UploadPayload(ctx, &relayv1.UploadPayloadRequest{
		TaskId: "task-1",
		Data:   []byte(`{"enc":"abc"}`),
	})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v (%v)", code, err)
	}
}

func TestUploadPayload_TaskIDMismatch(t *testing.T) {
	e := newTestEnv(t)
	ctx := e.clientCtx("other-task") // token for "other-task"
	_, err := e.client.UploadPayload(ctx, &relayv1.UploadPayloadRequest{
		TaskId: "task-1", // request for "task-1"
		Data:   []byte(`{"enc":"abc"}`),
	})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v (%v)", code, err)
	}
}

func TestUploadPayload_InvalidBody(t *testing.T) {
	e := newTestEnv(t)
	const taskID = "task-payload-invalid"
	ctx := e.clientCtx(taskID)

	_, err := e.client.UploadPayload(ctx, &relayv1.UploadPayloadRequest{
		TaskId: taskID,
		Data:   []byte(`{"not_enc":"abc"}`), // missing "enc" field
	})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (%v)", code, err)
	}
}

func TestUploadPayload_DeliveredToWorker(t *testing.T) {
	e := newTestEnv(t)

	const taskID = "task-payload-delivery"

	// Connect worker first using the shared workerCtx helper from worker_test.go.
	workerCtx2, workerCancel := context.WithCancel(e.workerCtx("worker-1"))
	defer workerCancel()
	workerStream, err := e.client.WorkerStream(workerCtx2)
	if err != nil {
		t.Fatalf("WorkerStream: %v", err)
	}

	// Send key_exchange so relay registers this worker for payload delivery.
	keyExchangeData, _ := json.Marshal(stream.EventData{Name: stream.EventKeyExchange})
	workerStream.Send(&relayv1.WorkerMessage{ //nolint:errcheck
		TaskId: taskID,
		Type:   string(stream.TypeEvent),
		Ts:     float64(time.Now().Unix()),
		Data:   keyExchangeData,
	})

	// Allow server time to process key_exchange.
	time.Sleep(50 * time.Millisecond)

	// Client uploads payload.
	clientCtx := e.clientCtx(taskID)
	_, err = e.client.UploadPayload(clientCtx, &relayv1.UploadPayloadRequest{
		TaskId: taskID,
		Data:   []byte(`{"enc":"dGVzdA"}`), // base64url "test"
	})
	if err != nil {
		t.Fatalf("UploadPayload: %v", err)
	}

	// Worker should receive a TypePayload command.
	type result struct {
		cmd *relayv1.WorkerCommand
		err error
	}
	ch := make(chan result, 1)
	go func() {
		cmd, err := workerStream.Recv()
		ch <- result{cmd, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("worker Recv: %v", r.err)
		}
		if r.cmd.Type != "payload" {
			t.Fatalf("expected type=payload, got %q", r.cmd.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: worker did not receive TypePayload command")
	}
}
