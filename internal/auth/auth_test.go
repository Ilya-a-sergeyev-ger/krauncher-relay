// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/metadata"

	"cas-relay/internal/auth"
)

const (
	testWorkerSecret = "test-worker-secret"
	testTokenSecret  = "test-token-secret"
	testWorkerID     = "worker-abc123"
	testTaskID       = "task-00000000-dead-beef-cafe-000000000001"
	testUserID       = "user-42"
)

// workerCtx builds an incoming gRPC context with worker auth metadata.
func workerCtx(id, ts, sig string) context.Context {
	md := metadata.Pairs(
		"x-worker-id", id,
		"x-worker-ts", ts,
		"x-worker-sig", sig,
	)
	return metadata.NewIncomingContext(context.Background(), md)
}

// --- Worker auth ---

func TestVerifyWorkerMetadata_Valid(t *testing.T) {
	id, ts, sig := auth.WorkerAuthMetadata(testWorkerID, testWorkerSecret)
	claims, err := auth.VerifyWorkerMetadata(workerCtx(id, ts, sig), testWorkerSecret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claims.WorkerID != testWorkerID {
		t.Fatalf("expected WorkerID=%q, got %q", testWorkerID, claims.WorkerID)
	}
}

func TestVerifyWorkerMetadata_WrongSecret(t *testing.T) {
	id, ts, sig := auth.WorkerAuthMetadata(testWorkerID, "different-secret")
	_, err := auth.VerifyWorkerMetadata(workerCtx(id, ts, sig), testWorkerSecret)
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

func TestVerifyWorkerMetadata_MissingHeaders(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	_, err := auth.VerifyWorkerMetadata(ctx, testWorkerSecret)
	if err == nil {
		t.Fatal("expected error for missing metadata")
	}
}

func TestVerifyWorkerMetadata_StaleTimestamp(t *testing.T) {
	ctx := workerCtx(testWorkerID, "1", "deadbeef") // epoch ts — outside ±30s
	_, err := auth.VerifyWorkerMetadata(ctx, testWorkerSecret)
	if err == nil {
		t.Fatal("expected error for stale timestamp")
	}
}

func TestVerifyWorkerMetadata_NoMetadata(t *testing.T) {
	_, err := auth.VerifyWorkerMetadata(context.Background(), testWorkerSecret)
	if err == nil {
		t.Fatal("expected error when context has no metadata")
	}
}

// --- Client JWT auth ---

// makeClientToken signs a relay_task_token with the given parameters.
func makeClientToken(taskID, userID, secret string, exp time.Time) string {
	type relayTokenClaims struct {
		TaskID string `json:"task_id"`
		UserID string `json:"user_id"`
		jwt.RegisteredClaims
	}
	claims := relayTokenClaims{
		TaskID: taskID,
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		panic(err)
	}
	return tok
}

// clientCtx builds an incoming gRPC context with Bearer authorization metadata.
func clientCtx(token string) context.Context {
	md := metadata.Pairs("authorization", "bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestVerifyClientMetadata_Valid(t *testing.T) {
	tok := makeClientToken(testTaskID, testUserID, testTokenSecret, time.Now().Add(time.Hour))
	claims, err := auth.VerifyClientMetadata(clientCtx(tok), testTokenSecret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claims.TaskID != testTaskID {
		t.Fatalf("expected TaskID=%q, got %q", testTaskID, claims.TaskID)
	}
	if claims.UserID != testUserID {
		t.Fatalf("expected UserID=%q, got %q", testUserID, claims.UserID)
	}
}

func TestVerifyClientMetadata_Expired(t *testing.T) {
	tok := makeClientToken(testTaskID, testUserID, testTokenSecret, time.Now().Add(-time.Hour))
	_, err := auth.VerifyClientMetadata(clientCtx(tok), testTokenSecret)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestVerifyClientMetadata_WrongSecret(t *testing.T) {
	tok := makeClientToken(testTaskID, testUserID, "wrong-secret", time.Now().Add(time.Hour))
	_, err := auth.VerifyClientMetadata(clientCtx(tok), testTokenSecret)
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

func TestVerifyClientMetadata_Missing(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	_, err := auth.VerifyClientMetadata(ctx, testTokenSecret)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
}

func TestVerifyClientMetadata_TamperedSignature(t *testing.T) {
	tok := makeClientToken(testTaskID, testUserID, testTokenSecret, time.Now().Add(time.Hour))
	tampered := tok[:len(tok)-1]
	if tok[len(tok)-1] == 'A' {
		tampered += "B"
	} else {
		tampered += "A"
	}
	_, err := auth.VerifyClientMetadata(clientCtx(tampered), testTokenSecret)
	if err == nil {
		t.Fatal("expected error for tampered token")
	}
}

func TestVerifyClientMetadata_NoMetadata(t *testing.T) {
	_, err := auth.VerifyClientMetadata(context.Background(), testTokenSecret)
	if err == nil {
		t.Fatal("expected error when context has no metadata")
	}
}
