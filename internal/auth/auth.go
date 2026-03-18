// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

// Package auth implements authentication for worker and client connections.
//
// Workers authenticate with HMAC-SHA256 signatures passed as gRPC metadata.
// Clients authenticate with short-lived JWT tokens issued by the broker,
// passed via the "authorization" metadata key.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/metadata"
)

// timestampWindow is the maximum allowed clock skew for worker auth.
const timestampWindow = 30 * time.Second

// WorkerClaims holds identity extracted from a valid worker auth request.
type WorkerClaims struct {
	WorkerID string
}

// ClientClaims holds identity extracted from a valid relay_task_token.
type ClientClaims struct {
	TaskID string
	UserID string
}

// VerifyWorkerMetadata validates HMAC-based worker authentication from gRPC metadata.
//
// The worker must send three metadata keys:
//
//	x-worker-id:  <worker_id>
//	x-worker-ts:  <unix_timestamp_seconds>
//	x-worker-sig: <hex(HMAC-SHA256(secret, "<worker_id>:<timestamp>"))>
//
// Requests with a timestamp outside ±30s of server time are rejected.
func VerifyWorkerMetadata(ctx context.Context, secret string) (*WorkerClaims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("missing gRPC metadata")
	}

	workerID := mdFirst(md, "x-worker-id")
	tsStr := mdFirst(md, "x-worker-ts")
	sig := mdFirst(md, "x-worker-sig")

	if workerID == "" || tsStr == "" || sig == "" {
		return nil, errors.New("missing worker auth metadata (x-worker-id, x-worker-ts, x-worker-sig)")
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, errors.New("x-worker-ts is not a valid integer")
	}
	diff := time.Since(time.Unix(ts, 0))
	if diff < -timestampWindow || diff > timestampWindow {
		return nil, fmt.Errorf("x-worker-ts outside allowed window (diff=%v, max=±%v)", diff, timestampWindow)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s:%s", workerID, tsStr)
	expected := hex.EncodeToString(mac.Sum(nil))

	// Constant-time comparison prevents timing oracle attacks.
	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return nil, errors.New("x-worker-sig mismatch")
	}

	return &WorkerClaims{WorkerID: workerID}, nil
}

// relayTokenClaims is the JWT claims structure for relay_task_token.
type relayTokenClaims struct {
	TaskID string `json:"task_id"`
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

// VerifyClientMetadata validates a relay_task_token JWT from gRPC metadata.
//
// The token must be passed as the "authorization" metadata key with value
// "bearer <jwt>" (case-insensitive prefix).
func VerifyClientMetadata(ctx context.Context, secret string) (*ClientClaims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("missing gRPC metadata")
	}

	tokenStr := ""
	if hdr := mdFirst(md, "authorization"); hdr != "" {
		if len(hdr) > 7 && strings.EqualFold(hdr[:7], "bearer ") {
			tokenStr = hdr[7:]
		}
	}
	if tokenStr == "" {
		return nil, errors.New("missing relay_task_token (authorization: bearer <jwt>)")
	}

	claims := &relayTokenClaims{}
	_, err := jwt.ParseWithClaims(
		tokenStr,
		claims,
		func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method %q", t.Header["alg"])
			}
			return []byte(secret), nil
		},
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid relay_task_token: %w", err)
	}
	if claims.TaskID == "" {
		return nil, errors.New("relay_task_token missing task_id claim")
	}

	return &ClientClaims{TaskID: claims.TaskID, UserID: claims.UserID}, nil
}

// WorkerAuthMetadata returns the three authentication metadata pairs a worker
// must attach to its WorkerStream call. Intended for tests and the worker client.
func WorkerAuthMetadata(workerID, secret string) (id, ts, sig string) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s:%s", workerID, now)
	return workerID, now, hex.EncodeToString(mac.Sum(nil))
}

// mdFirst returns the first value for key in md, or "" if absent.
func mdFirst(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}
