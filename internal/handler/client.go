// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package handler

import (
	"context"
	"encoding/json"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cas-relay/internal/auth"
	"cas-relay/internal/relayv1"
)

// maxPayloadBytes caps the encrypted payload body size (1 MB).
const maxPayloadBytes = 1 << 20

// TaskStream implements the server-side streaming RPC for client subscriptions.
//
// The client subscribes to a single task's output stream. It receives all
// messages published since the task started (backfill) plus live messages.
// The stream closes after the worker sends stream_ended + grace period.
func (s *RelayServer) TaskStream(req *relayv1.TaskStreamRequest, grpcStream relayv1.Relay_TaskStreamServer) error {
	claims, err := auth.VerifyClientMetadata(grpcStream.Context(), s.cfg.TokenSecret)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "client auth failed: %v", err)
	}
	if claims.TaskID != req.TaskId {
		return status.Errorf(codes.PermissionDenied,
			"token task_id %q does not match request task_id %q", claims.TaskID, req.TaskId)
	}

	logger := log.With().
		Str("task_id", req.TaskId).
		Str("user_id", claims.UserID).
		Logger()
	logger.Info().Msg("client_connected")
	defer logger.Info().Msg("client_disconnected")

	taskStream := s.hub.GetOrCreate(req.TaskId)
	ch := taskStream.Subscribe(grpcStream.Context())

	for msg := range ch {
		proto := &relayv1.TaskMessage{
			TaskId: msg.TaskID,
			Type:   string(msg.Type),
			Ts:     msg.TS,
			Seq:    msg.Seq,
			Data:   []byte(msg.Data),
		}
		if err := grpcStream.Send(proto); err != nil {
			return err
		}
	}
	return nil
}

// UploadPayload implements the unary RPC for encrypted payload upload.
//
// The client calls this after receiving the worker's key_exchange event and
// deriving the shared secret. The payload is forwarded to the worker's
// WorkerStream connection as a TypePayload command.
func (s *RelayServer) UploadPayload(ctx context.Context, req *relayv1.UploadPayloadRequest) (*relayv1.UploadPayloadResponse, error) {
	claims, err := auth.VerifyClientMetadata(ctx, s.cfg.TokenSecret)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "client auth failed: %v", err)
	}
	if claims.TaskID != req.TaskId {
		return nil, status.Errorf(codes.PermissionDenied,
			"token task_id %q does not match request task_id %q", claims.TaskID, req.TaskId)
	}

	if len(req.Data) > maxPayloadBytes {
		return nil, status.Errorf(codes.InvalidArgument, "payload too large (max 1 MB)")
	}

	// Validate that the data is JSON with a non-empty "enc" field.
	var parsed struct {
		Enc string `json:"enc"`
	}
	if err := json.Unmarshal(req.Data, &parsed); err != nil || parsed.Enc == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			`invalid payload data: expected {"enc":"<base64url>"}`)
	}

	s.payloadHub.DeliverPayload(req.TaskId, req.Data)

	log.Info().
		Str("task_id", req.TaskId).
		Str("user_id", claims.UserID).
		Int("bytes", len(req.Data)).
		Msg("payload_delivered")

	return &relayv1.UploadPayloadResponse{}, nil
}

