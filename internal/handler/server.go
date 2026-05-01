// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

// Package handler implements the Relay gRPC service.
package handler

import (
	"context"

	"cas-relay/internal/config"
	"cas-relay/internal/relayv1"
	"cas-relay/internal/stream"
)

// RelayServer implements relayv1.RelayServer.
// Methods are split across worker.go (WorkerStream) and client.go (TaskStream, UploadPayload).
type RelayServer struct {
	relayv1.UnimplementedRelayServer
	ctx        context.Context
	hub        *stream.Hub
	payloadHub *stream.PayloadHub
	cfg        *config.Config
}

// NewRelayServer constructs a RelayServer.
// ctx is the server-lifetime context; cancelled on shutdown so background
// goroutines (e.g. scheduleClose grace timers) can exit instead of leaking.
func NewRelayServer(ctx context.Context, hub *stream.Hub, payloadHub *stream.PayloadHub, cfg *config.Config) *RelayServer {
	return &RelayServer{ctx: ctx, hub: hub, payloadHub: payloadHub, cfg: cfg}
}
