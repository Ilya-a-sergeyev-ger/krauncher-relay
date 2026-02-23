// Package handler implements the Relay gRPC service.
package handler

import (
	"cas-relay/internal/config"
	"cas-relay/internal/relayv1"
	"cas-relay/internal/stream"
)

// RelayServer implements relayv1.RelayServer.
// Methods are split across worker.go (WorkerStream) and client.go (TaskStream, UploadPayload).
type RelayServer struct {
	relayv1.UnimplementedRelayServer
	hub        *stream.Hub
	payloadHub *stream.PayloadHub
	cfg        *config.Config
}

// NewRelayServer constructs a RelayServer.
func NewRelayServer(hub *stream.Hub, payloadHub *stream.PayloadHub, cfg *config.Config) *RelayServer {
	return &RelayServer{hub: hub, payloadHub: payloadHub, cfg: cfg}
}
