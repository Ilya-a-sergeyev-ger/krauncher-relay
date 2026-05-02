// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"cas-relay/internal/config"
	"cas-relay/internal/handler"
	"cas-relay/internal/relayv1"
	"cas-relay/internal/stream"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	setupLogging(cfg)

	hub := stream.NewHub(cfg.RingBufferSize)
	payloadHub := stream.NewPayloadHub()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(2 << 20), // 2 MB — accommodates 1 MB payload + framing overhead
	}
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			log.Fatal().Err(err).
				Str("cert", cfg.TLSCert).
				Str("key", cfg.TLSKey).
				Msg("relay_tls_load_failed")
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
		log.Info().Str("cert", cfg.TLSCert).Msg("relay_tls_enabled")
	} else {
		log.Warn().Msg("relay_tls_disabled_plaintext")
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	relayv1.RegisterRelayServer(grpcSrv, handler.NewRelayServer(ctx, hub, payloadHub, cfg))

	// Standard gRPC health check service (used by Kubernetes gRPC probes).
	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	go runGC(ctx, hub, cfg)
	go serveHealthHTTP(cfg.HealthPort, hub, cfg)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Fatal().Err(err).Int("port", cfg.Port).Msg("failed_to_listen")
	}

	go func() {
		log.Info().
			Int("grpc_port", cfg.Port).
			Int("health_port", cfg.HealthPort).
			Str("relay_id", cfg.RelayID).
			Int("ring_buffer_size", cfg.RingBufferSize).
			Dur("gc_interval", cfg.GCInterval).
			Dur("stream_ttl", cfg.StreamTTL).
			Msg("relay_starting")

		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("grpc_server_error")
		}
	}()

	<-ctx.Done()
	stop()
	log.Info().Msg("shutdown_initiated")
	grpcSrv.GracefulStop()
	log.Info().Msg("shutdown_complete")
}

// setupLogging configures the global zerolog logger from cfg.
func setupLogging(cfg *config.Config) {
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	var logger zerolog.Logger
	if cfg.LogJSON {
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	}
	if cfg.RelayID != "" {
		logger = logger.With().Str("relay_id", cfg.RelayID).Logger()
	}
	log.Logger = logger
}

// runGC periodically removes stale TaskStreams from the Hub.
func runGC(ctx context.Context, hub *stream.Hub, cfg *config.Config) {
	ticker := time.NewTicker(cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if n := hub.GC(cfg.StreamTTL); n > 0 {
				log.Info().Int("removed", n).Msg("gc_completed")
			}
		case <-ctx.Done():
			return
		}
	}
}

// serveHealthHTTP runs a lightweight HTTP server for /healthz on the health port.
// This complements the gRPC health service and supports HTTP-based probes.
func serveHealthHTTP(port int, hub *stream.Hub, cfg *config.Config) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","relay_id":%q,"active_streams":%d}`,
			cfg.RelayID, hub.Size())
	})
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warn().Err(err).Int("port", port).Msg("health_http_server_error")
	}
}
