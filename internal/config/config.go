// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

// Package config loads cas-relay runtime configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for cas-relay.
type Config struct {
	// RelayID identifies this geo instance (e.g. "eu-west-1").
	// Informational; included in log fields.
	RelayID string // CAS_RELAY_ID

	// Port is the gRPC port for worker and client connections.
	Port int // CAS_RELAY_PORT (default 9001)

	// HealthPort is the HTTP port for the /healthz endpoint used by
	// Kubernetes liveness probes and other HTTP-based health checks.
	HealthPort int // CAS_RELAY_HEALTH_PORT (default Port+1)

	// WorkerSecret is the HMAC-SHA256 key used to authenticate incoming
	// worker gRPC connections.
	WorkerSecret string // CAS_RELAY_SECRET (required)

	// TokenSecret is the HS256 key shared with the broker. It is used to
	// validate relay_task_tokens presented by clients.
	TokenSecret string // CAS_RELAY_TOKEN_SECRET (required)

	// RingBufferSize is the number of messages retained per task stream for
	// backfill delivery to late-joining subscribers.
	RingBufferSize int // CAS_RELAY_RING_BUFFER_SIZE (default 1000)

	// GCInterval is how often the hub's garbage collector runs.
	GCInterval time.Duration // CAS_RELAY_GC_INTERVAL_SEC (default 60)

	// StreamTTL is the maximum idle duration for a stream with no worker
	// activity before GC closes and removes it.
	StreamTTL time.Duration // CAS_RELAY_STREAM_TTL_SEC (default 300)

	// LogJSON controls structured JSON output. Set to false for human-readable
	// pretty-printed logs during development.
	LogJSON bool // CAS_RELAY_LOG_JSON (default true)

	// LogLevel is the minimum log level: "debug", "info", "warn", "error".
	LogLevel string // CAS_RELAY_LOG_LEVEL (default "info")
}

// Load reads configuration from environment variables and validates required fields.
func Load() (*Config, error) {
	port := getenvInt("CAS_RELAY_PORT", 9001)
	cfg := &Config{
		RelayID:        getenv("CAS_RELAY_ID", ""),
		Port:           port,
		HealthPort:     getenvInt("CAS_RELAY_HEALTH_PORT", port+1),
		WorkerSecret:   getenv("CAS_RELAY_SECRET", ""),
		TokenSecret:    getenv("CAS_RELAY_TOKEN_SECRET", ""),
		RingBufferSize: getenvInt("CAS_RELAY_RING_BUFFER_SIZE", 1000),
		GCInterval:     time.Duration(getenvInt("CAS_RELAY_GC_INTERVAL_SEC", 60)) * time.Second,
		StreamTTL:      time.Duration(getenvInt("CAS_RELAY_STREAM_TTL_SEC", 300)) * time.Second,
		LogJSON:        getenvBool("CAS_RELAY_LOG_JSON", true),
		LogLevel:       getenv("CAS_RELAY_LOG_LEVEL", "info"),
	}

	var errs []error
	if cfg.WorkerSecret == "" {
		errs = append(errs, errors.New("CAS_RELAY_SECRET is required"))
	}
	if cfg.TokenSecret == "" {
		errs = append(errs, errors.New("CAS_RELAY_TOKEN_SECRET is required"))
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		errs = append(errs, fmt.Errorf("CAS_RELAY_PORT=%d out of range [1,65535]", cfg.Port))
	}
	if cfg.RingBufferSize < 1 {
		errs = append(errs, errors.New("CAS_RELAY_RING_BUFFER_SIZE must be >= 1"))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
