// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package config_test

import (
	"testing"

	"cas-relay/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("CAS_RELAY_SECRET", "s")
	t.Setenv("CAS_RELAY_TOKEN_SECRET", "t")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 9001 {
		t.Errorf("expected default Port=9001, got %d", cfg.Port)
	}
	if cfg.RingBufferSize != 1000 {
		t.Errorf("expected default RingBufferSize=1000, got %d", cfg.RingBufferSize)
	}
	if !cfg.LogJSON {
		t.Error("expected default LogJSON=true")
	}
}

func TestLoad_CustomValues(t *testing.T) {
	t.Setenv("CAS_RELAY_SECRET", "worker-secret")
	t.Setenv("CAS_RELAY_TOKEN_SECRET", "jwt-secret")
	t.Setenv("CAS_RELAY_PORT", "9010")
	t.Setenv("CAS_RELAY_RING_BUFFER_SIZE", "500")
	t.Setenv("CAS_RELAY_LOG_JSON", "false")
	t.Setenv("CAS_RELAY_ID", "eu-west-1")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 9010 {
		t.Errorf("expected Port=9010, got %d", cfg.Port)
	}
	if cfg.RingBufferSize != 500 {
		t.Errorf("expected RingBufferSize=500, got %d", cfg.RingBufferSize)
	}
	if cfg.LogJSON {
		t.Error("expected LogJSON=false")
	}
	if cfg.RelayID != "eu-west-1" {
		t.Errorf("expected RelayID=eu-west-1, got %q", cfg.RelayID)
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	// Neither CAS_RELAY_SECRET nor CAS_RELAY_TOKEN_SECRET set.
	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for missing required env vars")
	}
}

func TestLoad_InvalidPort(t *testing.T) {
	t.Setenv("CAS_RELAY_SECRET", "s")
	t.Setenv("CAS_RELAY_TOKEN_SECRET", "t")
	t.Setenv("CAS_RELAY_PORT", "99999")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for port out of range")
	}
}
