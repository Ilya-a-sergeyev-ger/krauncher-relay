// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

// Package stream implements the core fan-out primitives for cas-relay:
// ring-buffered per-task streams with backfill and ordered delivery.
package stream

import "encoding/json"

// MessageType identifies the kind of data in a Message.
type MessageType string

const (
	TypeStdout  MessageType = "stdout"
	TypeStderr  MessageType = "stderr"
	TypeMetric  MessageType = "metric"
	TypeEvent   MessageType = "event"
	TypeStatus  MessageType = "status"
	TypePayload MessageType = "payload" // relay→worker: encrypted task payload delivery
	TypePing    MessageType = "ping"    // worker→relay: bidi stream readiness probe
	TypePong    MessageType = "pong"    // relay→worker: bidi stream readiness ack
)

// Message is the unified envelope for all data flowing through Relay.
// The Data field contains raw JSON whose shape depends on Type:
//
//	stdout/stderr  →  JSON string  (single output line)
//	metric         →  MetricData   (GPU util, VRAM, temp)
//	event          →  EventData    (lifecycle event + optional details)
//	status         →  arbitrary JSON from the user's status_function
type Message struct {
	TaskID string          `json:"task_id"`
	Type   MessageType     `json:"type"`
	TS     float64         `json:"ts"`  // Unix timestamp, seconds with fractional part
	Seq    uint64          `json:"seq"` // monotonic per-task counter assigned by TaskStream.Publish
	Data   json.RawMessage `json:"data"`
}

// MetricData is the shape of Message.Data when Type == TypeMetric.
type MetricData struct {
	GPUUtilPct  float64 `json:"gpu_util_pct"`
	VRAMUsedGB  float64 `json:"vram_used_gb"`
	VRAMTotalGB float64 `json:"vram_total_gb"`
	GPUTempC    float64 `json:"gpu_temp_c"`
}

// EventData is the shape of Message.Data when Type == TypeEvent.
type EventData struct {
	Name    string          `json:"name"`
	Details json.RawMessage `json:"details,omitempty"`
}

// Well-known lifecycle event names emitted by the worker.
const (
	EventDownloadStarted    = "download_started"
	EventDownloadComplete   = "download_complete"
	EventPipInstallStarted  = "pip_install_started"
	EventPipInstallComplete = "pip_install_complete"
	EventExecutionStarted   = "execution_started"
	EventExecutionComplete  = "execution_complete"
	EventTaskFailed         = "task_failed"
	EventStreamEnded        = "stream_ended"
	EventKeyExchange        = "key_exchange" // E2E: worker publishes its X25519 public key
)
