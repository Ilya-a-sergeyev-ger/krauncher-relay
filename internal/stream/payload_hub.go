// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package stream

import (
	"encoding/json"
	"sync"
)

// PayloadHub coordinates encrypted payload delivery from client to worker.
//
// A client uploads the encrypted task payload via PUT /tasks/{id}/payload.
// The worker registers itself (via the key_exchange event) to receive the payload.
// PayloadHub handles the race: payload may arrive before or after the worker registers.
type PayloadHub struct {
	mu       sync.Mutex
	payloads map[string]json.RawMessage // task_id → pending encrypted payload
	workers  map[string]chan<- Message  // task_id → worker outbound channel
}

// NewPayloadHub creates an empty PayloadHub.
func NewPayloadHub() *PayloadHub {
	return &PayloadHub{
		payloads: make(map[string]json.RawMessage),
		workers:  make(map[string]chan<- Message),
	}
}

// RegisterWorker associates taskID with the worker's outbound channel.
// If the payload has already arrived, it is delivered immediately and removed
// from the pending map. The channel must have sufficient buffer or be read
// promptly to avoid blocking.
func (ph *PayloadHub) RegisterWorker(taskID string, ch chan<- Message) {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	ph.workers[taskID] = ch

	if data, ok := ph.payloads[taskID]; ok {
		delete(ph.payloads, taskID)
		msg := Message{
			TaskID: taskID,
			Type:   TypePayload,
			Data:   data,
		}
		select {
		case ch <- msg:
		default:
			// Worker channel full — should not happen with buffer=4; log omitted
			// to keep stream package free of logger imports.
		}
	}
}

// DeliverPayload stores or immediately forwards the encrypted payload for taskID.
// If the worker is already registered, the payload is sent to it now and the
// pending entry is not created. If the worker is not yet registered, the payload
// is stored until RegisterWorker is called.
func (ph *PayloadHub) DeliverPayload(taskID string, data json.RawMessage) {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	if ch, ok := ph.workers[taskID]; ok {
		msg := Message{
			TaskID: taskID,
			Type:   TypePayload,
			Data:   data,
		}
		select {
		case ch <- msg:
		default:
		}
		return
	}

	// Worker not yet registered — store for later delivery.
	ph.payloads[taskID] = data
}

// Cleanup removes both the pending payload and worker registration for taskID.
// Called when the task stream ends to release memory.
func (ph *PayloadHub) Cleanup(taskID string) {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	delete(ph.payloads, taskID)
	delete(ph.workers, taskID)
}
