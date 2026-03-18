// Copyright (c) 2024-2026 Ilya Sergeev. Licensed under the MIT License.

package handler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cas-relay/internal/auth"
	"cas-relay/internal/relayv1"
	"cas-relay/internal/stream"
)

// streamEndedGrace is the delay between receiving a stream_ended event and
// closing the TaskStream in the Hub. The grace period lets late-joining
// clients subscribe and receive the final backfill before the stream disappears.
const streamEndedGrace = 30 * time.Second

// workerOutChanSize is the buffer for relay→worker commands (e.g. TypePayload).
const workerOutChanSize = 4

// WorkerStream implements the bidirectional streaming RPC.
//
// One long-lived stream per worker connection. The worker sends task output
// (stdout/stderr/metric/event/status); the relay sends back TypePayload
// commands when the client uploads an encrypted payload.
func (s *RelayServer) WorkerStream(grpcStream relayv1.Relay_WorkerStreamServer) error {
	claims, err := auth.VerifyWorkerMetadata(grpcStream.Context(), s.cfg.WorkerSecret)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "worker auth failed: %v", err)
	}

	logger := log.With().Str("worker_id", claims.WorkerID).Logger()
	logger.Info().Msg("worker_connected")
	defer logger.Info().Msg("worker_disconnected")

	// outCh carries relay→worker commands (TypePayload).
	// PayloadHub writes to this channel; the write loop drains it.
	outCh := make(chan stream.Message, workerOutChanSize)

	ctx, cancel := context.WithCancel(grpcStream.Context())
	defer cancel()

	// Read goroutine: receive from worker, publish to hub.
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- workerReadLoop(grpcStream, s.hub, s.payloadHub, outCh, logger)
	}()

	// Write loop (main goroutine): forward commands to worker.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErrCh:
			return err
		case msg, ok := <-outCh:
			if !ok {
				return nil
			}
			cmd := &relayv1.WorkerCommand{
				TaskId: msg.TaskID,
				Type:   string(msg.Type),
				Ts:     msg.TS,
				Data:   []byte(msg.Data),
			}
			if err := grpcStream.Send(cmd); err != nil {
				logger.Warn().Err(err).Str("task_id", msg.TaskID).Msg("worker_send_error")
				return err
			}
			logger.Info().
				Str("task_id", msg.TaskID).
				Str("type", string(msg.Type)).
				Msg("command_sent_to_worker")
		}
	}
}

// workerReadLoop receives WorkerMessage protos from the stream, converts them
// to internal stream.Message and publishes to the Hub.
func workerReadLoop(
	grpcStream relayv1.Relay_WorkerStreamServer,
	hub *stream.Hub,
	payloadHub *stream.PayloadHub,
	outCh chan<- stream.Message,
	logger zerolog.Logger,
) error {
	for {
		proto, err := grpcStream.Recv()
		if err != nil {
			return err
		}
		// Ping/pong: worker sends a ping (task_id="") to verify bidi
		// stream is ready.  Handle before the empty-task_id guard.
		if stream.MessageType(proto.Type) == stream.TypePing {
			pong := stream.Message{
				TaskID: proto.TaskId,
				Type:   stream.TypePong,
				TS:     proto.Ts,
			}
			select {
			case outCh <- pong:
			default:
			}
			logger.Debug().Msg("ping_pong")
			continue
		}

		if proto.TaskId == "" {
			logger.Warn().Msg("ignoring worker message with empty task_id")
			continue
		}

		msg := stream.Message{
			TaskID: proto.TaskId,
			Type:   stream.MessageType(proto.Type),
			TS:     proto.Ts,
			Data:   json.RawMessage(proto.Data),
		}

		ts := hub.GetOrCreate(msg.TaskID)
		ts.Publish(msg)
		hub.Touch(msg.TaskID)

		if isKeyExchange(msg) {
			payloadHub.RegisterWorker(msg.TaskID, outCh)
			logger.Info().Str("task_id", msg.TaskID).Msg("worker_registered_for_payload")
		}
		if isStreamEnded(msg) {
			payloadHub.Cleanup(msg.TaskID)
			scheduleClose(hub, msg.TaskID, logger)
		}
	}
}

// scheduleClose closes the TaskStream after the grace period.
func scheduleClose(hub *stream.Hub, taskID string, logger zerolog.Logger) {
	logger.Info().
		Str("task_id", taskID).
		Dur("grace_sec", streamEndedGrace).
		Msg("stream_ended_received")

	go func() {
		time.Sleep(streamEndedGrace)
		hub.Close(taskID)
		logger.Info().Str("task_id", taskID).Msg("stream_closed_after_grace")
	}()
}

func isKeyExchange(msg stream.Message) bool {
	if msg.Type != stream.TypeEvent {
		return false
	}
	var evt stream.EventData
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		return false
	}
	return evt.Name == stream.EventKeyExchange
}

func isStreamEnded(msg stream.Message) bool {
	if msg.Type != stream.TypeEvent {
		return false
	}
	var evt stream.EventData
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		return false
	}
	return evt.Name == stream.EventStreamEnded
}
