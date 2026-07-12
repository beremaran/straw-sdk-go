package egress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	strawpb "github.com/beremaran/straw-protos-go/straw/v1"
)

const (
	defaultHeartbeatInterval  = 5 * time.Second
	registerTimeout           = 5 * time.Second
	heartbeatTimeout          = 5 * time.Second
	registerBackoffFloor      = 1 * time.Second
	registerBackoffMax        = 30 * time.Second
	registerBackoffFactor     = 2
	runtimeSnapshotSubject    = "straw.v1.config.snapshot"
	runtimeSnapshotAckSubject = "straw.v1.config.ack"
)

// NATSConn is the minimal request/reply surface the SDK session runtime needs.
type NATSConn interface {
	Request(subject string, data []byte, timeout time.Duration) (*nats.Msg, error)
	Subscribe(subject string, cb nats.MsgHandler) (*nats.Subscription, error)
	Flush() error
	Publish(subject string, data []byte) error
}

// AssignmentServer serves per-session assignments and reports active requests.
type AssignmentServer interface {
	ActiveRequests() uint32
	Serve(stop <-chan struct{}) error
}

type runtimeController interface {
	SetDraining(draining bool)
	Draining() bool
}

type runtimeSnapshot struct {
	ConfigVersion  uint64 `json:"config_version"`
	WorkerSettings []struct {
		WorkerID string `json:"worker_id"`
		Enabled  bool   `json:"enabled"`
		Draining bool   `json:"draining"`
	} `json:"worker_settings"`
}

// AssignmentFactory builds an assignment server for a registered session.
type AssignmentFactory func(sessionID string, maxConcurrency uint32) (AssignmentServer, error)

// Register sends a worker registration request over NATS and returns the
// Control-assigned session id.
func Register(ctx context.Context, conn NATSConn, id Identity, caps Capabilities) (string, error) {
	req, err := BuildRegisterRequest(id, caps)
	if err != nil {
		return "", fmt.Errorf("build register request: %w", err)
	}

	env := &strawpb.Envelope{
		ProtocolMajor: ProtocolMajor,
		ProtocolMinor: 0,
		Payload:       &strawpb.Envelope_RegisterRequest{RegisterRequest: req},
	}

	raw, err := MarshalEnvelope(env)
	if err != nil {
		return "", fmt.Errorf("marshal envelope: %w", err)
	}

	reply, err := request(ctx, conn, RegistrationSubject(), raw, registerTimeout)
	if err != nil {
		return "", err
	}

	resp, err := UnmarshalEnvelope(reply)
	if err != nil {
		return "", fmt.Errorf("unmarshal envelope: %w", err)
	}

	ack := resp.GetRegisterAck()
	if ack == nil {
		return "", errRegisterAckMissing
	}

	if !ack.GetOk() {
		return "", fmt.Errorf("registration rejected: %w", errRegistrationRejected)
	}

	sessionID := ack.GetSessionId()
	if sessionID == "" {
		return "", errRegistrationNoSession
	}

	return sessionID, nil
}

// Heartbeat sends a worker heartbeat request over NATS.
func Heartbeat(ctx context.Context, conn NATSConn, id Identity, sessionID string, health strawpb.WorkerHealth, activeRequests, availableCapacity, maxConcurrency uint32, draining bool) error {
	hb := BuildHeartbeat(id, sessionID, health, activeRequests, availableCapacity, maxConcurrency, draining)

	env := &strawpb.Envelope{
		ProtocolMajor: ProtocolMajor,
		ProtocolMinor: 0,
		Payload:       &strawpb.Envelope_HeartbeatRequest{HeartbeatRequest: hb},
	}

	raw, err := MarshalEnvelope(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	reply, err := request(ctx, conn, HeartbeatSubject(), raw, heartbeatTimeout)
	if err != nil {
		return err
	}

	resp, err := UnmarshalEnvelope(reply)
	if err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}

	ack := resp.GetHeartbeatAck()
	if ack == nil {
		return errHeartbeatAckMissing
	}

	if !ack.GetOk() {
		return fmt.Errorf("heartbeat rejected: %w", errHeartbeatRejected)
	}

	return nil
}

// Run keeps the worker registered and heartbeating until ctx is canceled.
func Run(ctx context.Context, conn NATSConn, id Identity, caps Capabilities, heartbeatInterval time.Duration, ready *atomic.Bool, newAssignmentServer AssignmentFactory) error {
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}

	if newAssignmentServer == nil {
		return errAssignmentFactoryRequired
	}

	setReady(ready, false)

	for {
		sessionID, err := registerWithRetry(ctx, conn, id, caps, registerBackoffFloor, registerBackoffMax)
		if err != nil {
			return err
		}

		setReady(ready, true)

		sessionLost, err := runSession(ctx, conn, id, caps, sessionID, heartbeatInterval, ready, newAssignmentServer)

		setReady(ready, false)

		if err != nil {
			return err
		}

		if !sessionLost {
			return nil
		}

		slog.Warn("control rejected worker session, re-registering", "worker_id", id.WorkerID, "session_id", sessionID)
	}
}

func registerWithRetry(ctx context.Context, conn NATSConn, id Identity, caps Capabilities, backoffMin, backoffMax time.Duration) (string, error) {
	backoff := backoffMin

	for {
		sessionID, err := Register(ctx, conn, id, caps)
		if err == nil {
			return sessionID, nil
		}

		if ctx.Err() != nil {
			return "", fmt.Errorf("register: %w", ctx.Err())
		}

		slog.Warn("registration failed, retrying", "worker_id", id.WorkerID, "backoff", backoff.String(), "error", err)

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("register: %w", ctx.Err())
		case <-time.After(backoff):
		}

		backoff = min(backoff*registerBackoffFactor, backoffMax)
	}
}

func runSession(ctx context.Context, conn NATSConn, id Identity, caps Capabilities, sessionID string, heartbeatInterval time.Duration, ready *atomic.Bool, newAssignmentServer AssignmentFactory) (bool, error) {
	worker, err := newAssignmentServer(sessionID, caps.MaxConcurrency)
	if err != nil {
		return false, err
	}

	if controller, ok := worker.(runtimeController); ok {
		sub, subErr := conn.Subscribe(runtimeSnapshotSubject, func(msg *nats.Msg) {
			var snapshot runtimeSnapshot
			if json.Unmarshal(msg.Data, &snapshot) != nil {
				return
			}

			draining := false

			for _, setting := range snapshot.WorkerSettings {
				if setting.WorkerID == id.WorkerID {
					draining = setting.Draining || !setting.Enabled

					break
				}
			}

			controller.SetDraining(draining)

			ack, _ := json.Marshal(map[string]any{"worker_id": id.WorkerID, "config_version": snapshot.ConfigVersion, "status": "applied"})
			_ = conn.Publish(runtimeSnapshotAckSubject, ack)
		})
		if subErr == nil {
			_ = conn.Flush()

			defer func() { _ = sub.Unsubscribe() }()
		}
	}

	stop := make(chan struct{})
	stopServing := sync.OnceFunc(func() { close(stop) })

	serveDone := make(chan error, 1)

	go func() { serveDone <- worker.Serve(stop) }()

	defer func() {
		stopServing()
		<-serveDone
	}()

	return runHeartbeatLoop(ctx, conn, id, sessionID, caps, worker, heartbeatInterval, ready), nil
}

func runHeartbeatLoop(ctx context.Context, conn NATSConn, id Identity, sessionID string, caps Capabilities, worker AssignmentServer, heartbeatInterval time.Duration, ready *atomic.Bool) bool {
	sendHeartbeat := func(hbCtx context.Context, draining bool) error {
		active := worker.ActiveRequests()
		if controller, ok := worker.(runtimeController); ok {
			draining = draining || controller.Draining()
		}

		return Heartbeat(hbCtx, conn, id, sessionID, strawpb.WorkerHealth_WORKER_HEALTH_READY, active, capacityFromConcurrency(active, caps.MaxConcurrency), caps.MaxConcurrency, draining)
	}

	if errors.Is(sendHeartbeat(ctx, false), errHeartbeatRejected) {
		return true
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			setReady(ready, false)

			_ = sendHeartbeat(context.WithoutCancel(ctx), true)

			return false
		case <-ticker.C:
			if errors.Is(sendHeartbeat(ctx, false), errHeartbeatRejected) {
				return true
			}
		}
	}
}

func setReady(ready *atomic.Bool, v bool) {
	if ready != nil {
		ready.Store(v)
	}
}

func request(ctx context.Context, conn NATSConn, subject string, raw []byte, timeout time.Duration) ([]byte, error) {
	if ctx != nil {
		ctxErr := ctx.Err()
		if ctxErr != nil {
			return nil, fmt.Errorf("context error: %w", ctxErr)
		}
	}

	if conn == nil {
		return nil, errConnRequired
	}

	msg, err := conn.Request(subject, raw, timeout)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", subject, err)
	}

	return msg.Data, nil
}

func capacityFromConcurrency(activeRequests, maxConcurrency uint32) uint32 {
	if maxConcurrency == 0 {
		return 0
	}

	if activeRequests >= maxConcurrency {
		return 0
	}

	return maxConcurrency - activeRequests
}

var (
	errRegisterAckMissing        = errors.New("register ack missing")
	errRegistrationRejected      = errors.New("registration rejected")
	errRegistrationNoSession     = errors.New("registration accepted without session id")
	errHeartbeatAckMissing       = errors.New("heartbeat ack missing")
	errHeartbeatRejected         = errors.New("heartbeat rejected")
	errConnRequired              = errors.New("nats connection is required")
	errAssignmentFactoryRequired = errors.New("assignment factory is required")
)
