package egress

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	strawpb "github.com/beremaran/straw-oss/api/proto/straw/v1"
)

func runtimeTestIdentity(t *testing.T, workerID string) Identity {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	return Identity{WorkerID: workerID, CredentialID: "cred_runtime", ExecutorType: "egress", PrivateKey: priv}
}

type fakeNATSConn struct {
	mu      sync.Mutex
	handler func(subject string, data []byte) (*strawpb.Envelope, error)
}

func (c *fakeNATSConn) Request(subject string, data []byte, _ time.Duration) (*nats.Msg, error) {
	c.mu.Lock()
	handler := c.handler
	c.mu.Unlock()

	if handler == nil {
		return nil, errors.New("no handler")
	}

	env, err := handler(subject, data)
	if err != nil {
		return nil, err
	}

	raw, err := MarshalEnvelope(env)
	if err != nil {
		return nil, err
	}

	return &nats.Msg{Data: raw}, nil
}

func (c *fakeNATSConn) Subscribe(string, nats.MsgHandler) (*nats.Subscription, error) {
	return nil, errors.New("subscribe not implemented")
}

func (c *fakeNATSConn) Flush() error { return nil }

func (c *fakeNATSConn) Publish(string, []byte) error {
	return errors.New("publish not implemented")
}

func assertDistinctNonces(t *testing.T, nonces [][]byte) {
	t.Helper()

	for i := range nonces {
		if len(nonces[i]) == 0 {
			t.Fatalf("registration attempt %d carried an empty nonce", i+1)
		}

		for j := i + 1; j < len(nonces); j++ {
			if bytes.Equal(nonces[i], nonces[j]) {
				t.Fatalf("registration attempts %d and %d reused the same nonce", i+1, j+1)
			}
		}
	}
}

func TestRegisterWithRetrySucceedsAfterTransientFailures(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		nonces   [][]byte
		attempts int
	)

	conn := &fakeNATSConn{}
	conn.handler = func(subject string, data []byte) (*strawpb.Envelope, error) {
		if subject != RegistrationSubject() {
			t.Fatalf("subject = %q, want %q", subject, RegistrationSubject())
		}

		env, err := UnmarshalEnvelope(data)
		if err != nil {
			return nil, err
		}

		mu.Lock()
		attempts++
		n := attempts
		nonces = append(nonces, append([]byte(nil), env.GetRegisterRequest().GetNonce()...))
		mu.Unlock()

		reply := &strawpb.Envelope{ProtocolMajor: ProtocolMajor}
		if n >= 3 {
			reply.Payload = &strawpb.Envelope_RegisterAck{RegisterAck: &strawpb.RegisterAck{Ok: true, SessionId: "sess_retry"}}
		}

		return reply, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sessionID, err := registerWithRetry(ctx, conn, runtimeTestIdentity(t, "retry_worker"), Capabilities{MaxConcurrency: 1}, time.Millisecond, 4*time.Millisecond)
	if err != nil {
		t.Fatalf("registerWithRetry: %v", err)
	}

	if sessionID != "sess_retry" {
		t.Fatalf("sessionID = %q, want sess_retry", sessionID)
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}

	assertDistinctNonces(t, nonces)
}

func TestRegisterWithRetryStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	conn := &fakeNATSConn{}
	conn.handler = func(string, []byte) (*strawpb.Envelope, error) {
		return &strawpb.Envelope{ProtocolMajor: ProtocolMajor}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err := registerWithRetry(ctx, conn, runtimeTestIdentity(t, "cancel_worker"), Capabilities{MaxConcurrency: 1}, time.Hour, time.Hour)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("registerWithRetry error = %v, want context.DeadlineExceeded", err)
	}
}

func TestRunReregistersWhenControlForgetsSession(t *testing.T) {
	t.Parallel()

	var (
		mu             sync.Mutex
		nonces         [][]byte
		registrations  int
		liveHeartbeats int
		drainingSeen   bool
	)

	conn := &fakeNATSConn{}
	conn.handler = func(subject string, data []byte) (*strawpb.Envelope, error) {
		env, err := UnmarshalEnvelope(data)
		if err != nil {
			return nil, err
		}

		switch subject {
		case RegistrationSubject():
			mu.Lock()
			registrations++

			sessionID := "sess_dead"
			if registrations > 1 {
				sessionID = "sess_live"
			}

			nonces = append(nonces, append([]byte(nil), env.GetRegisterRequest().GetNonce()...))
			mu.Unlock()

			return &strawpb.Envelope{
				ProtocolMajor: ProtocolMajor,
				Payload:       &strawpb.Envelope_RegisterAck{RegisterAck: &strawpb.RegisterAck{Ok: true, SessionId: sessionID}},
			}, nil
		case HeartbeatSubject():
			hb := env.GetHeartbeatRequest()
			ack := &strawpb.HeartbeatAck{Ok: false, Error: "unknown_worker_session"}
			if hb.GetSessionId() == "sess_live" {
				ack = &strawpb.HeartbeatAck{Ok: true}

				mu.Lock()
				liveHeartbeats++
				drainingSeen = drainingSeen || hb.GetDraining()
				mu.Unlock()
			}

			return &strawpb.Envelope{
				ProtocolMajor: ProtocolMajor,
				Payload:       &strawpb.Envelope_HeartbeatAck{HeartbeatAck: ack},
			}, nil
		default:
			return nil, errors.New("unexpected subject")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := &atomic.Bool{}
	runDone := make(chan error, 1)

	go func() {
		runDone <- Run(ctx, conn, runtimeTestIdentity(t, "forget_worker"), Capabilities{MaxConcurrency: 1}, 5*time.Millisecond, ready, newFakeAssignmentServer)
	}()

	waitForCondition(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return registrations >= 2 && liveHeartbeats >= 1 && ready.Load()
	})

	cancel()

	select {
	case runErr := <-runDone:
		if runErr != nil {
			t.Fatalf("Run returned error = %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	if ready.Load() {
		t.Fatal("ready still true after Run returned")
	}

	mu.Lock()
	defer mu.Unlock()

	if !drainingSeen {
		t.Fatal("final draining heartbeat was not sent")
	}

	assertDistinctNonces(t, nonces)
}

type fakeAssignmentServer struct{}

func newFakeAssignmentServer(string, uint32) (AssignmentServer, error) {
	return fakeAssignmentServer{}, nil
}

func (fakeAssignmentServer) ActiveRequests() uint32 { return 0 }

func (fakeAssignmentServer) Serve(stop <-chan struct{}) error {
	<-stop

	return nil
}

func waitForCondition(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("condition timed out")
}
