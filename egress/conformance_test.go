package egress

// SDK-only conformance test (docs/public/architecture.md): proves the public sdk/egress
// API — Register, Heartbeat, NewWorker/Serve — speaks the real NATS wire
// protocol against a stub executor with no internal/* imports. It plays the
// Control role by hand (subscribe/reply, exact-session assign, c2e/e2c
// streams) against the fake wire broker in conformance_wire_test.go, since a
// genuine *nats.Conn is required for Worker's msg.Respond to work.

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	strawpb "github.com/beremaran/straw-oss/v2/api/proto/straw/v1"
)

const (
	conformanceWorkerID  = "wrk_conformance"
	conformanceSessionID = "sess_conformance"
	conformanceTenantID  = "ten_conformance"
)

// conformanceExecutor is the stub Executor: a static-response page, or a
// scripted executor error when errorMode is set.
type conformanceExecutor struct {
	errorMode bool
}

func (e *conformanceExecutor) Execute(_ context.Context, _ *strawpb.RequestStart, _ []byte, attempt uint32, _ func(*strawpb.StreamFrame)) []*strawpb.StreamFrame {
	if e.errorMode {
		return []*strawpb.StreamFrame{{
			StreamSeq: 1,
			Attempt:   attempt,
			Payload: &strawpb.StreamFrame_Error{Error: &strawpb.ErrorFrame{
				Code:    strawpb.ErrorCode_ERROR_CODE_UPSTREAM_CONNECTION_REFUSED,
				Details: map[string]string{"fact": "conformance_stub_refused"},
			}},
		}}
	}

	return []*strawpb.StreamFrame{
		{StreamSeq: 1, Attempt: attempt, Payload: &strawpb.StreamFrame_ResponseStart{ResponseStart: &strawpb.ResponseStart{
			Status:  200,
			Headers: []*strawpb.Header{{Name: "X-Conformance", Value: []byte("static")}},
		}}},
		{StreamSeq: 2, Attempt: attempt, Payload: &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Offset: 0, Data: []byte("static-page")}}},
		{StreamSeq: 3, Attempt: attempt, Payload: &strawpb.StreamFrame_End{End: &strawpb.EndFrame{Success: true}}},
	}
}

func TestSDKConformanceRegistrationHeartbeatAssignmentStreamAndExecutorError(t *testing.T) {
	t.Parallel()

	srv := newConformanceWireServer(t, 2_000_000)

	controlConn := mustConnectConformanceNATS(t, srv.URL())
	workerConn := mustConnectConformanceNATS(t, srv.URL())

	registerAndHeartbeatOnControl(t, controlConn)

	id := conformanceTestIdentity(t)
	caps := Capabilities{MaxConcurrency: 4, SoftwareVersion: "conformance-test"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sessionID, err := Register(ctx, workerConn, id, caps)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if sessionID != conformanceSessionID {
		t.Fatalf("sessionID = %q, want %q", sessionID, conformanceSessionID)
	}

	err = Heartbeat(ctx, workerConn, id, sessionID, strawpb.WorkerHealth_WORKER_HEALTH_READY, 0, caps.MaxConcurrency, caps.MaxConcurrency, false)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	exec := &conformanceExecutor{}

	worker, err := NewWorker(WorkerOptions{Conn: workerConn, Identity: id, Executor: exec, SessionID: sessionID, MaxConcurrency: caps.MaxConcurrency})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	stop := make(chan struct{})
	serveDone := make(chan error, 1)

	go func() { serveDone <- worker.Serve(stop) }()

	t.Cleanup(func() {
		close(stop)

		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("Worker.Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Worker.Serve did not return after stop")
		}
	})

	assignSubject, err := AssignmentSubject(conformanceWorkerID, sessionID)
	if err != nil {
		t.Fatalf("AssignmentSubject: %v", err)
	}

	frames := runAssignment(t, controlConn, assignSubject, sessionID, "req_conformance_ok")

	if got := frames[0].GetResponseStart().GetStatus(); got != 200 {
		t.Fatalf("first frame ResponseStart status = %d, want 200", got)
	}

	if got := string(frames[1].GetData().GetData()); got != "static-page" {
		t.Fatalf("second frame data = %q, want static-page", got)
	}

	if frames[2].GetEnd() == nil {
		t.Fatalf("third frame = %#v, want EndFrame", frames[2])
	}

	exec.errorMode = true

	errFrames := runAssignment(t, controlConn, assignSubject, sessionID, "req_conformance_error")
	if got := errFrames[0].GetError().GetCode(); got != strawpb.ErrorCode_ERROR_CODE_UPSTREAM_CONNECTION_REFUSED {
		t.Fatalf("executor error frame code = %v, want upstream_connection_refused", got)
	}
}

func conformanceTestIdentity(t *testing.T) Identity {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	return Identity{WorkerID: conformanceWorkerID, CredentialID: "cred_conform", ExecutorType: "egress", PrivateKey: priv}
}

func mustConnectConformanceNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()

	conn, err := nats.Connect(url, nats.MaxReconnects(0))
	if err != nil {
		t.Fatalf("connect fake NATS: %v", err)
	}

	t.Cleanup(conn.Close)

	return conn
}

// registerAndHeartbeatOnControl plays the Control side of registration and
// heartbeat: subscribe, validate the subject, and reply with a fixed session.
func registerAndHeartbeatOnControl(t *testing.T, controlConn *nats.Conn) {
	t.Helper()

	_, err := controlConn.Subscribe(RegistrationSubject(), func(msg *nats.Msg) {
		reply := &strawpb.Envelope{ProtocolMajor: ProtocolMajor, Payload: &strawpb.Envelope_RegisterAck{
			RegisterAck: &strawpb.RegisterAck{Ok: true, SessionId: conformanceSessionID},
		}}

		raw, marshalErr := MarshalEnvelope(reply)
		if marshalErr != nil {
			return
		}

		_ = msg.Respond(raw)
	})
	if err != nil {
		t.Fatalf("subscribe registration: %v", err)
	}

	_, err = controlConn.Subscribe(HeartbeatSubject(), func(msg *nats.Msg) {
		reply := &strawpb.Envelope{ProtocolMajor: ProtocolMajor, Payload: &strawpb.Envelope_HeartbeatAck{
			HeartbeatAck: &strawpb.HeartbeatAck{Ok: true},
		}}

		raw, marshalErr := MarshalEnvelope(reply)
		if marshalErr != nil {
			return
		}

		_ = msg.Respond(raw)
	})
	if err != nil {
		t.Fatalf("subscribe heartbeat: %v", err)
	}

	err = controlConn.Flush()
	if err != nil {
		t.Fatalf("flush control subscriptions: %v", err)
	}
}

// runAssignment plays the Control side of one decoded-HTTP assignment: it
// subscribes to e2c before assigning (per docs/public/architecture.md
// subscription ordering), sends AssignRequest, waits for ACCEPTED, publishes
// RequestStart, and collects the response StreamFrames.
func runAssignment(t *testing.T, controlConn *nats.Conn, assignSubject, sessionID, requestID string) []*strawpb.StreamFrame {
	t.Helper()

	e2cSubject, err := StreamSubject(requestID, conformanceWorkerID, sessionID, DirectionExecutorToControl)
	if err != nil {
		t.Fatalf("e2c StreamSubject: %v", err)
	}

	c2eSubject, err := StreamSubject(requestID, conformanceWorkerID, sessionID, DirectionControlToExecutor)
	if err != nil {
		t.Fatalf("c2e StreamSubject: %v", err)
	}

	received := make(chan *strawpb.StreamFrame, 8)

	e2cSub, err := controlConn.Subscribe(e2cSubject, func(msg *nats.Msg) {
		env, decodeErr := UnmarshalEnvelope(msg.Data)
		if decodeErr != nil {
			return
		}

		received <- env.GetStreamFrame()
	})
	if err != nil {
		t.Fatalf("subscribe e2c: %v", err)
	}

	t.Cleanup(func() { _ = e2cSub.Unsubscribe() })

	err = controlConn.Flush()
	if err != nil {
		t.Fatalf("flush e2c subscription: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)

	assign := &strawpb.AssignRequest{
		Mode:                       strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP,
		DeadlineUnixMs:             deadline.UnixMilli(),
		Attempt:                    1,
		InitialUploadCreditBytes:   1 << 20,
		InitialDownloadCreditBytes: 1 << 20,
	}

	assignEnv := &strawpb.Envelope{
		RequestId:      requestID,
		TenantId:       conformanceTenantID,
		DeadlineUnixMs: deadline.UnixMilli(),
		ProtocolMajor:  ProtocolMajor,
		Attempt:        1,
		Payload:        &strawpb.Envelope_AssignRequest{AssignRequest: assign},
	}

	assignRaw, err := MarshalEnvelope(assignEnv)
	if err != nil {
		t.Fatalf("marshal AssignRequest: %v", err)
	}

	ack := waitForAssignAck(t, controlConn, assignSubject, assignRaw)
	if ack.GetCode() != strawpb.AssignAckCode_ASSIGN_ACK_ACCEPTED {
		t.Fatalf("AssignAck code = %v, want ACCEPTED", ack.GetCode())
	}

	start := &strawpb.StreamFrame{
		StreamSeq: 1,
		Attempt:   1,
		Payload: &strawpb.StreamFrame_RequestStart{RequestStart: &strawpb.RequestStart{
			Mode:              strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP,
			Method:            "GET",
			Url:               "http://example.invalid/conformance",
			DeadlineUnixMs:    deadline.UnixMilli(),
			RedirectPolicy:    strawpb.RedirectPolicy_REDIRECT_POLICY_NO_FOLLOW,
			DestinationPolicy: &strawpb.DestinationPolicy{ResolutionMode: strawpb.DestinationResolutionMode_DESTINATION_RESOLUTION_DIRECT_LOCAL},
		}},
	}

	startEnv := &strawpb.Envelope{
		RequestId:      requestID,
		TenantId:       conformanceTenantID,
		DeadlineUnixMs: deadline.UnixMilli(),
		ProtocolMajor:  ProtocolMajor,
		Attempt:        1,
		Payload:        &strawpb.Envelope_StreamFrame{StreamFrame: start},
	}

	startRaw, err := MarshalEnvelope(startEnv)
	if err != nil {
		t.Fatalf("marshal RequestStart: %v", err)
	}

	err = controlConn.Publish(c2eSubject, startRaw)
	if err != nil {
		t.Fatalf("publish RequestStart: %v", err)
	}

	return collectUntilTerminal(t, received)
}

// waitForAssignAck retries the exact-session assign request until the
// executor's assignment subscription is live; the fake broker delivers
// reliably, so a request only fails while Worker.Serve hasn't subscribed yet.
func waitForAssignAck(t *testing.T, controlConn *nats.Conn, subject string, raw []byte) *strawpb.AssignAck {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := controlConn.Request(subject, raw, 100*time.Millisecond)
		if err == nil {
			env, decodeErr := UnmarshalEnvelope(msg.Data)
			if decodeErr != nil {
				t.Fatalf("unmarshal AssignAck: %v", decodeErr)
			}

			ack := env.GetAssignAck()
			if ack == nil {
				t.Fatalf("assign reply carried no AssignAck: %#v", env)
			}

			return ack
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("assign request to %s never got a response", subject)

	return nil
}

func collectUntilTerminal(t *testing.T, received <-chan *strawpb.StreamFrame) []*strawpb.StreamFrame {
	t.Helper()

	var frames []*strawpb.StreamFrame

	deadline := time.After(5 * time.Second)

	for {
		select {
		case frame := <-received:
			frames = append(frames, frame)

			switch frame.GetPayload().(type) {
			case *strawpb.StreamFrame_End, *strawpb.StreamFrame_Error, *strawpb.StreamFrame_Cancelled:
				return frames
			}
		case <-deadline:
			t.Fatalf("timed out waiting for terminal frame, got %d frames", len(frames))

			return frames
		}
	}
}
