package egress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	strawpb "github.com/beremaran/straw-oss/api/proto/straw/v1"
)

const (
	sdkTestWorker           = "sdk_worker"
	sdkTestSession          = "sdk_session"
	assignmentTestChrome120 = "chrome_120"
	testTunnelHost          = "tunnel.test"
	testCancel              = "client_disconnect"
)

type assignmentFakeConn struct {
	mu          sync.Mutex
	subscribed  []string
	flushes     int
	published   []*strawpb.StreamFrame
	publishCh   chan *strawpb.StreamFrame
	subHandlers map[string]nats.MsgHandler
}

func newAssignmentFakeConn() *assignmentFakeConn {
	return &assignmentFakeConn{publishCh: make(chan *strawpb.StreamFrame, 16), subHandlers: make(map[string]nats.MsgHandler)}
}

func (c *assignmentFakeConn) Request(string, []byte, time.Duration) (*nats.Msg, error) {
	return nil, errors.New("request not implemented")
}

func (c *assignmentFakeConn) Subscribe(subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscribed = append(c.subscribed, subject)
	c.subHandlers[subject] = cb

	return &nats.Subscription{}, nil
}

func (c *assignmentFakeConn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushes++

	return nil
}

func (c *assignmentFakeConn) Publish(_ string, raw []byte) error {
	env, err := UnmarshalEnvelope(raw)
	if err != nil {
		return err
	}
	frame := env.GetStreamFrame()
	c.mu.Lock()
	c.published = append(c.published, frame)
	c.mu.Unlock()
	c.publishCh <- frame

	return nil
}

func (c *assignmentFakeConn) nextFrame(t *testing.T) *strawpb.StreamFrame {
	t.Helper()
	select {
	case frame := <-c.publishCh:
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published frame")

		return nil
	}
}

type scriptedExecutor struct {
	result          []*strawpb.StreamFrame
	stream          []*strawpb.StreamFrame
	block           chan struct{}
	started         chan struct{}
	outboundProfile string
}

func (e *scriptedExecutor) Execute(ctx context.Context, _ *strawpb.RequestStart, _ []byte, attempt uint32, send func(*strawpb.StreamFrame)) []*strawpb.StreamFrame {
	if e.started != nil {
		close(e.started)
	}
	if send != nil {
		send(&strawpb.StreamFrame{Attempt: attempt, Payload: &strawpb.StreamFrame_OutboundStart{OutboundStart: &strawpb.OutboundStartFrame{TargetHost: "example.com", TargetPort: 80, ExecutedFingerprintProfile: e.outboundProfile}}})
		for _, frame := range e.stream {
			send(frame)
		}
	}
	if e.block != nil {
		select {
		case <-ctx.Done():
			return []*strawpb.StreamFrame{{Attempt: attempt, Payload: &strawpb.StreamFrame_Error{Error: &strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_CANCELLED}}}}
		case <-e.block:
		}
	}

	return e.result
}

type fakeTunnelOpener struct {
	conn   net.Conn
	target TunnelTarget
	err    *strawpb.ErrorFrame
}

func (o fakeTunnelOpener) OpenTunnel(context.Context, *strawpb.RequestStart) (net.Conn, TunnelTarget, *strawpb.ErrorFrame) {
	return o.conn, o.target, o.err
}

func TestSDKWorkerServeSubscribesAndFlushesAssignmentSubject(t *testing.T) {
	t.Parallel()

	conn := newAssignmentFakeConn()
	worker, err := NewWorker(WorkerOptions{Conn: conn, Identity: Identity{WorkerID: sdkTestWorker}, Executor: &scriptedExecutor{}, SessionID: sdkTestSession})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- worker.Serve(stop) }()

	want, err := AssignmentSubject(sdkTestWorker, sdkTestSession)
	if err != nil {
		t.Fatalf("AssignmentSubject: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()

		return len(conn.subscribed) == 1 && conn.subscribed[0] == want && conn.flushes == 1
	})
	close(stop)

	err = <-done
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

func TestSDKAssignmentAdmissionAcceptsAndRejects(t *testing.T) {
	t.Parallel()

	req := &strawpb.AssignRequest{
		Mode:           strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP,
		DeadlineUnixMs: time.Now().Add(time.Second).UnixMilli(),
		Attempt:        1,
	}

	if got := EvaluateAssignment(req, Capacity{MaxConcurrency: 2, ActiveRequests: 1, SupportedModes: []strawpb.RequestMode{strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP}}); got != strawpb.AssignAckCode_ASSIGN_ACK_ACCEPTED {
		t.Fatalf("accepted assignment code = %v", got)
	}

	if got := EvaluateAssignment(req, Capacity{MaxConcurrency: 1, ActiveRequests: 1}); got != strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_CAPACITY {
		t.Fatalf("capacity rejection code = %v", got)
	}

	if got := EvaluateAssignment(req, Capacity{Draining: true}); got != strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_DRAINING {
		t.Fatalf("draining rejection code = %v", got)
	}

	if got := EvaluateAssignment(req, Capacity{SupportedModes: []strawpb.RequestMode{strawpb.RequestMode_REQUEST_MODE_RAW_TUNNEL}}); got != strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_UNSUPPORTED {
		t.Fatalf("unsupported rejection code = %v", got)
	}
}

func TestSDKStreamValidatorRejectsSequenceGapAndAcceptsDuplicate(t *testing.T) {
	t.Parallel()

	validator := newStreamValidator(streamValidatorOptions{attempt: 1, initialCredit: 4})
	first := &strawpb.StreamFrame{StreamSeq: 1, Attempt: 1, Payload: &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Offset: 0, Data: []byte("a")}}}

	if got := validator.accept(first); got != frameAccepted {
		t.Fatalf("first frame outcome = %v", got)
	}

	duplicate := &strawpb.StreamFrame{StreamSeq: 1, Attempt: 1, Payload: &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Offset: 0, Data: []byte("a")}}}
	if got := validator.accept(duplicate); got != frameDuplicate {
		t.Fatalf("duplicate frame outcome = %v", got)
	}

	gap := &strawpb.StreamFrame{StreamSeq: 3, Attempt: 1, Payload: &strawpb.StreamFrame_End{End: &strawpb.EndFrame{Success: true}}}
	if got := validator.accept(gap); got != frameSequenceGap {
		t.Fatalf("gap frame outcome = %v", got)
	}
}

func TestSDKDecodedRuntimeStreamsResponseAndHonorsDownloadCredit(t *testing.T) {
	t.Parallel()

	conn := newAssignmentFakeConn()
	worker, err := NewWorker(WorkerOptions{
		Conn:      conn,
		Identity:  Identity{WorkerID: sdkTestWorker},
		Executor:  &scriptedExecutor{stream: []*strawpb.StreamFrame{{Attempt: 1, Payload: &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Data: []byte("ab")}}}}, result: []*strawpb.StreamFrame{{Attempt: 1, Payload: &strawpb.StreamFrame_End{End: &strawpb.EndFrame{Success: true}}}}},
		SessionID: sdkTestSession,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	req := &strawpb.AssignRequest{Attempt: 1, InitialUploadCreditBytes: 1 << 20, InitialDownloadCreditBytes: 1}
	env := assignmentEnvelope(req)
	frames := make(chan *strawpb.StreamFrame, 4)
	c2e, _ := StreamSubject("sdk_req", sdkTestWorker, sdkTestSession, DirectionControlToExecutor)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		worker.runDecodedRequest(ctx, cancel, req, env, frames, "e2c")
		close(done)
	}()
	frames <- requestStartFrame(1, "http://example.com/")

	first := conn.nextFrame(t)
	if first.GetOutboundStart() == nil {
		t.Fatalf("first frame = %#v, want OutboundStart", first)
	}
	data := conn.nextFrame(t)
	if got := string(data.GetData().GetData()); got != "a" {
		t.Fatalf("data = %q, want a", got)
	}
	select {
	case frame := <-conn.publishCh:
		t.Fatalf("unexpected frame without credit on %s: %#v", c2e, frame)
	case <-time.After(50 * time.Millisecond):
	}
	frames <- &strawpb.StreamFrame{StreamSeq: 2, Attempt: 1, Payload: &strawpb.StreamFrame_Credit{Credit: &strawpb.CreditFrame{DownloadCreditBytes: 1}}}
	if got := string(conn.nextFrame(t).GetData().GetData()); got != "b" {
		t.Fatalf("data = %q, want b", got)
	}
	if conn.nextFrame(t).GetEnd() == nil {
		t.Fatal("missing EndFrame")
	}
	<-done
}

func TestSDKDecodedRuntimePreservesExecutedFingerprintOnWire(t *testing.T) {
	t.Parallel()

	conn := newAssignmentFakeConn()
	worker, err := NewWorker(WorkerOptions{
		Conn:      conn,
		Identity:  Identity{WorkerID: sdkTestWorker},
		Executor:  &scriptedExecutor{outboundProfile: assignmentTestChrome120},
		SessionID: sdkTestSession,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	req := &strawpb.AssignRequest{Attempt: 1, InitialUploadCreditBytes: 1 << 20, InitialDownloadCreditBytes: 1 << 20}
	env := assignmentEnvelope(req)
	frames := make(chan *strawpb.StreamFrame, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		worker.runDecodedRequest(ctx, cancel, req, env, frames, "e2c")
		close(done)
	}()
	frames <- requestStartFrame(1, "http://example.com/")

	first := conn.nextFrame(t)
	if got := first.GetOutboundStart().GetExecutedFingerprintProfile(); got != assignmentTestChrome120 {
		t.Fatalf("executed fingerprint = %q, want chrome_120", got)
	}
	frames <- &strawpb.StreamFrame{StreamSeq: 2, Attempt: 1, Payload: &strawpb.StreamFrame_Cancel{Cancel: &strawpb.CancelFrame{Reason: testCancel}}}
	<-done
}

func TestSDKDecodedRuntimeCancellationAndExecutorError(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	conn := newAssignmentFakeConn()
	worker, err := NewWorker(WorkerOptions{Conn: conn, Identity: Identity{WorkerID: sdkTestWorker}, Executor: &scriptedExecutor{started: started, block: make(chan struct{})}, SessionID: sdkTestSession})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	req := &strawpb.AssignRequest{Attempt: 1, InitialUploadCreditBytes: 1 << 20, InitialDownloadCreditBytes: 1 << 20}
	env := assignmentEnvelope(req)
	frames := make(chan *strawpb.StreamFrame, 4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		worker.runDecodedRequest(ctx, cancel, req, env, frames, "e2c")
		close(done)
	}()
	frames <- requestStartFrame(1, "http://example.com/")
	<-started
	frames <- &strawpb.StreamFrame{StreamSeq: 2, Attempt: 1, Payload: &strawpb.StreamFrame_Cancel{Cancel: &strawpb.CancelFrame{Reason: testCancel}}}

	var cancelled *strawpb.StreamFrame
	for cancelled == nil {
		frame := conn.nextFrame(t)
		if frame.GetCancelled() != nil {
			cancelled = frame
		}
	}
	if got := cancelled.GetCancelled().GetReason(); got != testCancel {
		t.Fatalf("cancel reason = %q", got)
	}
	<-done

	conn = newAssignmentFakeConn()
	worker.conn = conn
	worker.executor = &scriptedExecutor{result: []*strawpb.StreamFrame{{Attempt: 1, Payload: &strawpb.StreamFrame_Error{Error: &strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_UPSTREAM_DNS_FAILURE}}}}}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	worker.runDecodedRequest(ctx, cancel, req, env, framesWith(requestStartFrame(1, "http://example.com/")), "e2c")
	var errFrame *strawpb.ErrorFrame
	for errFrame == nil {
		errFrame = conn.nextFrame(t).GetError()
	}
	if got := errFrame.GetCode(); got != strawpb.ErrorCode_ERROR_CODE_UPSTREAM_DNS_FAILURE {
		t.Fatalf("error code = %v, want upstream_dns_failure", got)
	}
}

func TestSDKRawTunnelRuntimeDataFlowAndUploadCredit(t *testing.T) {
	t.Parallel()

	local, remote := net.Pipe()
	defer func() { _ = remote.Close() }()

	conn := newAssignmentFakeConn()
	worker, err := NewWorker(WorkerOptions{
		Conn:      conn,
		Identity:  Identity{WorkerID: sdkTestWorker},
		Executor:  &scriptedExecutor{},
		Tunnels:   fakeTunnelOpener{conn: local, target: TunnelTarget{Host: testTunnelHost, Port: 443}},
		SessionID: sdkTestSession,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	req := &strawpb.AssignRequest{Attempt: 1, InitialUploadCreditBytes: 1 << 20, InitialDownloadCreditBytes: 1 << 20}
	env := assignmentEnvelope(req)
	frames := make(chan *strawpb.StreamFrame, 4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		worker.runDecodedRequest(ctx, cancel, req, env, frames, "e2c")
		close(done)
	}()

	frames <- rawTunnelStartFrame(1)
	if frame := conn.nextFrame(t); frame.GetOutboundStart().GetTargetHost() != testTunnelHost {
		t.Fatalf("first frame = %#v, want tunnel OutboundStart", frame)
	}
	if frame := conn.nextFrame(t); frame.GetResponseStart().GetStatus() != 200 {
		t.Fatalf("second frame = %#v, want 200 ResponseStart", frame)
	}

	frames <- &strawpb.StreamFrame{StreamSeq: 2, Attempt: 1, Payload: &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Offset: 0, Data: []byte("up")}}}
	buf := make([]byte, 2)
	_, err = io.ReadFull(remote, buf)
	if err != nil {
		t.Fatalf("read tunnel upload: %v", err)
	}
	if string(buf) != "up" {
		t.Fatalf("tunnel upload = %q, want up", buf)
	}
	if frame := conn.nextFrame(t); frame.GetCredit().GetUploadCreditBytes() != 2 {
		t.Fatalf("upload credit frame = %#v, want 2 bytes", frame)
	}

	go func() { _, _ = remote.Write([]byte("down")) }()
	if frame := nextPublishedData(t, conn); string(frame.GetData().GetData()) != "down" {
		t.Fatalf("download frame = %#v, want down data", frame)
	}
	_ = remote.Close()
	if frame := nextPublishedEnd(t, conn); frame.GetEnd() == nil {
		t.Fatalf("terminal frame = %#v, want EndFrame", frame)
	}
	<-done
}

func TestSDKRawTunnelRuntimeDownloadCreditAndCancellation(t *testing.T) {
	t.Parallel()

	local, remote := net.Pipe()
	defer func() { _ = remote.Close() }()

	conn := newAssignmentFakeConn()
	worker, err := NewWorker(WorkerOptions{
		Conn:      conn,
		Identity:  Identity{WorkerID: sdkTestWorker},
		Executor:  &scriptedExecutor{},
		Tunnels:   fakeTunnelOpener{conn: local, target: TunnelTarget{Host: testTunnelHost, Port: 443}},
		SessionID: sdkTestSession,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	req := &strawpb.AssignRequest{Attempt: 1, InitialUploadCreditBytes: 1 << 20, InitialDownloadCreditBytes: 1}
	env := assignmentEnvelope(req)
	frames := make(chan *strawpb.StreamFrame, 4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		worker.runDecodedRequest(ctx, cancel, req, env, frames, "e2c")
		close(done)
	}()

	frames <- rawTunnelStartFrame(1)
	_ = conn.nextFrame(t)
	_ = conn.nextFrame(t)

	go func() { _, _ = remote.Write([]byte("ab")) }()
	if got := string(nextPublishedData(t, conn).GetData().GetData()); got != "a" {
		t.Fatalf("first download = %q, want a", got)
	}
	select {
	case frame := <-conn.publishCh:
		t.Fatalf("unexpected frame before credit: %#v", frame)
	case <-time.After(50 * time.Millisecond):
	}
	frames <- &strawpb.StreamFrame{StreamSeq: 2, Attempt: 1, Payload: &strawpb.StreamFrame_Credit{Credit: &strawpb.CreditFrame{DownloadCreditBytes: 1}}}
	if got := string(nextPublishedData(t, conn).GetData().GetData()); got != "b" {
		t.Fatalf("second download = %q, want b", got)
	}

	frames <- &strawpb.StreamFrame{StreamSeq: 3, Attempt: 1, Payload: &strawpb.StreamFrame_Cancel{Cancel: &strawpb.CancelFrame{Reason: testCancel}}}
	if got := nextPublishedCancelled(t, conn).GetCancelled().GetReason(); got != testCancel {
		t.Fatalf("cancel reason = %q, want %s", got, testCancel)
	}
	<-done
}

func assignmentEnvelope(req *strawpb.AssignRequest) *strawpb.Envelope {
	return &strawpb.Envelope{
		RequestId:      "sdk_req",
		DeploymentId:   "ten_sdk",
		DeadlineUnixMs: time.Now().Add(time.Second).UnixMilli(),
		Attempt:        req.GetAttempt(),
		Payload:        &strawpb.Envelope_AssignRequest{AssignRequest: req},
	}
}

func requestStartFrame(seq uint64, url string) *strawpb.StreamFrame {
	return &strawpb.StreamFrame{StreamSeq: seq, Attempt: 1, Payload: &strawpb.StreamFrame_RequestStart{RequestStart: &strawpb.RequestStart{Mode: strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP, Method: http.MethodGet, Url: url}}}
}

func rawTunnelStartFrame(seq uint64) *strawpb.StreamFrame {
	return &strawpb.StreamFrame{StreamSeq: seq, Attempt: 1, Payload: &strawpb.StreamFrame_RequestStart{RequestStart: &strawpb.RequestStart{
		Mode:              strawpb.RequestMode_REQUEST_MODE_RAW_TUNNEL,
		Method:            http.MethodConnect,
		Url:               "connect://" + testTunnelHost + ":443",
		RedirectPolicy:    strawpb.RedirectPolicy_REDIRECT_POLICY_NO_FOLLOW,
		DestinationPolicy: &strawpb.DestinationPolicy{ResolutionMode: strawpb.DestinationResolutionMode_DESTINATION_RESOLUTION_DIRECT_LOCAL},
	}}}
}

func nextPublishedData(t *testing.T, conn *assignmentFakeConn) *strawpb.StreamFrame {
	t.Helper()

	for {
		frame := conn.nextFrame(t)
		if frame.GetData() != nil {
			return frame
		}
	}
}

func nextPublishedEnd(t *testing.T, conn *assignmentFakeConn) *strawpb.StreamFrame {
	t.Helper()

	for {
		frame := conn.nextFrame(t)
		if frame.GetEnd() != nil {
			return frame
		}
	}
}

func nextPublishedCancelled(t *testing.T, conn *assignmentFakeConn) *strawpb.StreamFrame {
	t.Helper()

	for {
		frame := conn.nextFrame(t)
		if frame.GetCancelled() != nil {
			return frame
		}
	}
}

func framesWith(frames ...*strawpb.StreamFrame) chan *strawpb.StreamFrame {
	ch := make(chan *strawpb.StreamFrame, len(frames))
	for _, frame := range frames {
		ch <- frame
	}

	return ch
}
