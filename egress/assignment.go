package egress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	strawpb "github.com/beremaran/straw-oss/v2/api/proto/straw/v1"
)

const (
	frameIdleTimeout         = 15 * time.Second
	idleCheckInterval        = 500 * time.Millisecond
	workerDrainTimeout       = 60 * time.Second
	streamFrameChannelBuffer = 32
	responseFrameDataBytes   = 32 << 10
	rawTunnelEstablished     = 200
	errorFactDetailKey       = "fact"
)

var errExecutorRequired = errors.New("executor is required")

// Worker runs the public SDK assignment stream runtime for one registered session.
type Worker struct {
	conn      NATSConn
	id        Identity
	executor  Executor
	bodyRefs  BodyRefResolver
	tunnels   TunnelOpener
	sessionID string

	maxConcurrency uint32
	supportedModes []strawpb.RequestMode

	mu       sync.Mutex
	active   uint32
	draining bool
	cancels  map[string]context.CancelFunc

	wg sync.WaitGroup
}

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	Conn           NATSConn
	Identity       Identity
	Executor       Executor
	BodyRefs       BodyRefResolver
	Tunnels        TunnelOpener
	SessionID      string
	MaxConcurrency uint32
	SupportedModes []strawpb.RequestMode
}

// NewWorker builds a Worker bound to a registered session.
func NewWorker(opts WorkerOptions) (*Worker, error) {
	if opts.Conn == nil {
		return nil, errConnRequired
	}

	if opts.Executor == nil {
		return nil, errExecutorRequired
	}

	err := ValidateSubjectToken(opts.SessionID)
	if err != nil {
		return nil, fmt.Errorf("session_id: %w", err)
	}

	modes := opts.SupportedModes
	if len(modes) == 0 {
		modes = []strawpb.RequestMode{strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP}
	}

	return &Worker{
		conn:           opts.Conn,
		id:             opts.Identity,
		executor:       opts.Executor,
		bodyRefs:       opts.BodyRefs,
		tunnels:        opts.Tunnels,
		sessionID:      opts.SessionID,
		maxConcurrency: opts.MaxConcurrency,
		supportedModes: modes,
		cancels:        make(map[string]context.CancelFunc),
	}, nil
}

// ActiveRequests reports the number of assignments currently executing.
func (w *Worker) ActiveRequests() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.active
}

// SetDraining applies a runtime-administration lifecycle decision. Existing
// requests continue; new assignments are rejected.
func (w *Worker) SetDraining(draining bool) {
	w.mu.Lock()
	w.draining = draining
	w.mu.Unlock()
}

// Draining reports the current runtime-administration lifecycle decision.
func (w *Worker) Draining() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.draining
}

// Serve subscribes to the exact-session assignment subject until stop closes.
func (w *Worker) Serve(stop <-chan struct{}) error {
	subject, err := AssignmentSubject(w.id.WorkerID, w.sessionID)
	if err != nil {
		return fmt.Errorf("assignment subject: %w", err)
	}

	sub, err := w.conn.Subscribe(subject, w.handleAssign)
	if err != nil {
		return fmt.Errorf("subscribe assignment: %w", err)
	}

	err = w.conn.Flush()
	if err != nil {
		_ = sub.Unsubscribe()

		return fmt.Errorf("flush assignment subscription: %w", err)
	}

	<-stop
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()

	_ = sub.Unsubscribe()

	drained := make(chan struct{})

	go func() {
		w.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(workerDrainTimeout):
		w.abandonInFlight()
		<-drained
	}

	return nil
}

func (w *Worker) abandonInFlight() {
	w.mu.Lock()

	cancels := make([]context.CancelFunc, 0, len(w.cancels))
	for _, cancel := range w.cancels {
		cancels = append(cancels, cancel)
	}
	w.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (w *Worker) handleAssign(msg *nats.Msg) {
	env, decodeErr := UnmarshalEnvelope(msg.Data)
	if decodeErr != nil {
		w.reply(msg, nil, &strawpb.AssignAck{Code: strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_ERROR})

		return
	}

	req := env.GetAssignRequest()

	code := EvaluateAssignment(req, w.snapshotCapacity())
	if code != strawpb.AssignAckCode_ASSIGN_ACK_ACCEPTED {
		w.reply(msg, env, &strawpb.AssignAck{Code: code})

		return
	}

	requestID := env.GetRequestId()

	e2cSubject, c2eSub, frames, ok := w.prepareRequestStream(env)
	if !ok {
		w.reply(msg, env, &strawpb.AssignAck{Code: strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_ERROR})

		return
	}

	reqCtx, cancel := requestContext(env.GetDeadlineUnixMs())
	w.reserve(requestID, cancel)
	w.reply(msg, env, &strawpb.AssignAck{Code: strawpb.AssignAckCode_ASSIGN_ACK_ACCEPTED})

	w.wg.Go(func() {
		defer func() { _ = c2eSub.Unsubscribe() }()
		defer w.release(requestID)

		w.runDecodedRequest(reqCtx, cancel, req, env, frames, e2cSubject)
	})
}

func (w *Worker) prepareRequestStream(env *strawpb.Envelope) (string, *nats.Subscription, chan *strawpb.StreamFrame, bool) {
	requestID := env.GetRequestId()

	c2eSubject, err := StreamSubject(requestID, w.id.WorkerID, w.sessionID, DirectionControlToExecutor)
	if err != nil {
		return "", nil, nil, false
	}

	e2cSubject, err := StreamSubject(requestID, w.id.WorkerID, w.sessionID, DirectionExecutorToControl)
	if err != nil {
		return "", nil, nil, false
	}

	frames := make(chan *strawpb.StreamFrame, streamFrameChannelBuffer)

	c2eSub, err := w.conn.Subscribe(c2eSubject, func(m *nats.Msg) { frames <- decodeStreamFrame(m.Data) })
	if err != nil {
		return "", nil, nil, false
	}

	err = w.conn.Flush()
	if err != nil {
		_ = c2eSub.Unsubscribe()

		return "", nil, nil, false
	}

	return e2cSubject, c2eSub, frames, true
}

func (w *Worker) snapshotCapacity() Capacity {
	w.mu.Lock()
	defer w.mu.Unlock()

	return Capacity{Draining: w.draining, ActiveRequests: w.active, MaxConcurrency: w.maxConcurrency, SupportedModes: w.supportedModes}
}

func (w *Worker) reserve(requestID string, cancel context.CancelFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.active++
	w.cancels[requestID] = cancel
}

func (w *Worker) release(requestID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.active--
	delete(w.cancels, requestID)
}

func requestContext(deadlineUnixMs int64) (context.Context, context.CancelFunc) {
	if deadlineUnixMs <= 0 {
		return context.WithCancel(context.Background())
	}

	return context.WithDeadline(context.Background(), time.UnixMilli(deadlineUnixMs))
}

func (w *Worker) runDecodedRequest(ctx context.Context, cancel context.CancelFunc, req *strawpb.AssignRequest, env *strawpb.Envelope, frames <-chan *strawpb.StreamFrame, e2cSubject string) {
	defer cancel()

	validator := newStreamValidator(streamValidatorOptions{
		attempt:       req.GetAttempt(),
		initialCredit: req.GetInitialUploadCreditBytes(),
		idleTimeout:   frameIdleTimeout,
		allowBodyRef:  w.bodyRefs != nil,
	})

	start, body, failure, ok := w.readRequestBody(ctx, validator, frames, req.GetExpectedUploadBytes(), env.GetTenantId(), env.GetRequestId())
	if !ok {
		return
	}

	if failure != nil {
		w.publish(e2cSubject, env, []*strawpb.StreamFrame{errorFrame(req.GetAttempt(), failure)})

		return
	}

	if start.GetMode() != strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP {
		if start.GetMode() == strawpb.RequestMode_REQUEST_MODE_RAW_TUNNEL && w.tunnels != nil {
			w.runRawTunnel(ctx, cancel, req, env, start, frames, validator, e2cSubject)

			return
		}

		w.publish(e2cSubject, env, []*strawpb.StreamFrame{errorFrame(req.GetAttempt(), &strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_UNSUPPORTED_INGRESS_MODE, Details: map[string]string{errorFactDetailKey: "unsupported_request_mode"}})})

		return
	}

	resultCh, downloadCredit := w.startDecodedExecution(ctx, req, env, start, body, e2cSubject)

	result, canceled, reason := waitForResult(resultCh, frames, validator, cancel, downloadCredit)
	if canceled {
		result = applyCancellation(result, reason)
	}

	w.publish(e2cSubject, env, result)
}

func (w *Worker) startDecodedExecution(ctx context.Context, req *strawpb.AssignRequest, env *strawpb.Envelope, start *strawpb.RequestStart, body []byte, e2cSubject string) (<-chan []*strawpb.StreamFrame, *responseCreditGate) {
	resultCh := make(chan []*strawpb.StreamFrame, 1)
	downloadCredit := newResponseCreditGate(req.GetInitialDownloadCreditBytes())

	go func() {
		seq := uint64(0)

		send := func(frame *strawpb.StreamFrame) {
			seq = w.publishResponseProgress(ctx, e2cSubject, env, downloadCredit, seq, frame)
		}

		if exec, ok := w.executor.(DeploymentExecutor); ok {
			resultCh <- exec.ExecuteWithDeployment(ctx, env.GetTenantId(), start, body, req.GetAttempt(), send)

			return
		}

		resultCh <- w.executor.Execute(ctx, start, body, req.GetAttempt(), send)
	}()

	return resultCh, downloadCredit
}

func (w *Worker) publishResponseProgress(ctx context.Context, subject string, env *strawpb.Envelope, credit *responseCreditGate, seq uint64, frame *strawpb.StreamFrame) uint64 {
	if data := frame.GetData(); data != nil {
		return w.publishResponseData(ctx, subject, env, credit, seq, frame.GetAttempt(), data)
	}

	seq++
	frame.StreamSeq = seq
	w.publish(subject, env, []*strawpb.StreamFrame{frame})

	return seq
}

func (w *Worker) publishResponseData(ctx context.Context, subject string, env *strawpb.Envelope, credit *responseCreditGate, seq uint64, attempt uint32, data *strawpb.DataFrame) uint64 {
	offset := data.GetOffset()
	remaining := data.GetData()

	for len(remaining) > 0 {
		n, ok := credit.takeAvailable(ctx, min(len(remaining), responseFrameDataBytes))
		if !ok {
			return seq
		}

		seq++
		chunk := remaining[:n]
		w.publish(subject, env, []*strawpb.StreamFrame{{
			StreamSeq: seq,
			Attempt:   attempt,
			Payload:   &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Offset: offset, Data: append([]byte(nil), chunk...)}},
		}})
		offset += uint64(len(chunk))
		remaining = remaining[n:]
	}

	return seq
}

func (w *Worker) runRawTunnel(ctx context.Context, cancel context.CancelFunc, req *strawpb.AssignRequest, env *strawpb.Envelope, start *strawpb.RequestStart, frames <-chan *strawpb.StreamFrame, validator *streamValidator, e2cSubject string) {
	defer cancel()

	conn, target, failure := w.tunnels.OpenTunnel(ctx, start)
	builder := newTunnelFrameBuilder(req.GetAttempt())

	if failure != nil {
		w.publish(e2cSubject, env, []*strawpb.StreamFrame{builder.outboundStart(target), builder.error(failure)})

		return
	}

	defer func() { _ = conn.Close() }()

	stream := rawTunnelStream{
		worker:    w,
		env:       env,
		subject:   e2cSubject,
		conn:      conn,
		builder:   builder,
		validator: validator,
		credit:    newResponseCreditGate(req.GetInitialDownloadCreditBytes()),
	}
	stream.publish(builder.outboundStart(target), builder.responseStart())
	stream.run(ctx, frames)
}

type rawTunnelStream struct {
	worker    *Worker
	env       *strawpb.Envelope
	subject   string
	conn      net.Conn
	builder   *tunnelFrameBuilder
	validator *streamValidator
	credit    *responseCreditGate
}

func (s rawTunnelStream) run(ctx context.Context, frames <-chan *strawpb.StreamFrame) {
	done := make(chan error, 1)

	go streamTunnelDownload(ctx, s.conn, s.credit, s.builder, s.publishOne, done)

	ticker := time.NewTicker(idleCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			s.handleDone(err)

			return
		case <-ctx.Done():
			s.publishOne(s.builder.error(&strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_TIMEOUT_EXCEEDED, Details: map[string]string{errorFactDetailKey: "request_cancelled"}}))

			return
		case <-ticker.C:
			if s.validator.idleExpired() {
				return
			}
		case frame := <-frames:
			if s.handleFrame(ctx, frame) {
				return
			}
		}
	}
}

func (s rawTunnelStream) handleDone(err error) {
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		s.publishOne(s.builder.error(&strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_UPSTREAM_RESET, Details: map[string]string{errorFactDetailKey: "upstream_reset_before_headers"}}))

		return
	}

	s.publishOne(s.builder.end())
}

func (s rawTunnelStream) handleFrame(ctx context.Context, frame *strawpb.StreamFrame) bool {
	if s.validator.accept(frame) != frameAccepted {
		return false
	}

	if data := frame.GetData(); data != nil {
		return s.handleData(ctx, data)
	}

	if credit := frame.GetCredit(); credit != nil {
		s.credit.grant(credit.GetDownloadCreditBytes())

		return false
	}

	if cancelFrame := frame.GetCancel(); cancelFrame != nil {
		s.publishOne(s.builder.cancelled(cancelFrame.GetReason()))

		return true
	}

	return false
}

func (s rawTunnelStream) handleData(_ context.Context, data *strawpb.DataFrame) bool {
	_, err := s.conn.Write(data.GetData())
	if err != nil {
		s.publishOne(s.builder.error(&strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_UPSTREAM_RESET, Details: map[string]string{errorFactDetailKey: err.Error()}}))

		return true
	}

	s.publishOne(s.builder.uploadCredit(uint64FromInt(len(data.GetData()))))

	return false
}

func (s rawTunnelStream) publish(frames ...*strawpb.StreamFrame) {
	s.worker.publish(s.subject, s.env, frames)
}

func (s rawTunnelStream) publishOne(frame *strawpb.StreamFrame) {
	s.publish(frame)
}

func streamTunnelDownload(ctx context.Context, conn net.Conn, credit *responseCreditGate, builder *tunnelFrameBuilder, publish func(*strawpb.StreamFrame), done chan<- error) {
	buf := make([]byte, responseFrameDataBytes)
	offset := uint64(0)

	for {
		n, err := conn.Read(buf)
		if n > 0 {
			remaining := buf[:n]
			for len(remaining) > 0 {
				taken, ok := credit.takeAvailable(ctx, len(remaining))
				if !ok {
					done <- ctx.Err()

					return
				}

				chunk := append([]byte(nil), remaining[:taken]...)
				publish(builder.data(offset, chunk))
				offset += uint64FromInt(taken)
				remaining = remaining[taken:]
			}
		}

		if err != nil {
			done <- err

			return
		}
	}
}

type tunnelFrameBuilder struct {
	mu      sync.Mutex
	attempt uint32
	seq     uint64
}

func newTunnelFrameBuilder(attempt uint32) *tunnelFrameBuilder {
	return &tunnelFrameBuilder{attempt: attempt}
}

func (b *tunnelFrameBuilder) next(frame *strawpb.StreamFrame) *strawpb.StreamFrame {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.seq++
	frame.StreamSeq = b.seq
	frame.Attempt = b.attempt

	return frame
}

func (b *tunnelFrameBuilder) outboundStart(target TunnelTarget) *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_OutboundStart{OutboundStart: &strawpb.OutboundStartFrame{TargetHost: target.Host, TargetPort: target.Port, Attempt: b.attempt, WorkerTimestampMs: time.Now().UnixMilli()}}})
}

func (b *tunnelFrameBuilder) responseStart() *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_ResponseStart{ResponseStart: &strawpb.ResponseStart{Status: rawTunnelEstablished}}})
}

func (b *tunnelFrameBuilder) data(offset uint64, data []byte) *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_Data{Data: &strawpb.DataFrame{Offset: offset, Data: data}}})
}

func (b *tunnelFrameBuilder) uploadCredit(bytes uint64) *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_Credit{Credit: &strawpb.CreditFrame{UploadCreditBytes: bytes}}})
}

func (b *tunnelFrameBuilder) cancelled(reason string) *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_Cancelled{Cancelled: &strawpb.CancelledFrame{Reason: reason}}})
}

func (b *tunnelFrameBuilder) end() *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_End{End: &strawpb.EndFrame{Success: true}}})
}

func (b *tunnelFrameBuilder) error(frame *strawpb.ErrorFrame) *strawpb.StreamFrame {
	return b.next(&strawpb.StreamFrame{Payload: &strawpb.StreamFrame_Error{Error: frame}})
}

func (w *Worker) readRequestBody(ctx context.Context, validator *streamValidator, frames <-chan *strawpb.StreamFrame, expectedUploadBytes int64, deploymentID, requestID string) (*strawpb.RequestStart, []byte, *strawpb.ErrorFrame, bool) {
	state := &requestBodyState{deploymentID: deploymentID, requestID: requestID, refs: w.bodyRefs}
	if expectedUploadBytes > 0 {
		state.expected = uint64(expectedUploadBytes)
	}

	ticker := time.NewTicker(idleCheckInterval)
	defer ticker.Stop()

	for !state.complete() {
		select {
		case <-ctx.Done():
			return nil, nil, nil, false
		case frame, ok := <-frames:
			if !ok || !state.accept(ctx, validator, frame) {
				return nil, nil, state.failure, state.failure != nil
			}
		case <-ticker.C:
			if validator.idleExpired() {
				return nil, nil, nil, false
			}
		}
	}

	return state.start, state.body, nil, true
}

type requestBodyState struct {
	start        *strawpb.RequestStart
	body         []byte
	received     uint64
	expected     uint64
	deploymentID string
	requestID    string
	refs         BodyRefResolver
	failure      *strawpb.ErrorFrame
}

func (s *requestBodyState) complete() bool {
	return s.start != nil && s.received >= s.expected
}

func (s *requestBodyState) accept(ctx context.Context, validator *streamValidator, frame *strawpb.StreamFrame) bool {
	outcome := validator.accept(frame)
	if outcome == frameDuplicate {
		return true
	}

	if outcome != frameAccepted {
		return false
	}

	switch p := frame.GetPayload().(type) {
	case *strawpb.StreamFrame_RequestStart:
		s.start = p.RequestStart
	case *strawpb.StreamFrame_Data:
		s.body = append(s.body, p.Data.GetData()...)
		s.received += uint64(len(p.Data.GetData()))
	case *strawpb.StreamFrame_BodyRef:
		return s.acceptBodyRef(ctx, p.BodyRef)
	case *strawpb.StreamFrame_Credit:
	default:
		return false
	}

	return true
}

func (s *requestBodyState) acceptBodyRef(ctx context.Context, frame *strawpb.BodyRefFrame) bool {
	if s.start == nil || s.refs == nil {
		return false
	}

	if !bodyRefObjectKeyScoped(frame, s.deploymentID, s.requestID) {
		s.failure = &strawpb.ErrorFrame{Code: strawpb.ErrorCode_ERROR_CODE_BODY_REF_UNAVAILABLE, Details: map[string]string{errorFactDetailKey: "body_ref_scope_mismatch"}}

		return false
	}

	body, failure := s.refs.DownloadBodyRef(ctx, frame)
	if failure != nil {
		s.failure = failure

		return false
	}

	s.body = body
	s.received = uint64(len(body))

	return true
}

func bodyRefObjectKeyScoped(frame *strawpb.BodyRefFrame, deploymentID, requestID string) bool {
	key := frame.GetS3().GetObjectKey()

	return strings.HasPrefix(key, "tenant/"+deploymentID+"/request/"+requestID+"/request/")
}

func waitForResult(resultCh <-chan []*strawpb.StreamFrame, frames <-chan *strawpb.StreamFrame, validator *streamValidator, cancel context.CancelFunc, downloadCredit *responseCreditGate) ([]*strawpb.StreamFrame, bool, string) {
	for {
		select {
		case result := <-resultCh:
			return result, false, ""
		case frame, ok := <-frames:
			if !ok {
				continue
			}

			if validator.accept(frame) != frameAccepted {
				continue
			}

			if credit := frame.GetCredit(); credit != nil {
				downloadCredit.grant(credit.GetDownloadCreditBytes())

				continue
			}

			cancelFrame, isCancel := frame.GetPayload().(*strawpb.StreamFrame_Cancel)
			if !isCancel {
				continue
			}

			cancel()

			return <-resultCh, true, cancelFrame.Cancel.GetReason()
		}
	}
}

type responseCreditGate struct {
	credit uint64
	grants chan uint64
}

func newResponseCreditGate(initial uint64) *responseCreditGate {
	if initial == 0 {
		return nil
	}

	return &responseCreditGate{credit: initial, grants: make(chan uint64, streamFrameChannelBuffer)}
}

func (g *responseCreditGate) grant(bytes uint64) {
	if g != nil && bytes != 0 {
		g.grants <- bytes
	}
}

func (g *responseCreditGate) takeAvailable(ctx context.Context, limit int) (int, bool) {
	if g == nil {
		return limit, true
	}

	for g.credit == 0 {
		select {
		case <-ctx.Done():
			return 0, false
		case grant := <-g.grants:
			g.credit += grant
		}
	}

	limitUint := uint64FromInt(limit)
	if g.credit < limitUint {
		g.credit--

		return 1, true
	}

	g.credit -= limitUint

	return limit, true
}

func applyCancellation(frames []*strawpb.StreamFrame, reason string) []*strawpb.StreamFrame {
	if len(frames) == 0 {
		return frames
	}

	last := frames[len(frames)-1]
	if _, ok := last.GetPayload().(*strawpb.StreamFrame_End); ok {
		return frames
	}

	frames[len(frames)-1] = &strawpb.StreamFrame{
		StreamSeq: last.GetStreamSeq(),
		Attempt:   last.GetAttempt(),
		Payload:   &strawpb.StreamFrame_Cancelled{Cancelled: &strawpb.CancelledFrame{Reason: reason}},
	}

	return frames
}

func (w *Worker) publish(subject string, env *strawpb.Envelope, frames []*strawpb.StreamFrame) {
	for _, frame := range frames {
		out := &strawpb.Envelope{
			RequestId:      env.GetRequestId(),
			TenantId:       env.GetTenantId(),
			TraceId:        env.GetTraceId(),
			DeadlineUnixMs: env.GetDeadlineUnixMs(),
			ProtocolMajor:  ProtocolMajor,
			Attempt:        env.GetAttempt(),
			Payload:        &strawpb.Envelope_StreamFrame{StreamFrame: frame},
		}

		raw, err := MarshalEnvelope(out)
		if err != nil {
			return
		}

		err = w.conn.Publish(subject, raw)
		if err != nil {
			return
		}
	}
}

func decodeStreamFrame(raw []byte) *strawpb.StreamFrame {
	env, err := UnmarshalEnvelope(raw)
	if err != nil {
		return nil
	}

	return env.GetStreamFrame()
}

func (w *Worker) reply(msg *nats.Msg, env *strawpb.Envelope, ack *strawpb.AssignAck) {
	reply := &strawpb.Envelope{ProtocolMajor: ProtocolMajor, Payload: &strawpb.Envelope_AssignAck{AssignAck: ack}}
	if env != nil {
		reply.RequestId = env.GetRequestId()
		reply.TenantId = env.GetTenantId()
		reply.TraceId = env.GetTraceId()
		reply.DeadlineUnixMs = env.GetDeadlineUnixMs()
		reply.Attempt = env.GetAttempt()
	}

	raw, err := MarshalEnvelope(reply)
	if err == nil {
		_ = msg.Respond(raw)
	}
}

func errorFrame(attempt uint32, errFrame *strawpb.ErrorFrame) *strawpb.StreamFrame {
	return &strawpb.StreamFrame{StreamSeq: 1, Attempt: attempt, Payload: &strawpb.StreamFrame_Error{Error: errFrame}}
}

func uint64FromInt(n int) uint64 {
	if n <= 0 {
		return 0
	}

	return uint64(n)
}
