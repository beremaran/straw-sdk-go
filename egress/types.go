package egress

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"
	"unicode"

	"google.golang.org/protobuf/proto"

	strawpb "github.com/beremaran/straw/v2/api/proto/straw/v1"
)

const (
	registrationNonceBytes = 16
	payloadSafetyMargin    = 64 * 1024

	// ProtocolMajor is the worker protocol major version this SDK speaks.
	ProtocolMajor uint32 = 1
)

var (
	errNilEnvelope                = errors.New("marshal envelope: nil")
	errSubjectTokenRequired       = errors.New("subject token is required")
	errSubjectTokenUnsafe         = errors.New("subject token contains unsafe character")
	errUnsupportedStreamDirection = errors.New("unsupported stream direction")
	errMaxPayloadTooSmall         = errors.New("nats max payload must exceed safety margin")
	errFrameBodyLimitTooLarge     = errors.New("configured frame/body limit exceeds nats max payload")
	errNATSServersRequired        = errors.New("nats servers are required")
	errNATSServerEmpty            = errors.New("nats server is empty")
)

// Executor is the public execution seam for SDK-built workers.
type Executor interface {
	Execute(ctx context.Context, start *strawpb.RequestStart, body []byte, attempt uint32, send func(*strawpb.StreamFrame)) []*strawpb.StreamFrame
}

// TenantExecutor is implemented by executors that need the tenant id while
// executing a decoded HTTP assignment.
type TenantExecutor interface {
	ExecuteWithTenant(ctx context.Context, tenantID string, start *strawpb.RequestStart, body []byte, attempt uint32, send func(*strawpb.StreamFrame)) []*strawpb.StreamFrame
}

// BodyRefResolver downloads a request body reference. It is optional and only
// used when Control sends BodyRef frames.
type BodyRefResolver interface {
	DownloadBodyRef(ctx context.Context, frame *strawpb.BodyRefFrame) ([]byte, *strawpb.ErrorFrame)
}

// TunnelOpener validates and opens raw CONNECT tunnel upstream connections.
type TunnelOpener interface {
	OpenTunnel(ctx context.Context, start *strawpb.RequestStart) (net.Conn, TunnelTarget, *strawpb.ErrorFrame)
}

// TunnelTarget is the already-validated raw tunnel destination.
type TunnelTarget struct {
	Host string
	Port uint32
}

// Identity holds the stable identity a worker registers with.
type Identity struct {
	WorkerID     string
	CredentialID string
	ExecutorType string
	PrivateKey   ed25519.PrivateKey
}

// Capabilities are the capability claims a worker advertises at registration.
type Capabilities struct {
	AllowedPools          []*strawpb.RegisterRequest_PoolRef
	Tags                  []string
	Countries             []string
	Regions               []string
	IPTypes               []string
	SupportedIngressModes []string
	MaxConcurrency        uint32
	SoftwareVersion       string
	InitialDraining       bool
}

// Capacity describes the executor's current admission state when an
// AssignRequest arrives.
type Capacity struct {
	Draining       bool
	ActiveRequests uint32
	MaxConcurrency uint32
	// SupportedModes lists the RequestModes this executor accepts. An empty
	// list means "any valid mode".
	SupportedModes []strawpb.RequestMode
}

// StreamDirection identifies the direction of a stream subject.
type StreamDirection string

const (
	// DirectionControlToExecutor marks control-to-executor stream subjects.
	DirectionControlToExecutor StreamDirection = "c2e"
	// DirectionExecutorToControl marks executor-to-control stream subjects.
	DirectionExecutorToControl StreamDirection = "e2c"
)

// BuildRegisterRequest assembles and signs a RegisterRequest for the worker.
func BuildRegisterRequest(id Identity, caps Capabilities) (*strawpb.RegisterRequest, error) {
	pools := make([]*strawpb.RegisterRequest_PoolRef, 0, len(caps.AllowedPools))
	for _, p := range caps.AllowedPools {
		pools = append(pools, &strawpb.RegisterRequest_PoolRef{TenantId: p.GetTenantId(), PoolId: p.GetPoolId()})
	}

	nonce := make([]byte, registrationNonceBytes)

	_, err := rand.Read(nonce)
	if err != nil {
		return nil, fmt.Errorf("generate registration nonce: %w", err)
	}

	req := &strawpb.RegisterRequest{
		WorkerId:              id.WorkerID,
		ExecutorType:          id.ExecutorType,
		CredentialId:          id.CredentialID,
		ProtocolMajor:         ProtocolMajor,
		ProtocolMinor:         0,
		SoftwareVersion:       caps.SoftwareVersion,
		AllowedPools:          pools,
		Tags:                  caps.Tags,
		Countries:             caps.Countries,
		Regions:               caps.Regions,
		IpTypes:               caps.IPTypes,
		SupportedIngressModes: caps.SupportedIngressModes,
		MaxConcurrency:        caps.MaxConcurrency,
		InitialDraining:       caps.InitialDraining,
		Nonce:                 nonce,
		IssuedAtUnixMs:        time.Now().UnixMilli(),
	}
	req.SignedToken = strawpb.SignRegistration(id.PrivateKey, req)

	return req, nil
}

// BuildHeartbeat assembles a HeartbeatRequest for the given active session.
func BuildHeartbeat(id Identity, sessionID string, health strawpb.WorkerHealth, activeRequests, availableCapacity, maxConcurrency uint32, draining bool) *strawpb.HeartbeatRequest {
	return &strawpb.HeartbeatRequest{
		WorkerId:          id.WorkerID,
		SessionId:         sessionID,
		Health:            health,
		ActiveRequests:    activeRequests,
		AvailableCapacity: availableCapacity,
		MaxConcurrency:    maxConcurrency,
		Draining:          draining,
		WorkerTimestampMs: time.Now().UnixMilli(),
	}
}

// InboxPrefix returns the scoped reply-inbox prefix this worker must configure
// on its NATS request/reply client.
func (id Identity) InboxPrefix() (string, error) {
	return WorkerInboxPrefix(id.WorkerID)
}

func (c Capacity) hasCapacity() bool {
	if c.MaxConcurrency == 0 {
		return true
	}

	return c.ActiveRequests < c.MaxConcurrency
}

func (c Capacity) supportsMode(mode strawpb.RequestMode) bool {
	if len(c.SupportedModes) == 0 {
		return true
	}

	return slices.Contains(c.SupportedModes, mode)
}

// EvaluateAssignment implements the executor-side admission decision for an
// AssignRequest.
func EvaluateAssignment(req *strawpb.AssignRequest, capacity Capacity) strawpb.AssignAckCode {
	if req == nil || req.Validate() != nil {
		return strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_ERROR
	}

	if capacity.Draining {
		return strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_DRAINING
	}

	if !capacity.supportsMode(req.GetMode()) {
		return strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_UNSUPPORTED
	}

	if !capacity.hasCapacity() {
		return strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_CAPACITY
	}

	return strawpb.AssignAckCode_ASSIGN_ACK_ACCEPTED
}

// RegistrationSubject returns the NATS subject for worker registration.
func RegistrationSubject() string { return "straw.v1.control.register" }

// HeartbeatSubject returns the NATS subject for worker heartbeats.
func HeartbeatSubject() string { return "straw.v1.control.heartbeat" }

// LogTelemetrySubject returns the transient Egress-to-Control log subject.
func LogTelemetrySubject() string { return "straw.v1.control.logs" }

// ControlInboxPrefix returns the reply-inbox prefix used by control clients.
func ControlInboxPrefix() string { return "_INBOX.ctl" }

// WorkerInboxPrefix returns the worker-specific reply inbox prefix.
func WorkerInboxPrefix(workerID string) (string, error) {
	err := ValidateSubjectToken(workerID)
	if err != nil {
		return "", fmt.Errorf("worker_id: %w", err)
	}

	return "_INBOX.wrk." + workerID, nil
}

// ValidateSubjectToken reports whether token is a safe NATS subject token.
func ValidateSubjectToken(token string) error {
	if token == "" {
		return errSubjectTokenRequired
	}

	for _, r := range token {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '-', r == '_':
		default:
			return fmt.Errorf("%w: %q", errSubjectTokenUnsafe, r)
		}
	}

	return nil
}

// AssignmentSubject returns the assignment subject for a worker session.
func AssignmentSubject(workerID, sessionID string) (string, error) {
	err := ValidateSubjectToken(workerID)
	if err != nil {
		return "", fmt.Errorf("worker_id: %w", err)
	}

	err = ValidateSubjectToken(sessionID)
	if err != nil {
		return "", fmt.Errorf("session_id: %w", err)
	}

	return fmt.Sprintf("straw.v1.executor.%s.%s.assign", workerID, sessionID), nil
}

// StreamSubject returns the stream subject for a request, worker, and session.
func StreamSubject(requestID, workerID, sessionID string, direction StreamDirection) (string, error) {
	err := ValidateSubjectToken(requestID)
	if err != nil {
		return "", fmt.Errorf("request_id: %w", err)
	}

	err = ValidateSubjectToken(workerID)
	if err != nil {
		return "", fmt.Errorf("worker_id: %w", err)
	}

	err = ValidateSubjectToken(sessionID)
	if err != nil {
		return "", fmt.Errorf("session_id: %w", err)
	}

	switch direction {
	case DirectionControlToExecutor, DirectionExecutorToControl:
		return fmt.Sprintf("straw.v1.req.%s.%s.%s.%s", requestID, workerID, sessionID, direction), nil
	default:
		return "", fmt.Errorf("%w: %q", errUnsupportedStreamDirection, direction)
	}
}

// TerminalSubject returns the terminal subject for a request stream.
func TerminalSubject(requestID, workerID, sessionID string, direction StreamDirection) (string, error) {
	return StreamSubject(requestID, workerID, sessionID, direction)
}

// ValidateMaxPayload checks the configured NATS payload limits.
func ValidateMaxPayload(maxPayloadBytes *uint64, maxFrameDataBytes, maxInlineRequestBodyBytes, maxInlineResponseBodyBytes uint64) error {
	if maxPayloadBytes == nil {
		return nil
	}

	if *maxPayloadBytes <= payloadSafetyMargin {
		return fmt.Errorf("%w: %d <= %d", errMaxPayloadTooSmall, *maxPayloadBytes, payloadSafetyMargin)
	}

	limit := max(maxInlineRequestBodyBytes, maxFrameDataBytes)

	limit = max(limit, maxInlineResponseBodyBytes)
	if limit > *maxPayloadBytes-payloadSafetyMargin {
		return fmt.Errorf("%w: %d > %d - %d safety margin", errFrameBodyLimitTooLarge, limit, *maxPayloadBytes, payloadSafetyMargin)
	}

	return nil
}

// ValidateServers validates the configured NATS server list.
func ValidateServers(servers []string) error {
	if len(servers) == 0 {
		return errNATSServersRequired
	}

	for i, server := range servers {
		if server == "" {
			return fmt.Errorf("%w: %d", errNATSServerEmpty, i)
		}
	}

	return nil
}

// MarshalEnvelope encodes a Straw protobuf Envelope for NATS transport.
func MarshalEnvelope(env *strawpb.Envelope) ([]byte, error) {
	if env == nil {
		return nil, errNilEnvelope
	}

	raw, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}

	return raw, nil
}

// UnmarshalEnvelope decodes a Straw protobuf Envelope from NATS transport.
func UnmarshalEnvelope(raw []byte) (*strawpb.Envelope, error) {
	env := &strawpb.Envelope{}

	err := proto.Unmarshal(raw, env)
	if err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}

	return env, nil
}
