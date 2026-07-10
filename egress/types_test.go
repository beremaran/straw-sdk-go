package egress

import (
	"crypto/ed25519"
	"slices"
	"testing"

	strawpb "github.com/beremaran/straw/v2/api/proto/straw/v1"
)

const sdkTypesTestChrome120 = "chrome_120"

const (
	testWorker1 = "worker-1"
	testWcred1  = "wcred_1"
	testTenantA = "ten_a"
	testPool1   = "pool_1"
	testEgress  = "egress"
)

func TestBuildRegisterRequestSignsVerifiably(t *testing.T) {
	t.Parallel()

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	req, err := BuildRegisterRequest(
		Identity{WorkerID: testWorker1, CredentialID: testWcred1, ExecutorType: testEgress, PrivateKey: priv},
		Capabilities{
			AllowedPools:          []*strawpb.RegisterRequest_PoolRef{{TenantId: testTenantA, PoolId: testPool1}},
			Tags:                  []string{"local"},
			Countries:             []string{"AU"},
			Regions:               []string{"wa"},
			IPTypes:               []string{"datacenter"},
			SupportedIngressModes: []string{"rest"},
			MaxConcurrency:        4,
			SoftwareVersion:       "test",
			InitialDraining:       true,
		},
	)
	if err != nil {
		t.Fatalf("BuildRegisterRequest: %v", err)
	}

	if req.GetProtocolMajor() != ProtocolMajor {
		t.Fatalf("protocol major = %d, want %d", req.GetProtocolMajor(), ProtocolMajor)
	}
	if !strawpb.VerifyRegistrationSignature(pub, req, req.GetSignedToken()) {
		t.Fatal("signature produced by BuildRegisterRequest did not verify")
	}
	if !slices.Equal(req.GetCountries(), []string{"AU"}) || req.GetMaxConcurrency() != 4 || !req.GetInitialDraining() {
		t.Fatalf("capabilities not copied into RegisterRequest: %+v", req)
	}

	otherPub, _, _ := ed25519.GenerateKey(nil)
	if strawpb.VerifyRegistrationSignature(otherPub, req, req.GetSignedToken()) {
		t.Fatal("signature verified under the wrong public key")
	}
}

func TestBuildRegisterRequestCopiesFingerprintProfiles(t *testing.T) {
	t.Parallel()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	profiles := []string{sdkTypesTestChrome120}
	req, err := BuildRegisterRequest(
		Identity{WorkerID: testWorker1, CredentialID: testWcred1, ExecutorType: testEgress, PrivateKey: priv},
		Capabilities{SupportedFingerprintProfiles: profiles},
	)
	if err != nil {
		t.Fatalf("BuildRegisterRequest: %v", err)
	}

	profiles[0] = "mutated_after_build"
	if got := req.GetSupportedFingerprintProfiles(); !slices.Equal(got, []string{sdkTypesTestChrome120}) {
		t.Fatalf("supported fingerprint profiles = %v, want copied [chrome_120]", got)
	}
	if req.GetProtocolMinor() != 1 {
		t.Fatalf("protocol minor = %d, want 1 for fingerprint capabilities", req.GetProtocolMinor())
	}
}

func TestBuildHeartbeat(t *testing.T) {
	t.Parallel()

	hb := BuildHeartbeat(Identity{WorkerID: testWorker1}, "sess_1", strawpb.WorkerHealth_WORKER_HEALTH_DEGRADED, 2, 6, 8, true)
	if hb.GetWorkerId() != testWorker1 || hb.GetSessionId() != "sess_1" || hb.GetHealth() != strawpb.WorkerHealth_WORKER_HEALTH_DEGRADED {
		t.Fatalf("heartbeat = %+v, unexpected identity/session/health", hb)
	}
	if hb.GetActiveRequests() != 2 || hb.GetAvailableCapacity() != 6 || hb.GetMaxConcurrency() != 8 || !hb.GetDraining() {
		t.Fatalf("heartbeat capacity fields = %+v", hb)
	}
	if hb.GetWorkerTimestampMs() == 0 {
		t.Fatal("worker timestamp not set")
	}
}

func TestEvaluateAssignmentPrecedence(t *testing.T) {
	t.Parallel()

	req := &strawpb.AssignRequest{Mode: strawpb.RequestMode_REQUEST_MODE_DECODED_HTTP}
	if got := EvaluateAssignment(req, Capacity{Draining: true}); got != strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_DRAINING {
		t.Fatalf("draining decision = %v, want rejected_draining", got)
	}
	if got := EvaluateAssignment(req, Capacity{SupportedModes: []strawpb.RequestMode{strawpb.RequestMode_REQUEST_MODE_RAW_TUNNEL}}); got != strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_UNSUPPORTED {
		t.Fatalf("mode decision = %v, want rejected_unsupported", got)
	}
	if got := EvaluateAssignment(req, Capacity{MaxConcurrency: 1, ActiveRequests: 1}); got != strawpb.AssignAckCode_ASSIGN_ACK_REJECTED_CAPACITY {
		t.Fatalf("capacity decision = %v, want rejected_capacity", got)
	}
	if got := EvaluateAssignment(req, Capacity{}); got != strawpb.AssignAckCode_ASSIGN_ACK_ACCEPTED {
		t.Fatalf("accept decision = %v, want accepted", got)
	}
}

func TestSubjectsValidateSafeTokens(t *testing.T) {
	t.Parallel()

	assign, err := AssignmentSubject("worker-1", "sess_1")
	if err != nil {
		t.Fatalf("AssignmentSubject: %v", err)
	}
	if assign != "straw.v1.executor.worker-1.sess_1.assign" {
		t.Fatalf("assignment subject = %q", assign)
	}

	stream, err := StreamSubject("req_1", "worker-1", "sess_1", DirectionExecutorToControl)
	if err != nil {
		t.Fatalf("StreamSubject: %v", err)
	}
	if stream != "straw.v1.req.req_1.worker-1.sess_1.e2c" {
		t.Fatalf("stream subject = %q", stream)
	}

	_, err = StreamSubject("bad.token", "worker-1", "sess_1", DirectionExecutorToControl)
	if err == nil {
		t.Fatal("StreamSubject accepted unsafe request token")
	}

	_, err = (Identity{WorkerID: "bad.token"}).InboxPrefix()
	if err == nil {
		t.Fatal("InboxPrefix accepted unsafe worker token")
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	env := &strawpb.Envelope{
		RequestId:      "req_1",
		TenantId:       testTenantA,
		TraceId:        "trace_1",
		ProtocolMajor:  ProtocolMajor,
		Attempt:        1,
		DeadlineUnixMs: 12345,
		Payload: &strawpb.Envelope_StreamFrame{
			StreamFrame: &strawpb.StreamFrame{
				StreamSeq: 1,
				Attempt:   1,
				Payload: &strawpb.StreamFrame_Data{
					Data: &strawpb.DataFrame{Offset: 0, Data: []byte("hello")},
				},
			},
		},
	}

	raw, err := MarshalEnvelope(env)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	got, err := UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	if got.GetRequestId() != env.GetRequestId() || string(got.GetStreamFrame().GetData().GetData()) != "hello" {
		t.Fatalf("round trip = %+v, want request/data preserved", got)
	}
	_, err = MarshalEnvelope(nil)
	if err == nil {
		t.Fatal("MarshalEnvelope(nil) succeeded")
	}
}
