package egress

import (
	"time"

	strawpb "github.com/beremaran/straw/v2/api/proto/straw/v1"
)

type frameOutcome int

const (
	frameAccepted frameOutcome = iota
	frameDuplicate
	frameAfterTerminal
	frameSequenceGap
	frameOffsetMismatch
	frameCreditExhausted
	frameAttemptMismatch
	frameInvalid
)

type streamValidator struct {
	attempt      uint32
	expectedSeq  uint64
	offset       uint64
	credit       uint64
	terminal     bool
	idleTimeout  time.Duration
	lastActivity time.Time
	now          func() time.Time
	allowBodyRef bool
}

type streamValidatorOptions struct {
	attempt       uint32
	initialCredit uint64
	idleTimeout   time.Duration
	now           func() time.Time
	allowBodyRef  bool
}

func newStreamValidator(opts streamValidatorOptions) *streamValidator {
	now := opts.now
	if now == nil {
		now = time.Now
	}

	return &streamValidator{
		attempt:      opts.attempt,
		expectedSeq:  1,
		credit:       opts.initialCredit,
		idleTimeout:  opts.idleTimeout,
		lastActivity: now(),
		now:          now,
		allowBodyRef: opts.allowBodyRef,
	}
}

func (v *streamValidator) accept(f *strawpb.StreamFrame) frameOutcome {
	if outcome := v.validateFrameShell(f); outcome != frameAccepted {
		return outcome
	}

	seq := f.GetStreamSeq()
	switch {
	case seq < v.expectedSeq:
		return frameDuplicate
	case seq > v.expectedSeq:
		return frameSequenceGap
	}

	if _, ok := f.GetPayload().(*strawpb.StreamFrame_BodyRef); ok && !v.allowBodyRef {
		return frameInvalid
	}

	if outcome := v.acceptData(f.GetData()); outcome != frameAccepted {
		return outcome
	}

	v.expectedSeq++
	v.lastActivity = v.now()

	if isTerminalFrame(f) {
		v.terminal = true
	}

	return frameAccepted
}

func (v *streamValidator) idleExpired() bool {
	return v.idleTimeout > 0 && !v.terminal && v.now().Sub(v.lastActivity) >= v.idleTimeout
}

func (v *streamValidator) validateFrameShell(f *strawpb.StreamFrame) frameOutcome {
	if f == nil || f.GetPayload() == nil {
		return frameInvalid
	}

	if v.terminal {
		return frameAfterTerminal
	}

	if f.GetStreamSeq() == 0 {
		return frameInvalid
	}

	if f.GetAttempt() != v.attempt {
		return frameAttemptMismatch
	}

	return frameAccepted
}

func (v *streamValidator) acceptData(data *strawpb.DataFrame) frameOutcome {
	if data == nil {
		return frameAccepted
	}

	if data.GetOffset() != v.offset {
		return frameOffsetMismatch
	}

	n := uint64(len(data.GetData()))
	if n > v.credit {
		return frameCreditExhausted
	}

	v.credit -= n
	v.offset += n

	return frameAccepted
}

func isTerminalFrame(f *strawpb.StreamFrame) bool {
	switch f.GetPayload().(type) {
	case *strawpb.StreamFrame_End, *strawpb.StreamFrame_Error, *strawpb.StreamFrame_Cancelled:
		return true
	default:
		return false
	}
}
