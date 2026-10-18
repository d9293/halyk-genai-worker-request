package submission

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d9293/halyk-genai-worker-request/internal/client/ai"
	"github.com/d9293/halyk-genai-worker-request/internal/client/integrator"
)

var testConfig = Config{BatchSize: 4, Workers: 2, RenewEvery: 10 * time.Millisecond}

func claimed(attemptID string) integrator.ClaimedSubmission {
	return integrator.ClaimedSubmission{
		AttemptID: attemptID, AttemptNo: 1, RequestGUID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		Operation: "REGISTER_COMPARE", Input: json.RawMessage(`{"adjudicate":true}`),
		Documents: []integrator.SubmissionDocument{
			{Position: 2, S3URI: "s3://bpm-bucket/b.pdf"},
			{Position: 1, S3URI: "s3://bpm-bucket/a.pdf"},
		},
		ClaimToken: "token", ClaimUntil: time.Now().Add(time.Minute),
	}
}

// fixture wires the loop to fakes: AI accepts and Integrator.Api agrees, unless a test changes them.
type fixture struct {
	loop *Loop
	api  *integratorClientMock
	ai   *aiClientMock
}

func newFixture(items ...integrator.ClaimedSubmission) *fixture {
	f := &fixture{}
	f.api = &integratorClientMock{
		ClaimSubmissionsFunc: func(context.Context, int) ([]integrator.ClaimedSubmission, error) { return items, nil },
		ReportOutcomeFunc:    func(context.Context, integrator.ClaimedSubmission, integrator.Outcome) error { return nil },
		RenewSubmissionFunc: func(context.Context, integrator.ClaimedSubmission) (time.Time, error) {
			return time.Now().Add(time.Minute), nil
		},
	}
	f.ai = &aiClientMock{
		SubmitFunc: func(context.Context, ai.Submission) ai.Result {
			return ai.Result{Outcome: ai.OutcomeAccepted, RemoteStatus: "QUEUED"}
		},
	}
	f.loop = NewLoop(testConfig, f.api, f.ai)
	return f
}

func TestStep_SendsAndReportsAccepted(t *testing.T) {
	f := newFixture(claimed("a1"))

	didWork, err := f.loop.Step(context.Background())
	if err != nil || !didWork {
		t.Fatalf("didWork=%v err=%v", didWork, err)
	}

	sent := f.ai.SubmitCalls()
	if len(sent) != 1 {
		t.Fatalf("AI called %d times", len(sent))
	}
	s := sent[0].S
	if s.TaskID != "a1" || s.Operation != "REGISTER_COMPARE" || string(s.Input) != `{"adjudicate":true}` ||
		!slices.Equal(s.Files, []string{"s3://bpm-bucket/a.pdf", "s3://bpm-bucket/b.pdf"}) {
		t.Errorf("sent %+v; files must be in position order", s)
	}

	reports := f.api.ReportOutcomeCalls()
	if len(reports) != 1 {
		t.Fatalf("reported %d times", len(reports))
	}
	if got := reports[0].Outcome; got.Outcome != "ACCEPTED" || got.RemoteStatus != "QUEUED" || got.Failure != nil {
		t.Errorf("outcome = %+v", got)
	}
	if reports[0].S.ClaimToken != "token" {
		t.Error("the report must carry the claim")
	}
}

func TestStep_ReportsFailureOfUnknownAndRejected(t *testing.T) {
	for _, r := range []ai.Result{
		{Outcome: ai.OutcomeUnknown, FailureCode: ai.CodeAITimeout, FailureMessage: "no answer"},
		{Outcome: ai.OutcomeRejected, FailureCode: ai.CodeAIValidationError, FailureMessage: "bad files"},
	} {
		t.Run(r.Outcome, func(t *testing.T) {
			f := newFixture(claimed("a1"))
			f.ai.SubmitFunc = func(context.Context, ai.Submission) ai.Result { return r }

			_, _ = f.loop.Step(context.Background())

			got := f.api.ReportOutcomeCalls()[0].Outcome
			if got.Outcome != r.Outcome || got.Failure == nil || got.Failure.Code != r.FailureCode || got.Failure.Message != r.FailureMessage {
				t.Errorf("outcome = %+v", got)
			}
		})
	}
}

func TestStep_NothingClaimed(t *testing.T) {
	f := newFixture()
	if didWork, err := f.loop.Step(context.Background()); didWork || err != nil {
		t.Errorf("didWork=%v err=%v", didWork, err)
	}
	if len(f.ai.SubmitCalls()) != 0 {
		t.Error("AI must not be called")
	}
}

func TestStep_ClaimError(t *testing.T) {
	f := newFixture()
	f.api.ClaimSubmissionsFunc = func(context.Context, int) ([]integrator.ClaimedSubmission, error) {
		return nil, integrator.ErrUnavailable
	}
	if _, err := f.loop.Step(context.Background()); !errors.Is(err, integrator.ErrUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestStep_SlowPostRenewsClaim(t *testing.T) {
	f := newFixture(claimed("a1"))
	f.ai.SubmitFunc = func(ctx context.Context, _ ai.Submission) ai.Result {
		time.Sleep(55 * time.Millisecond)
		return ai.Result{Outcome: ai.OutcomeAccepted, RemoteStatus: "QUEUED"}
	}

	_, _ = f.loop.Step(context.Background())

	if n := len(f.api.RenewSubmissionCalls()); n < 3 {
		t.Errorf("renewed %d times during a 55ms POST with renew every 10ms", n)
	}
	if len(f.api.ReportOutcomeCalls()) != 1 {
		t.Error("outcome must be reported")
	}
}

func TestStep_LostClaimDoesNotCancelPost(t *testing.T) {
	f := newFixture(claimed("a1"))
	f.api.RenewSubmissionFunc = func(context.Context, integrator.ClaimedSubmission) (time.Time, error) {
		return time.Time{}, integrator.ErrClaimLost
	}
	f.ai.SubmitFunc = func(ctx context.Context, _ ai.Submission) ai.Result {
		time.Sleep(40 * time.Millisecond)
		if ctx.Err() != nil {
			t.Error("the POST was cancelled because of the lost claim")
		}
		return ai.Result{Outcome: ai.OutcomeAccepted, RemoteStatus: "QUEUED"}
	}
	f.api.ReportOutcomeFunc = func(context.Context, integrator.ClaimedSubmission, integrator.Outcome) error {
		return integrator.ErrClaimLost
	}

	if _, err := f.loop.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(f.api.RenewSubmissionCalls()); n != 1 {
		t.Errorf("renewed %d times; renewing must stop once the claim is lost", n)
	}
}

func TestStep_ReportsEvenWhenShutdownCancelsThePost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFixture(claimed("a1"))
	f.ai.SubmitFunc = func(context.Context, ai.Submission) ai.Result {
		cancel() // grace period is over while the POST is in flight
		return ai.Result{Outcome: ai.OutcomeUnknown, FailureCode: ai.CodeAIUnavailable}
	}
	var reportErr error
	f.api.ReportOutcomeFunc = func(ctx context.Context, _ integrator.ClaimedSubmission, _ integrator.Outcome) error {
		reportErr = ctx.Err()
		return reportErr
	}

	_, _ = f.loop.Step(ctx)

	reports := f.api.ReportOutcomeCalls()
	if len(reports) != 1 || reports[0].Outcome.Outcome != "UNKNOWN" {
		t.Fatalf("reports = %+v", reports)
	}
	if reportErr != nil {
		t.Error("the report must not be cancelled by shutdown")
	}
}

func TestStep_SkipsTasksNotStartedBeforeShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFixture(claimed("a1"), claimed("a2"))

	_, _ = f.loop.Step(ctx)

	if len(f.ai.SubmitCalls()) != 0 || len(f.api.ReportOutcomeCalls()) != 0 {
		t.Error("nothing may be sent or reported once shutdown has begun")
	}
}

func TestStep_RunsAtMostWorkersAtOnce(t *testing.T) {
	f := newFixture(claimed("a1"), claimed("a2"), claimed("a3"), claimed("a4"))
	var running, maxSeen atomic.Int32
	f.ai.SubmitFunc = func(context.Context, ai.Submission) ai.Result {
		n := running.Add(1)
		for m := maxSeen.Load(); n > m && !maxSeen.CompareAndSwap(m, n); m = maxSeen.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return ai.Result{Outcome: ai.OutcomeAccepted, RemoteStatus: "QUEUED"}
	}

	_, _ = f.loop.Step(context.Background())

	if len(f.ai.SubmitCalls()) != 4 || len(f.api.ReportOutcomeCalls()) != 4 {
		t.Fatal("every claimed task must be sent and reported")
	}
	if int(maxSeen.Load()) > testConfig.Workers {
		t.Errorf("%d POSTs ran at once, limit %d", maxSeen.Load(), testConfig.Workers)
	}
}

func TestToAI_NoDocuments(t *testing.T) {
	s := claimed("a1")
	s.Documents = nil
	if got := toAI(s); len(got.Files) != 0 {
		t.Errorf("files = %v", got.Files)
	}
}
