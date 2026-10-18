// Package submission sends prepared tasks to AI, once each, and reports what
// happened to Integrator.Api.
package submission

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/d9293/halyk-genai-worker-request/internal/client/ai"
	"github.com/d9293/halyk-genai-worker-request/internal/client/integrator"
)

//go:generate go run github.com/matryer/moq@v0.8.1 -out mocks_test.go . integratorClient aiClient

type integratorClient interface {
	ClaimSubmissions(ctx context.Context, limit int) ([]integrator.ClaimedSubmission, error)
	ReportOutcome(ctx context.Context, s integrator.ClaimedSubmission, outcome integrator.Outcome) error
	RenewSubmission(ctx context.Context, s integrator.ClaimedSubmission) (time.Time, error)
}

type aiClient interface {
	Submit(ctx context.Context, s ai.Submission) ai.Result
}

// reportTimeout bounds reporting an outcome, which also runs during shutdown.
const reportTimeout = 10 * time.Second

type Config struct {
	// BatchSize is how many tasks one round claims.
	BatchSize int
	// Workers is how many POSTs to AI run at the same time.
	Workers int
	// RenewEvery is how often the claim is extended while a POST is in progress.
	RenewEvery time.Duration
}

type Loop struct {
	cfg Config
	api integratorClient
	ai  aiClient
}

func NewLoop(cfg Config, api integratorClient, ai aiClient) *Loop {
	return &Loop{cfg: cfg, api: api, ai: ai}
}

// Step claims a batch of tasks and sends them to AI, at most Workers at once.
// It reports whether there was any work.
func (l *Loop) Step(ctx context.Context) (bool, error) {
	items, err := l.api.ClaimSubmissions(ctx, l.cfg.BatchSize)
	if err != nil {
		return false, fmt.Errorf("claim submissions: %w", err)
	}
	if len(items) == 0 {
		return false, nil
	}

	slots := make(chan struct{}, l.cfg.Workers)
	var wg sync.WaitGroup
	for _, s := range items {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			l.process(ctx, s)
		})
	}
	wg.Wait()
	return true, nil
}

// process sends one task and reports the outcome. Errors are logged, not
// returned: one bad task must not stop the others.
func (l *Loop) process(ctx context.Context, s integrator.ClaimedSubmission) {
	log := slog.With("attemptId", s.AttemptID, "attemptNo", s.AttemptNo, "requestGuid", s.RequestGUID, "operation", s.Operation)

	if ctx.Err() != nil {
		// Shutting down before the POST: nothing was sent. The claim expires and
		// Integrator.Api takes the task back.
		log.Info("shutdown before sending; leaving the task to Integrator.Api")
		return
	}

	postCtx, stopRenew := context.WithCancel(ctx)
	go l.keepClaim(postCtx, s, log)
	result := l.ai.Submit(postCtx, toAI(s))
	stopRenew()

	// The POST may have reached AI, so the outcome must be reported even during
	// shutdown. If the report is lost anyway, Integrator.Api treats the expired
	// claim as UNKNOWN and finds out by polling.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	err := l.api.ReportOutcome(reportCtx, s, toOutcome(result))
	switch {
	case errors.Is(err, integrator.ErrClaimLost):
		log.Warn("claim lost before the outcome was reported; Integrator.Api finds out by polling", "outcome", result.Outcome)
	case err != nil:
		log.Warn("could not report the outcome", "outcome", result.Outcome, "error", err)
	case result.Outcome == ai.OutcomeAccepted:
		log.Info("sent to AI", "remoteStatus", result.RemoteStatus)
	default:
		log.Warn("sent to AI without acceptance", "outcome", result.Outcome, "code", result.FailureCode, "message", result.FailureMessage)
	}
}

// keepClaim renews the claim every RenewEvery until ctx ends. A failed renew
// does not stop the POST: cancelling it would not undo anything on AI's side.
func (l *Loop) keepClaim(ctx context.Context, s integrator.ClaimedSubmission, log *slog.Logger) {
	ticker := time.NewTicker(l.cfg.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if _, err := l.api.RenewSubmission(ctx, s); err != nil && ctx.Err() == nil {
			log.Warn("could not renew the claim during the POST", "error", err)
			if errors.Is(err, integrator.ErrClaimLost) {
				return
			}
		}
	}
}

func toAI(s integrator.ClaimedSubmission) ai.Submission {
	docs := slices.SortedFunc(slices.Values(s.Documents), func(a, b integrator.SubmissionDocument) int {
		return cmp.Compare(a.Position, b.Position)
	})
	files := make([]string, len(docs))
	for i, d := range docs {
		files[i] = d.S3URI
	}
	return ai.Submission{TaskID: s.AttemptID, Operation: s.Operation, Input: s.Input, Files: files}
}

func toOutcome(r ai.Result) integrator.Outcome {
	out := integrator.Outcome{Outcome: r.Outcome, RemoteStatus: r.RemoteStatus}
	if r.FailureCode != "" {
		out.Failure = &integrator.Failure{Code: r.FailureCode, Message: r.FailureMessage}
	}
	return out
}
