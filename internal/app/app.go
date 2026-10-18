// Package app wires worker.Request and runs it until shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/d9293/halyk-genai-worker-request/internal/client/ai"
	"github.com/d9293/halyk-genai-worker-request/internal/client/integrator"
	"github.com/d9293/halyk-genai-worker-request/internal/config"
	"github.com/d9293/halyk-genai-worker-request/internal/runner"
	"github.com/d9293/halyk-genai-worker-request/internal/source"
	"github.com/d9293/halyk-genai-worker-request/internal/storage/s3"
	"github.com/d9293/halyk-genai-worker-request/internal/usecase/document"
	"github.com/d9293/halyk-genai-worker-request/internal/usecase/submission"
)

// loop is one independent worker loop, e.g. copying documents or sending to AI.
type loop struct {
	name string
	step runner.Step
}

// Run starts the loops and the health endpoint, and on ctx cancellation shuts
// down in order: stop claiming new work, let work in progress finish for up to
// ShutdownGrace, then cancel what is left.
func Run(ctx context.Context, cfg config.Config) error {
	api := integrator.New(integrator.Config{
		BaseURL:    cfg.Integrator.URL,
		Timeout:    cfg.Integrator.Timeout,
		Retries:    cfg.Integrator.Retries,
		RetryDelay: cfg.Integrator.RetryDelay,
		WorkerID:   cfg.WorkerID,
	})

	storeCtx, cancelStore := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStore()
	storage, err := s3.New(storeCtx, s3.Config{
		Endpoint:        cfg.S3.Endpoint,
		Region:          cfg.S3.Region,
		Bucket:          cfg.S3.Bucket,
		AccessKeyID:     cfg.S3.AccessKeyID,
		SecretAccessKey: cfg.S3.SecretAccessKey,
		PartSize:        cfg.S3.PartSize,
	})
	if err != nil {
		return err
	}

	documents := document.NewLoop(document.Config{
		BatchSize:       cfg.Documents.BatchSize,
		Workers:         cfg.Documents.Workers,
		RenewEvery:      cfg.Documents.RenewEvery,
		MaxCopyDuration: cfg.Documents.MaxCopyDuration,
	}, api, source.New(source.Config{
		MaxBytes:      cfg.Documents.MaxBytes,
		HeaderTimeout: cfg.Documents.SourceHeaderTimeout,
	}), storage)

	aiClient, err := ai.New(ai.Config{
		BaseURLs: map[string]string{
			"REGISTER_COMPARE": cfg.AI.RegisterCompareURL,
			"SOCIAL_MEDIA":     cfg.AI.SocialMediaURL,
		},
		Timeout: cfg.AI.Timeout,
	})
	if err != nil {
		return err
	}
	submissions := submission.NewLoop(submission.Config{
		BatchSize:  cfg.Submissions.BatchSize,
		Workers:    cfg.Submissions.Workers,
		RenewEvery: cfg.Submissions.RenewEvery,
	}, api, aiClient)

	loops := []loop{
		{name: "documents", step: documents.Step},
		{name: "submissions", step: submissions.Step},
	}

	health := &http.Server{
		Addr:              ":" + cfg.HealthPort,
		Handler:           healthHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	healthErr := make(chan error, 1)
	go func() { healthErr <- health.ListenAndServe() }()

	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()

	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Go(func() {
			runner.Run(ctx, work, l.name, runner.Config{IdleDelay: cfg.Loop.IdleDelay, MaxIdleDelay: cfg.Loop.MaxIdleDelay}, l.step)
		})
	}

	slog.Info("worker.Request started",
		"workerId", cfg.WorkerID,
		"integrator", cfg.Integrator.URL,
		"s3", cfg.S3.Endpoint+"/"+cfg.S3.Bucket,
		"healthPort", cfg.HealthPort,
		"loops", len(loops),
	)

	select {
	case err := <-healthErr:
		return fmt.Errorf("health server: %w", err)
	case <-ctx.Done():
	}

	slog.Info("stopping: no new work is claimed; waiting for work in progress", "grace", cfg.ShutdownGrace)
	if !waitTimeout(&wg, cfg.ShutdownGrace) {
		slog.Warn("grace period over, cancelling work in progress")
		cancelWork()
		wg.Wait()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := health.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown health server: %w", err)
	}
	slog.Info("worker.Request stopped")
	return nil
}

func healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// waitTimeout waits for wg, giving up after d. It reports whether wg finished.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}
