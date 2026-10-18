// Package document copies task documents from their Attstore links into S3
// for AI, and reports each copy to Integrator.Api.
package document

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/d9293/halyk-genai-worker-request/internal/client/integrator"
	"github.com/d9293/halyk-genai-worker-request/internal/source"
)

//go:generate moq -out mocks_test.go . integratorClient downloader store

type integratorClient interface {
	ClaimDocuments(ctx context.Context, limit int) ([]integrator.ClaimedDocument, error)
	CompleteDocument(ctx context.Context, d integrator.ClaimedDocument, result integrator.CopyResult) error
	FailDocument(ctx context.Context, d integrator.ClaimedDocument, failure integrator.Failure, retryable bool) error
	RenewDocument(ctx context.Context, d integrator.ClaimedDocument) (time.Time, error)
}

type downloader interface {
	Open(ctx context.Context, url string) (*source.Download, error)
}

type store interface {
	Put(ctx context.Context, key string, r io.Reader, contentType string) (string, error)
}

type Config struct {
	// BatchSize is how many documents one round claims.
	BatchSize int
	// Workers is how many documents are copied at the same time.
	Workers int
	// RenewEvery is how often the claim is extended while copying (about a quarter of the lease).
	RenewEvery time.Duration
	// MaxCopyDuration gives up on one copy after this long.
	MaxCopyDuration time.Duration
}

type Loop struct {
	cfg     Config
	api     integratorClient
	source  downloader
	storage store
	now     func() time.Time
}

func NewLoop(cfg Config, api integratorClient, source downloader, storage store) *Loop {
	return &Loop{cfg: cfg, api: api, source: source, storage: storage, now: time.Now}
}

// Step claims a batch of documents and copies them, at most Workers at once.
// It reports whether there was any work.
func (l *Loop) Step(ctx context.Context) (bool, error) {
	docs, err := l.api.ClaimDocuments(ctx, l.cfg.BatchSize)
	if err != nil {
		return false, fmt.Errorf("claim documents: %w", err)
	}
	if len(docs) == 0 {
		return false, nil
	}

	slots := make(chan struct{}, l.cfg.Workers)
	var wg sync.WaitGroup
	for _, d := range docs {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			l.process(ctx, d)
		})
	}
	wg.Wait()
	return true, nil
}

// process copies one document and reports the outcome. Errors are logged,
// not returned: one bad document must not stop the others.
func (l *Loop) process(ctx context.Context, d integrator.ClaimedDocument) {
	log := slog.With("documentId", d.ID, "requestGuid", d.RequestGUID, "position", d.Position)

	copyCtx, cancel := context.WithTimeout(ctx, l.cfg.MaxCopyDuration)
	defer cancel()
	lost := l.keepClaim(copyCtx, cancel, d)

	result, err := l.copy(copyCtx, d)
	cancel() // stops the heartbeat
	switch {
	case lost.Load():
		log.Warn("claim lost while copying; dropping the document, Integrator.Api hands it out again")
		return
	case ctx.Err() != nil:
		// Shutting down: no report. The claim expires and another worker takes the document.
		log.Info("shutdown during copy; leaving the document for another worker")
		return
	case err != nil:
		failure, retryable := classify(err)
		log.Warn("copy failed", "code", failure.Code, "retryable", retryable, "error", err)
		if err := l.api.FailDocument(ctx, d, failure, retryable); err != nil {
			log.Warn("could not report the failure", "error", err)
		}
		return
	}

	if err := l.api.CompleteDocument(ctx, d, result); err != nil {
		// The copy stays in S3 unused; Integrator.Api hands the document out again.
		log.Warn("could not report the copy", "s3Uri", result.S3URI, "error", err)
		return
	}
	log.Info("document copied", "s3Uri", result.S3URI, "sizeBytes", result.SizeBytes)
}

// keepClaim renews the claim every RenewEvery until ctx ends. If the claim is
// lost, or cannot be renewed before it expires, it cancels the copy and sets
// the returned flag.
func (l *Loop) keepClaim(ctx context.Context, cancelCopy context.CancelFunc, d integrator.ClaimedDocument) *atomic.Bool {
	lost := &atomic.Bool{}
	go func() {
		claimUntil := d.ClaimUntil
		ticker := time.NewTicker(l.cfg.RenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			until, err := l.api.RenewDocument(ctx, d)
			switch {
			case err == nil:
				claimUntil = until
			case ctx.Err() != nil:
				return // the copy finished meanwhile
			case errors.Is(err, integrator.ErrClaimLost) || l.now().After(claimUntil):
				lost.Store(true)
				cancelCopy()
				return
			}
			// Other errors: keep the copy going and try again; the claim is still valid.
		}
	}()
	return lost
}

// copy streams the document from its source into storage, counting its size
// and SHA-256 on the way.
func (l *Loop) copy(ctx context.Context, d integrator.ClaimedDocument) (integrator.CopyResult, error) {
	dl, err := l.source.Open(ctx, d.AttstoreURL)
	if err != nil {
		return integrator.CopyResult{}, err
	}
	defer dl.Body.Close()

	key, err := l.objectKey(d, dl.ContentType)
	if err != nil {
		return integrator.CopyResult{}, err
	}
	contentType := dl.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	hash := sha256.New()
	counter := &countingReader{r: io.TeeReader(dl.Body, hash)}
	uri, err := l.storage.Put(ctx, key, counter, contentType)
	if err != nil {
		return integrator.CopyResult{}, err
	}
	return integrator.CopyResult{
		S3URI:          uri,
		SizeBytes:      counter.n,
		ChecksumSHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}

// objectKey builds {operation}/{YYYY-MM-DD}/{requestGuid}-{position}-{uploadId}{ext}.
// The upload ID makes every copy a new object: a retry never overwrites an
// earlier one.
func (l *Loop) objectKey(d integrator.ClaimedDocument, contentType string) (string, error) {
	uploadID := make([]byte, 4)
	if _, err := rand.Read(uploadID); err != nil {
		return "", err
	}
	operation := strings.ReplaceAll(strings.ToLower(d.Operation), "_", "-")
	return fmt.Sprintf("%s/%s/%s-%d-%s%s",
		operation, l.now().UTC().Format("2006-01-02"), d.RequestGUID, d.Position,
		hex.EncodeToString(uploadID), extension(d.AttstoreURL, contentType)), nil
}

// extension takes the file extension from the link, else from the content type.
func extension(link, contentType string) string {
	if u, err := url.Parse(link); err == nil {
		if ext := strings.ToLower(path.Ext(u.Path)); ext != "" && len(ext) <= 6 {
			return ext
		}
	}
	if mediaType, _, err := mime.ParseMediaType(contentType); err == nil {
		if exts, _ := mime.ExtensionsByType(mediaType); len(exts) > 0 {
			return exts[0]
		}
	}
	return ""
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
