package document

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d9293/halyk-genai-worker-request/internal/client/integrator"
	"github.com/d9293/halyk-genai-worker-request/internal/source"
	"github.com/d9293/halyk-genai-worker-request/internal/storage/s3"
)

const content = "%PDF-1.4 sample document"

var testConfig = Config{BatchSize: 4, Workers: 2, RenewEvery: 10 * time.Millisecond, MaxCopyDuration: time.Second}

func claimed(position int) integrator.ClaimedDocument {
	return integrator.ClaimedDocument{
		ID: fmt.Sprintf("doc-%d", position), RequestGUID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		Operation: "REGISTER_COMPARE", AttstoreURL: "http://localhost:9000/bpm-bucket/attstore-mock/registry-a.pdf",
		Position: position, ClaimToken: "token", ClaimUntil: time.Now().Add(time.Minute),
	}
}

// fixture wires the loop to fakes that succeed unless a test changes them.
type fixture struct {
	loop    *Loop
	api     *integratorClientMock
	source  *downloaderMock
	storage *storeMock
	stored  atomic.Value // last stored content
}

func newFixture(docs ...integrator.ClaimedDocument) *fixture {
	f := &fixture{}
	f.api = &integratorClientMock{
		ClaimDocumentsFunc: func(context.Context, int) ([]integrator.ClaimedDocument, error) { return docs, nil },
		CompleteDocumentFunc: func(context.Context, integrator.ClaimedDocument, integrator.CopyResult) error {
			return nil
		},
		FailDocumentFunc: func(context.Context, integrator.ClaimedDocument, integrator.Failure, bool) error { return nil },
		RenewDocumentFunc: func(context.Context, integrator.ClaimedDocument) (time.Time, error) {
			return time.Now().Add(time.Minute), nil
		},
	}
	f.source = &downloaderMock{
		OpenFunc: func(context.Context, string) (*source.Download, error) {
			return &source.Download{Body: io.NopCloser(strings.NewReader(content)), ContentType: "application/pdf"}, nil
		},
	}
	f.storage = &storeMock{
		PutFunc: func(_ context.Context, key string, r io.Reader, _ string) (string, error) {
			data, err := io.ReadAll(r)
			f.stored.Store(string(data))
			return "s3://bpm-bucket/" + key, err
		},
	}
	f.loop = NewLoop(testConfig, f.api, f.source, f.storage)
	f.loop.now = func() time.Time { return time.Date(2026, 10, 18, 9, 30, 0, 0, time.UTC) }
	return f
}

func TestStep_CopiesAndCompletes(t *testing.T) {
	f := newFixture(claimed(1))

	didWork, err := f.loop.Step(context.Background())
	if err != nil || !didWork {
		t.Fatalf("didWork=%v err=%v", didWork, err)
	}

	calls := f.api.CompleteDocumentCalls()
	if len(calls) != 1 || len(f.api.FailDocumentCalls()) != 0 {
		t.Fatalf("complete calls = %d, fail calls = %d", len(calls), len(f.api.FailDocumentCalls()))
	}
	sum := sha256.Sum256([]byte(content))
	r := calls[0].Result
	if r.SizeBytes != int64(len(content)) || r.ChecksumSHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("result = %+v", r)
	}
	// {operation}/{date}/{requestGuid}-{position}-{uploadId}{ext}
	keyPattern := `^s3://bpm-bucket/register-compare/2026-10-18/dddddddd-dddd-4ddd-8ddd-dddddddddddd-1-[0-9a-f]{8}\.pdf$`
	if !regexp.MustCompile(keyPattern).MatchString(r.S3URI) {
		t.Errorf("s3Uri = %s, want %s", r.S3URI, keyPattern)
	}
	if f.stored.Load() != content {
		t.Errorf("stored %q", f.stored.Load())
	}
	if ct := f.storage.PutCalls()[0].ContentType; ct != "application/pdf" {
		t.Errorf("content type = %q", ct)
	}
}

func TestStep_NothingToDo(t *testing.T) {
	f := newFixture()
	if didWork, err := f.loop.Step(context.Background()); didWork || err != nil {
		t.Errorf("didWork=%v err=%v", didWork, err)
	}
}

func TestStep_ClaimFailureIsReturned(t *testing.T) {
	f := newFixture()
	f.api.ClaimDocumentsFunc = func(context.Context, int) ([]integrator.ClaimedDocument, error) {
		return nil, integrator.ErrUnavailable
	}
	if _, err := f.loop.Step(context.Background()); !errors.Is(err, integrator.ErrUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestStep_FailuresAreClassified(t *testing.T) {
	tests := []struct {
		name      string
		openErr   error
		putErr    error
		code      string
		retryable bool
	}{
		{"source 404", fmt.Errorf("%w: HTTP 404", source.ErrNotFound), nil, CodeSourceNotFound, false},
		{"source 403", fmt.Errorf("%w: HTTP 403", source.ErrForbidden), nil, CodeSourceForbidden, false},
		{"too large", source.ErrTooLarge, nil, CodeSourceTooLarge, false},
		{"source 503", fmt.Errorf("%w: HTTP 503", source.ErrUnavailable), nil, CodeSourceUnavailable, true},
		{"storage down", nil, fmt.Errorf("%w: x", s3.ErrUnavailable), CodeStorageUnavailable, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(claimed(0))
			if tt.openErr != nil {
				f.source.OpenFunc = func(context.Context, string) (*source.Download, error) { return nil, tt.openErr }
			}
			if tt.putErr != nil {
				f.storage.PutFunc = func(context.Context, string, io.Reader, string) (string, error) { return "", tt.putErr }
			}

			_, _ = f.loop.Step(context.Background())

			calls := f.api.FailDocumentCalls()
			if len(calls) != 1 || len(f.api.CompleteDocumentCalls()) != 0 {
				t.Fatalf("fail calls = %d, complete calls = %d", len(calls), len(f.api.CompleteDocumentCalls()))
			}
			if calls[0].Failure.Code != tt.code || calls[0].Retryable != tt.retryable {
				t.Errorf("failure = %+v retryable=%v, want %s %v", calls[0].Failure, calls[0].Retryable, tt.code, tt.retryable)
			}
			if strings.Contains(calls[0].Failure.Message, "http") {
				t.Error("the message must not contain the (possibly signed) link")
			}
		})
	}
}

// slowSource streams the content only after delay, or stops when ctx ends.
func slowSource(delay time.Duration) func(context.Context, string) (*source.Download, error) {
	return func(ctx context.Context, _ string) (*source.Download, error) {
		select {
		case <-time.After(delay):
			return &source.Download{Body: io.NopCloser(strings.NewReader(content))}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func TestStep_HeartbeatRenewsDuringALongCopy(t *testing.T) {
	f := newFixture(claimed(0))
	f.source.OpenFunc = slowSource(60 * time.Millisecond)

	_, _ = f.loop.Step(context.Background())

	if n := len(f.api.RenewDocumentCalls()); n < 3 {
		t.Errorf("renewed %d times during a 60ms copy with renew every 10ms", n)
	}
	if len(f.api.CompleteDocumentCalls()) != 1 {
		t.Error("the copy must still complete")
	}
}

func TestStep_ClaimLostStopsTheCopy(t *testing.T) {
	f := newFixture(claimed(0))
	f.source.OpenFunc = slowSource(time.Second)
	f.api.RenewDocumentFunc = func(context.Context, integrator.ClaimedDocument) (time.Time, error) {
		return time.Time{}, integrator.ErrClaimLost
	}

	start := time.Now()
	_, _ = f.loop.Step(context.Background())

	if time.Since(start) > 500*time.Millisecond {
		t.Error("the copy was not stopped when the claim was lost")
	}
	if len(f.api.CompleteDocumentCalls())+len(f.api.FailDocumentCalls()) != 0 {
		t.Error("a document whose claim was lost must not be reported")
	}
}

func TestStep_ExpiredClaimStopsTheCopy(t *testing.T) {
	doc := claimed(0)
	doc.ClaimUntil = time.Now().Add(-time.Second) // already expired
	f := newFixture(doc)
	f.loop.now = time.Now
	f.source.OpenFunc = slowSource(time.Second)
	f.api.RenewDocumentFunc = func(context.Context, integrator.ClaimedDocument) (time.Time, error) {
		return time.Time{}, integrator.ErrUnavailable // cannot renew, and the claim ran out
	}

	_, _ = f.loop.Step(context.Background())

	if len(f.api.CompleteDocumentCalls())+len(f.api.FailDocumentCalls()) != 0 {
		t.Error("a document whose claim ran out must not be reported")
	}
}

func TestStep_RenewHiccupDoesNotStopTheCopy(t *testing.T) {
	f := newFixture(claimed(0))
	f.source.OpenFunc = slowSource(40 * time.Millisecond)
	var calls atomic.Int32
	f.api.RenewDocumentFunc = func(context.Context, integrator.ClaimedDocument) (time.Time, error) {
		if calls.Add(1) == 1 {
			return time.Time{}, integrator.ErrUnavailable // one failed renew, the claim is still valid
		}
		return time.Now().Add(time.Minute), nil
	}
	f.loop.now = time.Now

	_, _ = f.loop.Step(context.Background())

	if len(f.api.CompleteDocumentCalls()) != 1 {
		t.Error("one failed renew must not stop a copy whose claim is still valid")
	}
}

func TestStep_CopyTimeout(t *testing.T) {
	f := newFixture(claimed(0))
	f.loop.cfg.MaxCopyDuration = 20 * time.Millisecond
	f.source.OpenFunc = slowSource(time.Second)

	_, _ = f.loop.Step(context.Background())

	calls := f.api.FailDocumentCalls()
	if len(calls) != 1 || calls[0].Failure.Code != CodeCopyTimeout || !calls[0].Retryable {
		t.Errorf("fail calls = %+v, want one retryable COPY_TIMEOUT", calls)
	}
}

func TestStep_ShutdownLeavesTheDocument(t *testing.T) {
	f := newFixture(claimed(0))
	f.source.OpenFunc = slowSource(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, _ = f.loop.Step(ctx)

	if len(f.api.CompleteDocumentCalls())+len(f.api.FailDocumentCalls()) != 0 {
		t.Error("on shutdown the document must not be reported: its claim expires and another worker takes it")
	}
}

func TestStep_AtMostWorkersCopiesAtOnce(t *testing.T) {
	f := newFixture(claimed(0), claimed(1), claimed(2), claimed(3))
	var running, peak atomic.Int32
	f.source.OpenFunc = func(context.Context, string) (*source.Download, error) {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return &source.Download{Body: io.NopCloser(strings.NewReader(content))}, nil
	}

	_, _ = f.loop.Step(context.Background())

	if p := peak.Load(); p != int32(testConfig.Workers) {
		t.Errorf("peak parallel copies = %d, want %d", p, testConfig.Workers)
	}
	if n := len(f.api.CompleteDocumentCalls()); n != 4 {
		t.Errorf("completed %d of 4", n)
	}
}

func TestExtension(t *testing.T) {
	tests := []struct{ link, contentType, want string }{
		{"http://host/a/registry.PDF", "", ".pdf"},
		{"http://host/a/scan.tiff?sig=x", "image/tiff", ".tiff"},
		{"http://host/download?id=1", "application/pdf", ".pdf"},
		{"http://host/download?id=1", "", ""},
	}
	for _, tt := range tests {
		if got := extension(tt.link, tt.contentType); got != tt.want {
			t.Errorf("extension(%q, %q) = %q, want %q", tt.link, tt.contentType, got, tt.want)
		}
	}
}
