package integrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAPI answers every call with the next of answers (the last one repeats)
// and records what it received.
type fakeAPI struct {
	answers []answer
	calls   atomic.Int32
	path    atomic.Value // last request path
	body    atomic.Value // last request body
}

type answer struct {
	status int
	body   string
	delay  time.Duration
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := int(f.calls.Add(1)) - 1
	a := f.answers[min(n, len(f.answers)-1)]
	b, _ := io.ReadAll(r.Body)
	f.path.Store(r.URL.Path)
	f.body.Store(string(b))
	time.Sleep(a.delay)
	w.WriteHeader(a.status)
	_, _ = w.Write([]byte(a.body))
}

func newClient(t *testing.T, answers ...answer) (*Client, *fakeAPI) {
	t.Helper()
	api := &fakeAPI{answers: answers}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	return New(Config{
		BaseURL: srv.URL + "/", Timeout: 200 * time.Millisecond,
		Retries: 2, RetryDelay: time.Millisecond, WorkerID: "pod-1",
	}), api
}

func jsonBody(t *testing.T, api *fakeAPI) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(api.body.Load().(string)), &m); err != nil {
		t.Fatalf("request body: %v", err)
	}
	return m
}

var doc = ClaimedDocument{ID: "d1", ClaimToken: "t1"}

func TestClaimDocuments(t *testing.T) {
	c, api := newClient(t, answer{200, `{"items":[{"id":"d1","requestGuid":"g1","operation":"REGISTER_COMPARE","bpmDocumentId":"b1",
		"attstoreUrl":"http://localhost:9000/bpm-bucket/attstore-mock/a.pdf","position":1,
		"claimToken":"t1","claimUntil":"2026-10-18T10:00:00Z"}]}`, 0})

	docs, err := c.ClaimDocuments(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if api.path.Load() != "/internal/v1/documents/claim" {
		t.Errorf("path = %v", api.path.Load())
	}
	if b := jsonBody(t, api); b["workerId"] != "pod-1" || b["limit"] != float64(4) {
		t.Errorf("body = %v", b)
	}
	want := ClaimedDocument{
		ID: "d1", RequestGUID: "g1", Operation: "REGISTER_COMPARE", BPMDocumentID: "b1",
		AttstoreURL: "http://localhost:9000/bpm-bucket/attstore-mock/a.pdf", Position: 1,
		ClaimToken: "t1", ClaimUntil: time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC),
	}
	if len(docs) != 1 || docs[0] != want {
		t.Errorf("docs = %+v", docs)
	}
}

func TestDocumentCalls(t *testing.T) {
	c, api := newClient(t, answer{200, `{"applied":true}`, 0})
	ctx := context.Background()

	if err := c.CompleteDocument(ctx, doc, CopyResult{S3URI: "s3://b/k", SizeBytes: 5, ChecksumSHA256: "abc"}); err != nil {
		t.Fatal(err)
	}
	b := jsonBody(t, api)
	if api.path.Load() != "/internal/v1/documents/d1/complete" ||
		b["claimToken"] != "t1" || b["s3Uri"] != "s3://b/k" || b["sizeBytes"] != float64(5) || b["checksumSha256"] != "abc" {
		t.Errorf("complete: %v %v", api.path.Load(), b)
	}

	if err := c.FailDocument(ctx, doc, Failure{Code: "SOURCE_UNAVAILABLE", Message: "503"}, true); err != nil {
		t.Fatal(err)
	}
	b = jsonBody(t, api)
	if api.path.Load() != "/internal/v1/documents/d1/failure" ||
		b["code"] != "SOURCE_UNAVAILABLE" || b["message"] != "503" || b["retryable"] != true {
		t.Errorf("failure: %v %v", api.path.Load(), b)
	}
}

func TestRenew(t *testing.T) {
	c, api := newClient(t, answer{200, `{"claimUntil":"2026-10-18T10:01:00Z"}`, 0})
	until, err := c.RenewDocument(context.Background(), doc)
	if err != nil || !until.Equal(time.Date(2026, 10, 18, 10, 1, 0, 0, time.UTC)) {
		t.Fatalf("until = %v, err %v", until, err)
	}
	if api.path.Load() != "/internal/v1/documents/d1/renew" || jsonBody(t, api)["claimToken"] != "t1" {
		t.Errorf("renew: %v", api.path.Load())
	}
}

func TestSubmissionCalls(t *testing.T) {
	c, api := newClient(t, answer{200, `{"items":[{"attemptId":"a1","attemptNo":1,"requestGuid":"g1","requestId":"r1",
		"operation":"REGISTER_COMPARE","input":{"adjudicate":true},"documents":[{"position":0,"s3Uri":"s3://b/0"}],
		"claimToken":"t2","claimUntil":"2026-10-18T10:00:00Z"}]}`, 0})
	ctx := context.Background()

	subs, err := c.ClaimSubmissions(ctx, 1)
	if err != nil || len(subs) != 1 {
		t.Fatalf("subs = %+v, err %v", subs, err)
	}
	s := subs[0]
	if s.AttemptID != "a1" || s.Operation != "REGISTER_COMPARE" || string(s.Input) != `{"adjudicate":true}` ||
		len(s.Documents) != 1 || s.Documents[0].S3URI != "s3://b/0" {
		t.Errorf("submission = %+v", s)
	}
	if api.path.Load() != "/internal/v1/submissions/claim" {
		t.Errorf("path = %v", api.path.Load())
	}

	api.answers = []answer{{200, `{"applied":true}`, 0}}
	accepted := Outcome{Outcome: OutcomeAccepted, RemoteStatus: "QUEUED"}
	if err := c.ReportOutcome(ctx, s, accepted); err != nil {
		t.Fatal(err)
	}
	b := jsonBody(t, api)
	if api.path.Load() != "/internal/v1/submissions/a1/outcome" ||
		b["claimToken"] != "t2" || b["outcome"] != "ACCEPTED" || b["remoteStatus"] != "QUEUED" || b["failure"] != nil {
		t.Errorf("outcome: %v %v", api.path.Load(), b)
	}

	rejected := Outcome{Outcome: OutcomeRejected, Failure: &Failure{Code: "INVALID_INPUT", Message: "bad"}}
	if err := c.ReportOutcome(ctx, s, rejected); err != nil {
		t.Fatal(err)
	}
	if b := jsonBody(t, api); b["remoteStatus"] != nil || b["failure"].(map[string]any)["code"] != "INVALID_INPUT" {
		t.Errorf("rejected outcome body = %v", b)
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name      string
		answers   []answer
		wantErr   error
		wantCalls int32
	}{
		{"claim lost: not retried", []answer{{409, `{"error":{"code":"CLAIM_LOST","message":"x"}}`, 0}}, ErrClaimLost, 1},
		{"not found: not retried", []answer{{404, `{"error":{"code":"DOCUMENT_NOT_FOUND","message":"x"}}`, 0}}, ErrNotFound, 1},
		{"503 then OK: retried", []answer{{503, `{"error":{"code":"SERVICE_UNAVAILABLE"}}`, 0}, {200, `{"applied":true}`, 0}}, nil, 2},
		{"always 503: gives up", []answer{{503, ``, 0}}, ErrUnavailable, 3},
		{"timeout: retried, gives up", []answer{{200, ``, time.Second}}, ErrUnavailable, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, api := newClient(t, tt.answers...)
			err := c.CompleteDocument(context.Background(), doc, CopyResult{})
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if n := api.calls.Load(); n != tt.wantCalls {
				t.Errorf("calls = %d, want %d", n, tt.wantCalls)
			}
		})
	}
}

func TestBadRequestIsNotRetried(t *testing.T) {
	c, api := newClient(t, answer{400, `{"error":{"code":"VALIDATION_ERROR","message":"Некорректный запрос"}}`, 0})

	err := c.CompleteDocument(context.Background(), doc, CopyResult{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 400 || apiErr.Code != "VALIDATION_ERROR" {
		t.Fatalf("err = %v, want an APIError 400", err)
	}
	if n := api.calls.Load(); n != 1 {
		t.Errorf("calls = %d, want 1", n)
	}
}

func TestShutdownStopsRetrying(t *testing.T) {
	c, api := newClient(t, answer{503, ``, 0})
	c.cfg.RetryDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := c.CompleteDocument(ctx, doc, CopyResult{})
	if !errors.Is(err, context.Canceled) || api.calls.Load() != 1 {
		t.Errorf("err = %v, calls %d; want cancellation after the first call", err, api.calls.Load())
	}
}
