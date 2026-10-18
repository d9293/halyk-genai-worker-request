package ai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAI struct {
	status int
	body   string
	delay  time.Duration
	calls  atomic.Int32
	path   atomic.Value
	sent   atomic.Value
}

func (f *fakeAI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	b, _ := io.ReadAll(r.Body)
	f.path.Store(r.URL.Path)
	f.sent.Store(string(b))
	time.Sleep(f.delay)
	w.WriteHeader(f.status)
	_, _ = w.Write([]byte(f.body))
}

func newClient(t *testing.T, ai *fakeAI) *Client {
	t.Helper()
	srv := httptest.NewServer(ai)
	t.Cleanup(srv.Close)
	c, err := New(Config{
		BaseURLs: map[string]string{"REGISTER_COMPARE": srv.URL + "/register-compare/", "SOCIAL_MEDIA": srv.URL + "/social-media"},
		Timeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func compare(taskID string) Submission {
	return Submission{
		TaskID: taskID, Operation: "REGISTER_COMPARE", Input: json.RawMessage(`{"adjudicate":false}`),
		Files: []string{"s3://bpm-bucket/a.pdf", "s3://bpm-bucket/b.pdf"},
	}
}

func sent(t *testing.T, ai *fakeAI) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(ai.sent.Load().(string)), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRegisterCompare_Accepted(t *testing.T) {
	ai := &fakeAI{status: 202, body: `{"task_id":"a1","status":"queued"}`}
	got := newClient(t, ai).Submit(context.Background(), compare("a1"))

	if got != (Result{Outcome: OutcomeAccepted, RemoteStatus: "QUEUED"}) {
		t.Errorf("result = %+v", got)
	}
	if ai.path.Load() != "/register-compare/api/v1/compare" {
		t.Errorf("path = %v", ai.path.Load())
	}
	b := sent(t, ai)
	files, _ := b["files"].([]any)
	if b["task_id"] != "a1" || b["adjudicate"] != false || len(files) != 2 || files[0] != "s3://bpm-bucket/a.pdf" {
		t.Errorf("body = %v", b)
	}
}

func TestSocialMedia_Accepted(t *testing.T) {
	ai := &fakeAI{status: 202, body: `{"task_id":"a2","status":"processing"}`}
	s := Submission{TaskID: "a2", Operation: "SOCIAL_MEDIA", Input: json.RawMessage(`{"iin":"000000000002","fullName":"Тест"}`)}

	got := newClient(t, ai).Submit(context.Background(), s)
	if got != (Result{Outcome: OutcomeAccepted, RemoteStatus: "PROCESSING"}) {
		t.Errorf("result = %+v", got)
	}
	if b := sent(t, ai); ai.path.Load() != "/social-media/api/v1/search" ||
		b["task_id"] != "a2" || b["iin"] != "000000000002" || b["full_name"] != "Тест" {
		t.Errorf("path %v, body %v", ai.path.Load(), b)
	}
}

func TestAnswers(t *testing.T) {
	tests := []struct {
		name    string
		ai      *fakeAI
		outcome string
		code    string
	}{
		{"409 duplicate", &fakeAI{status: 409, body: `{"detail":"Задача с таким task_id уже выполняется: a1"}`}, OutcomeUnknown, CodeAIDuplicateTask},
		{"422 invalid", &fakeAI{status: 422, body: `{"detail":[{"loc":["body","files"],"msg":"нужно ровно 2 ссылки"}]}`}, OutcomeRejected, CodeAIValidationError},
		{"500", &fakeAI{status: 500, body: `{"detail":"boom"}`}, OutcomeUnknown, CodeAIUnavailable},
		{"503 from a proxy", &fakeAI{status: 503, body: `<html>Service Unavailable</html>`}, OutcomeUnknown, CodeAIUnavailable},
		{"404 wrong route", &fakeAI{status: 404, body: `{"detail":"Not Found"}`}, OutcomeUnknown, CodeAIUnavailable},
		{"timeout", &fakeAI{status: 202, body: `{"task_id":"a1","status":"queued"}`, delay: time.Second}, OutcomeUnknown, CodeAITimeout},
		{"other task id", &fakeAI{status: 202, body: `{"task_id":"zzz","status":"queued"}`}, OutcomeUnknown, CodeRemoteTaskIDMismatch},
		{"unreadable 202", &fakeAI{status: 202, body: `ok`}, OutcomeUnknown, CodeAIUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newClient(t, tt.ai).Submit(context.Background(), compare("a1"))
			if got.Outcome != tt.outcome || got.FailureCode != tt.code {
				t.Errorf("result = %+v, want %s %s", got, tt.outcome, tt.code)
			}
			if n := tt.ai.calls.Load(); n != 1 {
				t.Errorf("AI was called %d times; a POST must never be repeated", n)
			}
		})
	}
}

func TestRejectedMessageCarriesAIsReason(t *testing.T) {
	ai := &fakeAI{status: 422, body: `{"detail":[{"loc":["body","files"],"msg":"нужно ровно 2 ссылки"}]}`}
	got := newClient(t, ai).Submit(context.Background(), compare("a1"))
	if !strings.Contains(got.FailureMessage, "нужно ровно 2 ссылки") || !strings.Contains(got.FailureMessage, "files") {
		t.Errorf("message = %q", got.FailureMessage)
	}
}

func TestUnreachableAI(t *testing.T) {
	c, err := New(Config{BaseURLs: map[string]string{"REGISTER_COMPARE": "http://127.0.0.1:1", "SOCIAL_MEDIA": "http://127.0.0.1:1"}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Submit(context.Background(), compare("a1")); got.Outcome != OutcomeUnknown || got.FailureCode != CodeAIUnavailable {
		t.Errorf("result = %+v", got)
	}
}

func TestRejectedBeforeCallingAI(t *testing.T) {
	tests := []struct {
		name string
		s    Submission
		code string
	}{
		{"unknown operation", Submission{TaskID: "a1", Operation: "ANALYZE_DECISIONS", Input: json.RawMessage(`{}`)}, CodeOperationUnsupported},
		{"one file", Submission{TaskID: "a1", Operation: "REGISTER_COMPARE", Input: json.RawMessage(`{}`), Files: []string{"s3://b/a"}}, CodeInvalidPreparedInput},
		{"social media without iin", Submission{TaskID: "a1", Operation: "SOCIAL_MEDIA", Input: json.RawMessage(`{"fullName":"x"}`)}, CodeInvalidPreparedInput},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ai := &fakeAI{status: 202}
			got := newClient(t, ai).Submit(context.Background(), tt.s)
			if got.Outcome != OutcomeRejected || got.FailureCode != tt.code {
				t.Errorf("result = %+v, want REJECTED %s", got, tt.code)
			}
			if ai.calls.Load() != 0 {
				t.Error("AI must not be called for a request the worker cannot build")
			}
		})
	}
}

func TestDefaultAdjudicateIsTrue(t *testing.T) {
	ai := &fakeAI{status: 202, body: `{"task_id":"a1","status":"queued"}`}
	s := compare("a1")
	s.Input = json.RawMessage(`{}`)
	newClient(t, ai).Submit(context.Background(), s)
	if sent(t, ai)["adjudicate"] != true {
		t.Error("adjudicate must default to true")
	}
}

func TestNewNeedsEveryBaseURL(t *testing.T) {
	if _, err := New(Config{BaseURLs: map[string]string{"REGISTER_COMPARE": "http://x"}}); err == nil {
		t.Error("expected an error for the missing SOCIAL_MEDIA base URL")
	}
}
