package integrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestContract runs the client against a real Integrator.Api, to prove the
// two agree on paths and JSON. Skipped unless CONTRACT_INTEGRATOR_URL is set:
//
//	CONTRACT_INTEGRATOR_URL=http://localhost:8080 go test -run Contract ./internal/client/integrator/
//
// It registers one REGISTER_COMPARE task and walks it through documents and
// submission, so the API needs AI places free.
func TestContract(t *testing.T) {
	baseURL := os.Getenv("CONTRACT_INTEGRATOR_URL")
	if baseURL == "" {
		t.Skip("CONTRACT_INTEGRATOR_URL not set")
	}
	ctx := context.Background()
	c := New(Config{BaseURL: baseURL, Timeout: 5 * time.Second, Retries: 1, RetryDelay: 100 * time.Millisecond, WorkerID: "contract-test"})

	requestGUID := newUUID()
	registerTask(t, baseURL, requestGUID)

	// Documents: claim both, renew, complete.
	var mine []ClaimedDocument
	for range 5 {
		docs, err := c.ClaimDocuments(ctx, 10)
		if err != nil {
			t.Fatalf("claim documents: %v", err)
		}
		for _, d := range docs {
			if d.RequestGUID == requestGUID {
				mine = append(mine, d)
			}
		}
		if len(mine) == 2 {
			break
		}
	}
	if len(mine) != 2 {
		t.Fatalf("claimed %d documents of the new task, want 2", len(mine))
	}
	if mine[0].Operation != "REGISTER_COMPARE" {
		t.Errorf("operation = %q, want REGISTER_COMPARE", mine[0].Operation)
	}
	if _, err := c.RenewDocument(ctx, mine[0]); err != nil {
		t.Fatalf("renew document: %v", err)
	}
	for _, d := range mine {
		result := CopyResult{S3URI: fmt.Sprintf("s3://bpm-bucket/contract/%s-%d.pdf", requestGUID, d.Position), SizeBytes: 100, ChecksumSHA256: strings.Repeat("a", 64)}
		if err := c.CompleteDocument(ctx, d, result); err != nil {
			t.Fatalf("complete document: %v", err)
		}
	}
	if _, err := c.RenewDocument(ctx, mine[0]); !errors.Is(err, ErrClaimLost) {
		t.Errorf("renew after complete: err = %v, want ErrClaimLost", err)
	}

	// Submission: claim it, renew, report ACCEPTED.
	var sub *ClaimedSubmission
	for range 20 {
		subs, err := c.ClaimSubmissions(ctx, 1)
		if err != nil {
			t.Fatalf("claim submissions: %v", err)
		}
		for i := range subs {
			if subs[i].RequestGUID == requestGUID {
				sub = &subs[i]
			} else {
				// Someone else's task: leave it as unknown, so it is polled, not lost.
				_ = c.ReportOutcome(ctx, subs[i], Outcome{Outcome: OutcomeUnknown})
			}
		}
		if sub != nil {
			break
		}
	}
	if sub == nil {
		t.Fatal("the task was not handed out for submission (AI places full?)")
	}
	if sub.Operation != "REGISTER_COMPARE" || len(sub.Documents) != 2 || sub.Documents[1].Position != 1 {
		t.Errorf("submission = %+v", sub)
	}
	if _, err := c.RenewSubmission(ctx, *sub); err != nil {
		t.Fatalf("renew submission: %v", err)
	}
	if err := c.ReportOutcome(ctx, *sub, Outcome{Outcome: OutcomeAccepted, RemoteStatus: "QUEUED"}); err != nil {
		t.Fatalf("report outcome: %v", err)
	}
}

func registerTask(t *testing.T, baseURL, requestGUID string) {
	t.Helper()
	body := fmt.Sprintf(`{"requestId":%q,"requestGuid":%q,"operation":"REGISTER_COMPARE","input":{},"documents":[
		{"documentId":%q,"attstoreUrl":"http://localhost:9000/bpm-bucket/attstore-mock/registry-a.pdf"},
		{"documentId":%q,"attstoreUrl":"http://localhost:9000/bpm-bucket/attstore-mock/registry-b.pdf"}]}`,
		newUUID(), requestGUID, newUUID(), newUUID())
	resp, err := http.Post(strings.TrimRight(baseURL, "/")+"/api/v1/tasks", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("register task: %d", resp.StatusCode)
	}
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
