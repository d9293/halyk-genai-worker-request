package integrator

import (
	"context"
	"encoding/json"
	"time"
)

// ClaimedSubmission is one POST to AI this worker must make now. It already
// holds one of the limited AI places.
type ClaimedSubmission struct {
	// AttemptID is sent to AI as its task_id.
	AttemptID   string `json:"attemptId"`
	AttemptNo   int    `json:"attemptNo"`
	RequestGUID string `json:"requestGuid"`
	RequestID   string `json:"requestId"`
	Operation   string `json:"operation"`
	// Input is the operation's input as BPM registered it, normalized.
	Input json.RawMessage `json:"input"`
	// Documents are the S3 copies in the order AI must receive them; empty for operations without files.
	Documents  []SubmissionDocument `json:"documents"`
	ClaimToken string               `json:"claimToken"`
	ClaimUntil time.Time            `json:"claimUntil"`
}

type SubmissionDocument struct {
	Position int    `json:"position"`
	S3URI    string `json:"s3Uri"`
}

// Outcome values: what happened when the worker sent the task to AI.
const (
	// OutcomeAccepted: AI confirmed it took the task.
	OutcomeAccepted = "ACCEPTED"
	// OutcomeUnknown: no reliable answer; AI may have the task. Never send it again yourself.
	OutcomeUnknown = "UNKNOWN"
	// OutcomeRejected: AI clearly refused the task.
	OutcomeRejected = "REJECTED"
)

// Outcome is what the worker reports after its POST to AI.
type Outcome struct {
	Outcome string `json:"outcome"`
	// RemoteStatus is AI's status of an accepted task: QUEUED or PROCESSING.
	RemoteStatus string `json:"remoteStatus,omitempty"`
	// Failure explains UNKNOWN (optional) and REJECTED (required).
	Failure *Failure `json:"failure,omitempty"`
}

// ClaimSubmissions takes up to limit tasks to send to AI. Empty when nothing
// is ready or all AI places are taken.
func (c *Client) ClaimSubmissions(ctx context.Context, limit int) ([]ClaimedSubmission, error) {
	var resp struct {
		Items []ClaimedSubmission `json:"items"`
	}
	err := c.post(ctx, "/internal/v1/submissions/claim", claimRequest{WorkerID: c.cfg.WorkerID, Limit: limit}, &resp)
	return resp.Items, err
}

// ReportOutcome reports what happened when sending to AI. Repeating it is safe.
func (c *Client) ReportOutcome(ctx context.Context, s ClaimedSubmission, outcome Outcome) error {
	body := struct {
		ClaimToken string `json:"claimToken"`
		Outcome
	}{s.ClaimToken, outcome}
	return c.post(ctx, "/internal/v1/submissions/"+s.AttemptID+"/outcome", body, nil)
}

// RenewSubmission extends the claim while the POST to AI is in progress.
func (c *Client) RenewSubmission(ctx context.Context, s ClaimedSubmission) (time.Time, error) {
	var resp renewResponse
	err := c.post(ctx, "/internal/v1/submissions/"+s.AttemptID+"/renew", renewRequest{ClaimToken: s.ClaimToken}, &resp)
	return resp.ClaimUntil, err
}
