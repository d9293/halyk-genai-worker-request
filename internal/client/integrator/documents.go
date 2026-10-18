package integrator

import (
	"context"
	"time"
)

// ClaimedDocument is a document this worker must copy to S3, owned until ClaimUntil.
type ClaimedDocument struct {
	// ID is Integrator.Api's document ID: use it in complete, failure and renew.
	ID          string `json:"id"`
	RequestGUID string `json:"requestGuid"`
	// Operation of the task, e.g. REGISTER_COMPARE; part of the storage path.
	Operation string `json:"operation"`
	// BPMDocumentID is BPM's ID of the file, for logging only.
	BPMDocumentID string `json:"bpmDocumentId"`
	// AttstoreURL is where to download the file from.
	AttstoreURL string `json:"attstoreUrl"`
	// Position of the file in the task; AI receives files in this order.
	Position   int       `json:"position"`
	ClaimToken string    `json:"claimToken"`
	ClaimUntil time.Time `json:"claimUntil"`
}

// CopyResult describes the copy stored in S3.
type CopyResult struct {
	S3URI          string `json:"s3Uri"`
	SizeBytes      int64  `json:"sizeBytes"`
	ChecksumSHA256 string `json:"checksumSha256"`
}

// ClaimDocuments takes up to limit documents to copy. Empty when there is nothing to do.
func (c *Client) ClaimDocuments(ctx context.Context, limit int) ([]ClaimedDocument, error) {
	var resp struct {
		Items []ClaimedDocument `json:"items"`
	}
	err := c.post(ctx, "/internal/v1/documents/claim", claimRequest{WorkerID: c.cfg.WorkerID, Limit: limit}, &resp)
	return resp.Items, err
}

// CompleteDocument reports a finished copy. Repeating it with the same result is safe.
func (c *Client) CompleteDocument(ctx context.Context, d ClaimedDocument, result CopyResult) error {
	body := struct {
		ClaimToken string `json:"claimToken"`
		CopyResult
	}{d.ClaimToken, result}
	return c.post(ctx, "/internal/v1/documents/"+d.ID+"/complete", body, nil)
}

// FailDocument reports a failed copy. retryable says whether trying again
// later may help; Integrator.Api applies its own retry limit.
func (c *Client) FailDocument(ctx context.Context, d ClaimedDocument, failure Failure, retryable bool) error {
	body := struct {
		ClaimToken string `json:"claimToken"`
		Code       string `json:"code"`
		Message    string `json:"message"`
		Retryable  bool   `json:"retryable"`
	}{d.ClaimToken, failure.Code, failure.Message, retryable}
	return c.post(ctx, "/internal/v1/documents/"+d.ID+"/failure", body, nil)
}

// RenewDocument extends the claim while copying and returns the new expiry.
func (c *Client) RenewDocument(ctx context.Context, d ClaimedDocument) (time.Time, error) {
	var resp renewResponse
	err := c.post(ctx, "/internal/v1/documents/"+d.ID+"/renew", renewRequest{ClaimToken: d.ClaimToken}, &resp)
	return resp.ClaimUntil, err
}
