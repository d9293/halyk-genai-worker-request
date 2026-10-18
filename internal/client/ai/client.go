// Package ai sends tasks to the AI team's services, one adapter per
// operation. Each task is POSTed exactly once: after any doubt (timeout, lost
// answer) it is never sent again here; Integrator.Api finds out by polling.
package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Outcome values, as Integrator.Api expects them.
const (
	OutcomeAccepted = "ACCEPTED"
	OutcomeUnknown  = "UNKNOWN"
	OutcomeRejected = "REJECTED"
)

// Failure codes.
const (
	// AI refused the request as invalid (422).
	CodeAIValidationError = "AI_VALIDATION_ERROR"
	// AI already has a task with this task_id (409): an earlier POST probably got through.
	CodeAIDuplicateTask = "AI_DUPLICATE_TASK"
	// No answer in time; AI may have the task.
	CodeAITimeout = "AI_TIMEOUT"
	// AI could not be reached or answered unexpectedly; it may have the task.
	CodeAIUnavailable = "AI_UNAVAILABLE"
	// AI accepted a task, but under another task_id.
	CodeRemoteTaskIDMismatch = "REMOTE_TASK_ID_MISMATCH"
	// Found before calling AI: the worker cannot send this operation or input.
	CodeOperationUnsupported = "WORKER_OPERATION_UNSUPPORTED"
	CodeInvalidPreparedInput = "INVALID_PREPARED_INPUT"
)

const maxMessageLen = 2000

// Submission is one task to send.
type Submission struct {
	// TaskID is the attempt ID; AI knows the task by it.
	TaskID    string
	Operation string
	// Input is the operation's input as BPM registered it.
	Input json.RawMessage
	// Files are the S3 links of the documents, in the order AI must receive them.
	Files []string
}

// Result is what the POST led to, ready to report to Integrator.Api.
type Result struct {
	Outcome string
	// RemoteStatus is AI's status of an accepted task: QUEUED or PROCESSING.
	RemoteStatus   string
	FailureCode    string
	FailureMessage string
}

// operation builds the request body of one AI service.
type operation interface {
	// path of the POST under the operation's base URL.
	path() string
	// body builds the JSON body, or fails if the input is unusable.
	body(s Submission) (any, error)
}

type Config struct {
	// BaseURLs per operation code, e.g. REGISTER_COMPARE → http://localhost:8081/register-compare.
	BaseURLs map[string]string
	// Timeout of one POST.
	Timeout time.Duration
}

type Client struct {
	baseURLs   map[string]string
	operations map[string]operation
	http       *http.Client
}

func New(cfg Config) (*Client, error) {
	c := &Client{
		baseURLs: make(map[string]string),
		operations: map[string]operation{
			"REGISTER_COMPARE": registerCompare{},
			"SOCIAL_MEDIA":     socialMedia{},
		},
		http: &http.Client{Timeout: cfg.Timeout},
	}
	for op := range c.operations {
		base := strings.TrimRight(cfg.BaseURLs[op], "/")
		if base == "" {
			return nil, fmt.Errorf("no AI base URL configured for %s", op)
		}
		c.baseURLs[op] = base
	}
	return c, nil
}

// Submit sends the task once and says what happened.
func (c *Client) Submit(ctx context.Context, s Submission) Result {
	op, ok := c.operations[s.Operation]
	if !ok {
		return rejected(CodeOperationUnsupported, "The worker cannot send operation "+s.Operation)
	}
	body, err := op.body(s)
	if err != nil {
		return rejected(CodeInvalidPreparedInput, err.Error())
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return rejected(CodeInvalidPreparedInput, err.Error())
	}

	// From here on the request may reach AI: no outcome below may be REJECTED
	// unless AI itself refused it.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURLs[s.Operation]+op.path(), bytes.NewReader(payload))
	if err != nil {
		return rejected(CodeInvalidPreparedInput, err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return unknown(CodeAITimeout, "No answer from AI in time; it may have the task")
		}
		return unknown(CodeAIUnavailable, "AI could not be reached: "+err.Error())
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return interpret(resp.StatusCode, data, s.TaskID)
}

// interpret maps AI's answer to an outcome. Only a clear refusal is REJECTED;
// anything unclear is UNKNOWN, because AI may have accepted the task.
func interpret(status int, data []byte, taskID string) Result {
	switch {
	case status == http.StatusAccepted || status == http.StatusOK || status == http.StatusCreated:
		var accepted struct {
			TaskID string `json:"task_id"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(data, &accepted); err != nil {
			return unknown(CodeAIUnavailable, "AI accepted with an unreadable answer")
		}
		if accepted.TaskID != taskID {
			return unknown(CodeRemoteTaskIDMismatch, fmt.Sprintf("AI accepted task_id %q, sent %q", accepted.TaskID, taskID))
		}
		remote := "QUEUED"
		if strings.EqualFold(accepted.Status, "processing") {
			remote = "PROCESSING"
		}
		return Result{Outcome: OutcomeAccepted, RemoteStatus: remote}
	case status == http.StatusConflict:
		return unknown(CodeAIDuplicateTask, "AI already has a task with this task_id")
	case status == http.StatusUnprocessableEntity:
		return rejected(CodeAIValidationError, "AI refused the request: "+detail(data))
	}
	return unknown(CodeAIUnavailable, fmt.Sprintf("AI answered HTTP %d: %s", status, detail(data)))
}

// detail reads FastAPI's {"detail": "..."} or {"detail": [{"msg": "..."}]}.
func detail(data []byte) string {
	var body struct {
		Detail json.RawMessage `json:"detail"`
	}
	if json.Unmarshal(data, &body) != nil || len(body.Detail) == 0 {
		return truncate(strings.TrimSpace(string(data)))
	}
	var text string
	if json.Unmarshal(body.Detail, &text) == nil {
		return truncate(text)
	}
	var items []struct {
		Loc []any  `json:"loc"`
		Msg string `json:"msg"`
	}
	if json.Unmarshal(body.Detail, &items) == nil {
		parts := make([]string, 0, len(items))
		for _, it := range items {
			parts = append(parts, fmt.Sprintf("%v: %s", it.Loc, it.Msg))
		}
		return truncate(strings.Join(parts, "; "))
	}
	return truncate(string(body.Detail))
}

func truncate(s string) string {
	if r := []rune(s); len(r) > maxMessageLen {
		return string(r[:maxMessageLen])
	}
	return s
}

func rejected(code, message string) Result {
	return Result{Outcome: OutcomeRejected, FailureCode: code, FailureMessage: truncate(message)}
}

func unknown(code, message string) Result {
	return Result{Outcome: OutcomeUnknown, FailureCode: code, FailureMessage: truncate(message)}
}
