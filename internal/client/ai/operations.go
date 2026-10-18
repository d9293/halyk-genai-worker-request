package ai

import (
	"encoding/json"
	"errors"
	"fmt"
)

// registerCompare is the AI team's Reestr service (ud-registers-compare 1.0.0):
// POST /api/v1/compare {"task_id", "files": [2 links], "adjudicate"}.
type registerCompare struct{}

func (registerCompare) path() string { return "/api/v1/compare" }

func (registerCompare) body(s Submission) (any, error) {
	var in struct {
		Adjudicate *bool `json:"adjudicate"`
	}
	if err := json.Unmarshal(s.Input, &in); err != nil {
		return nil, fmt.Errorf("register-compare input: %w", err)
	}
	if len(s.Files) != 2 {
		return nil, fmt.Errorf("register-compare needs exactly 2 files, got %d", len(s.Files))
	}
	adjudicate := in.Adjudicate == nil || *in.Adjudicate
	return struct {
		TaskID     string   `json:"task_id"`
		Files      []string `json:"files"`
		Adjudicate bool     `json:"adjudicate"`
	}{s.TaskID, s.Files, adjudicate}, nil
}

// socialMedia is a social media lookup service. The AI team has not described
// it yet; this follows the mock (POST /api/v1/search {"task_id", "iin", "full_name"}).
type socialMedia struct{}

func (socialMedia) path() string { return "/api/v1/search" }

func (socialMedia) body(s Submission) (any, error) {
	var in struct {
		IIN      string `json:"iin"`
		FullName string `json:"fullName"`
	}
	if err := json.Unmarshal(s.Input, &in); err != nil {
		return nil, fmt.Errorf("social-media input: %w", err)
	}
	if in.IIN == "" || in.FullName == "" {
		return nil, errors.New("social-media input needs iin and fullName")
	}
	return struct {
		TaskID   string `json:"task_id"`
		IIN      string `json:"iin"`
		FullName string `json:"full_name"`
	}{s.TaskID, in.IIN, in.FullName}, nil
}
