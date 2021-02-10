package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Webhook struct {
	ID         string
	ActionName string
	URL        string
}

type WebhookCommitInfo struct {
	Message   string            `json:"message"`
	Committer string            `json:"committer"`
	Metadata  map[string]string `json:"metadata"`
}

type WebhookEventInfo struct {
	RunID         string            `json:"run_id"`
	EventType     string            `json:"event_type"`
	EventTime     string            `json:"event_time"`
	ActionName    string            `json:"action_name"`
	HookID        string            `json:"hook_id"`
	RepositoryID  string            `json:"repository_id"`
	BranchID      string            `json:"branch_id"`
	SourceRef     string            `json:"source_ref"`
	CommitMessage string            `json:"commit_message"`
	Committer     string            `json:"committer"`
	Metadata      map[string]string `json:"metadata"`
}

var ErrWebhookResponseStatusCode = errors.New("webhook non-success status code")

func NewWebhook(action *Action, h ActionHook) Hook {
	return &Webhook{
		ID:         h.ID,
		ActionName: action.Name,
		URL:        h.Properties["url"],
	}
}

func (w *Webhook) Run(ctx context.Context, ed Event, writer OutputWriter) error {
	// post event information as json to webhook endpoint
	eventData, err := w.marshalEvent(ed)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, w.URL, bytes.NewReader(eventData))
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: time.Minute,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// log response body if needed
	if resp.Body != nil && resp.ContentLength > 0 {
		if err := writer.OutputWrite(ctx, w.ID, resp.Body); err != nil {
			return err
		}
	}

	// check status code
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w (%d)", ErrWebhookResponseStatusCode, resp.StatusCode)
	}
	return nil
}

func (w *Webhook) marshalEvent(ed Event) ([]byte, error) {
	info := WebhookEventInfo{
		RunID:         ed.RunID,
		EventType:     string(ed.EventType),
		EventTime:     time.Now().UTC().Format(time.RFC3339),
		ActionName:    w.ActionName,
		HookID:        w.ID,
		RepositoryID:  ed.RepositoryID,
		BranchID:      ed.BranchID,
		SourceRef:     ed.SourceRef,
		CommitMessage: ed.CommitMessage,
		Committer:     ed.Committer,
		Metadata:      ed.Metadata,
	}
	return json.Marshal(info)
}
