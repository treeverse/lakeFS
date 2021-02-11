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
	Timeout    time.Duration
}

type WebhookEventInfo struct {
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

const webhookClientTimeout = 5 * time.Minute

var (
	ErrWebhookRequestFailed = errors.New("webhook request failed")
	ErrWebhookMissingURL    = errors.New("webhook missing url")
)

func NewWebhook(h ActionHook, action *Action) (Hook, error) {
	webhookURL := h.Properties["url"]
	if len(webhookURL) == 0 {
		return nil, ErrWebhookMissingURL
	}

	requestTimeout := webhookClientTimeout
	timeoutDuration := h.Properties["timeout"]
	if len(timeoutDuration) > 0 {
		d, err := time.ParseDuration(timeoutDuration)
		if err != nil {
			return nil, fmt.Errorf("webhook request duration: %w", err)
		}
		requestTimeout = d
	}
	return &Webhook{
		ID:         h.ID,
		ActionName: action.Name,
		Timeout:    requestTimeout,
		URL:        webhookURL,
	}, nil
}

func (w *Webhook) Run(ctx context.Context, event Event) error {
	// post event information as json to webhook endpoint
	eventData, err := w.marshalEventInformation(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, w.URL, bytes.NewReader(eventData))
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: w.Timeout,
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
		if err := event.Output.OutputWrite(ctx, w.ID, resp.Body); err != nil {
			return err
		}
	}

	// check status code
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w (status code: %d)", ErrWebhookRequestFailed, resp.StatusCode)
	}
	return nil
}

func (w *Webhook) marshalEventInformation(ed Event) ([]byte, error) {
	now := time.Now()
	info := WebhookEventInfo{
		EventType:     string(ed.EventType),
		EventTime:     now.UTC().Format(time.RFC3339),
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
