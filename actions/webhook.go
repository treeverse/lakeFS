package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/graveler"
)

type Webhook struct {
	ID          string
	ActionName  string
	URL         string
	Timeout     time.Duration
	QueryParams map[string][]string
}

type WebhookEventInfo struct {
	EventType      string            `json:"event_type"`
	EventTime      string            `json:"event_time"`
	ActionName     string            `json:"action_name"`
	HookID         string            `json:"hook_id"`
	RepositoryID   string            `json:"repository_id"`
	BranchID       string            `json:"branch_id"`
	SourceRef      string            `json:"source_ref,omitempty"`
	CommitMessage  string            `json:"commit_message"`
	Committer      string            `json:"committer"`
	CommitMetadata map[string]string `json:"commit_metadata,omitempty"`
}

const (
	webhookClientTimeout      = 1 * time.Minute
	webhookTimeoutPropertyKey = "timeout"
	queryParamsPropertyKey    = "query_params"
)

var (
	ErrWebhookRequestFailed = errors.New("webhook request failed")
	ErrWebhookWrongFormat   = errors.New("webhook wrong format")
)

func NewWebhook(h ActionHook, action *Action) (Hook, error) {
	webhookURL := h.Properties["url"].(string)
	if len(webhookURL) == 0 {
		return nil, fmt.Errorf("missing url: %w", ErrWebhookWrongFormat)
	}

	queryParams, err := extractQueryParams(h.Properties)
	if err != nil {
		return nil, fmt.Errorf("extracting query params: %w", err)
	}

	requestTimeout := webhookClientTimeout
	timeoutDuration, ok := h.Properties[webhookTimeoutPropertyKey]
	if ok && len(timeoutDuration.(string)) > 0 {
		d, err := time.ParseDuration(timeoutDuration.(string))
		if err != nil {
			return nil, fmt.Errorf("webhook request duration: %w", err)
		}
		requestTimeout = d
	}
	return &Webhook{
		ID:          h.ID,
		ActionName:  action.Name,
		Timeout:     requestTimeout,
		URL:         webhookURL,
		QueryParams: queryParams,
	}, nil
}

func (w *Webhook) Run(ctx context.Context, record graveler.HookRecord, writer *HookOutputWriter) error {
	// post event information as json to webhook endpoint
	eventData, err := w.marshalEventInformation(record)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, w.URL, bytes.NewReader(eventData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	q := req.URL.Query()
	for k, vals := range w.QueryParams {
		for _, v := range vals {
			q.Add(k, v)
		}
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{
		Timeout: w.Timeout,
	}
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// log response body if needed
	if resp.Body != nil && resp.ContentLength != 0 {
		if err := writer.OutputWrite(ctx, resp.Body, resp.ContentLength); err != nil {
			return err
		}
	}

	// check status code
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w (status code: %d)", ErrWebhookRequestFailed, resp.StatusCode)
	}
	return nil
}

func (w *Webhook) marshalEventInformation(record graveler.HookRecord) ([]byte, error) {
	now := time.Now()
	info := WebhookEventInfo{
		EventType:      string(record.EventType),
		EventTime:      now.UTC().Format(time.RFC3339),
		ActionName:     w.ActionName,
		HookID:         w.ID,
		RepositoryID:   record.RepositoryID.String(),
		BranchID:       record.BranchID.String(),
		SourceRef:      record.SourceRef.String(),
		CommitMessage:  record.Commit.Message,
		Committer:      record.Commit.Committer,
		CommitMetadata: record.Commit.Metadata,
	}
	return json.Marshal(info)
}

func extractQueryParams(props map[string]interface{}) (map[string][]string, error) {
	params, ok := props[queryParamsPropertyKey]
	if !ok {
		return nil, nil
	}

	paramsMap, ok := params.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unsupported query params: %w", ErrWebhookWrongFormat)
	}

	res := map[string][]string{}
	for k, v := range paramsMap {
		if ar, ok := v.([]interface{}); ok {
			for _, v := range ar {
				res[k] = append(res[k], v.(string))
			}
			continue
		}
		res[k] = []string{v.(string)}
	}

	return res, nil
}
