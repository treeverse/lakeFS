package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type Webhook struct {
	HookBase
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
	webhookClientDefaultTimeout = 1 * time.Minute
	webhookTimeoutPropertyKey   = "timeout"
	webhookURLPropertyKey       = "url"
	queryParamsPropertyKey      = "query_params"
)

var (
	errWebhookRequestFailed = errors.New("webhook request failed")
	errWebhookWrongFormat   = errors.New("webhook wrong format")
)

func NewWebhook(h ActionHook, action *Action) (Hook, error) {
	url, ok := h.Properties[webhookURLPropertyKey]
	if !ok {
		return nil, fmt.Errorf("missing url: %w", errWebhookWrongFormat)
	}
	webhookURL, ok := url.(string)
	if !ok {
		return nil, fmt.Errorf("webhook url must be string: %w", errWebhookWrongFormat)
	}

	queryParams, err := extractQueryParams(h.Properties)
	if err != nil {
		return nil, fmt.Errorf("extracting query params: %w", err)
	}

	requestTimeout := webhookClientDefaultTimeout
	if timeoutDuration, ok := h.Properties[webhookTimeoutPropertyKey]; ok {
		if timeout, ok := timeoutDuration.(string); ok && len(timeout) > 0 {
			d, err := time.ParseDuration(timeout)
			if err != nil {
				return nil, fmt.Errorf("webhook request duration: %w", err)
			}
			requestTimeout = d
		}
	}

	return &Webhook{
		HookBase: HookBase{
			ID:         h.ID,
			ActionName: action.Name,
		},
		Timeout:     requestTimeout,
		URL:         webhookURL,
		QueryParams: queryParams,
	}, nil
}

func (w *Webhook) Run(ctx context.Context, record graveler.HookRecord, writer *HookOutputWriter) (err error) {
	// post event information as json to webhook endpoint
	eventData, err := w.marshalEventInformation(record)
	if err != nil {
		return err
	}

	reqReader := bytes.NewReader(eventData)
	req, err := http.NewRequest(http.MethodPost, w.URL, reqReader)
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
	statusCode, err := executeAndLogHTTP(ctx, req, writer, w.Timeout)
	if err != nil {
		return err
	}

	// check status code
	if statusCode < 200 || statusCode >= 300 {
		return fmt.Errorf("%w (status code: %d)", errWebhookRequestFailed, statusCode)
	}
	return nil
}

func executeAndLogHTTP(ctx context.Context, req *http.Request, writer *HookOutputWriter, timeout time.Duration) (int, error) {
	req = req.WithContext(ctx)

	client := &http.Client{
		Timeout: timeout,
	}
	start := time.Now()

	var err error
	buf := bytes.NewBufferString("Request:\n")
	defer func() {
		err2 := writer.OutputWrite(ctx, buf, int64(buf.Len()))
		if err == nil {
			err = err2
		}
	}()

	if dumpReq, err := httputil.DumpRequestOut(req, true); err == nil {
		buf.Write(dumpReq)
	} else {
		buf.WriteString(fmt.Sprintf("Failed dumping request: %s", err))
	}

	resp, err := client.Do(req)
	elapsed := time.Since(start)
	buf.WriteString(fmt.Sprintf("\nRequest duration: %s\n", elapsed))
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	buf.WriteString("\nResponse:\n")
	if dumpResp, err := httputil.DumpResponse(resp, true); err == nil {
		buf.Write(dumpResp)
	} else {
		buf.WriteString(fmt.Sprintf("Failed dumping response: %s", err))
	}

	return resp.StatusCode, nil
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

	paramsMap, ok := params.(Properties)
	if !ok {
		return nil, fmt.Errorf("unsupported query params: %w", errWebhookWrongFormat)
	}

	res := map[string][]string{}
	for k, v := range paramsMap {
		if ar, ok := v.([]interface{}); ok {
			for _, v := range ar {
				av, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("query params array should contains only strings: %w", errWebhookWrongFormat)
				}

				res[k] = append(res[k], av)
			}
			continue
		}
		av, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("query params single value should be of type string: %w", errWebhookWrongFormat)
		}
		res[k] = []string{av}
	}

	return res, nil
}
