package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Webhook struct {
	HookBase
	URL         string
	Timeout     time.Duration
	QueryParams map[string][]SecureString
	Headers     map[string]SecureString
}

const (
	webhookClientDefaultTimeout = 1 * time.Minute
	webhookTimeoutPropertyKey   = "timeout"
	webhookURLPropertyKey       = "url"
	queryParamsPropertyKey      = "query_params"
	HeadersPropertyKey          = "headers"
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

	headers, err := extractHeaders(h.Properties)
	if err != nil {
		return nil, fmt.Errorf("extracting headers: %w", err)
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
		Headers:     headers,
	}, nil
}

func (w *Webhook) Run(ctx context.Context, record graveler.HookRecord, writer *HookOutputWriter) (err error) {
	// post event information as json to webhook endpoint
	logging.FromContext(ctx).
		WithField("hook_type", "webhook").
		WithField("event_type", record.EventType).
		Debug("hook action executing")

	eventData, err := marshalEventInformation(w.ActionName, w.ID, record)
	if err != nil {
		return err
	}

	reqReader := bytes.NewReader(eventData)
	buf := bytes.NewBufferString(fmt.Sprintf("Request:\nPOST %s\n", w.URL))

	req, err := http.NewRequest(http.MethodPost, w.URL, reqReader)
	if err != nil {
		return err
	}
	w.Headers["Content-Type"] = SecureString{val: "application/json"}

	buf.WriteString("Query Params:\n")
	q := req.URL.Query()
	for k, vals := range w.QueryParams {
		for _, v := range vals {
			q.Add(k, v.val)
			buf.WriteString(fmt.Sprintf("%s: %s\n", k, v.String()))
		}
	}

	buf.WriteString("Headers:\n")
	for k, v := range w.Headers {
		req.Header.Add(k, v.val)
		buf.WriteString(fmt.Sprintf("%s: %s\n", k, v.String()))
	}
	req.URL.RawQuery = q.Encode()

	buf.WriteString(fmt.Sprintf("Request Body:\n%s\n\n", eventData))

	statusCode, err := executeAndLogResponse(ctx, req, buf, writer, w.Timeout)
	if err != nil {
		return err
	}

	// check status code
	if statusCode < 200 || statusCode >= 300 {
		return fmt.Errorf("%w (status code: %d)", errWebhookRequestFailed, statusCode)
	}
	return nil
}

func executeAndLogResponse(ctx context.Context, req *http.Request, buf *bytes.Buffer, writer *HookOutputWriter, timeout time.Duration) (n int, err error) {
	req = req.WithContext(ctx)

	client := &http.Client{
		Timeout: timeout,
	}
	start := time.Now()

	defer func() {
		err2 := writer.OutputWrite(ctx, buf, int64(buf.Len()))
		if err == nil {
			err = err2
		}
	}()

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

func extractQueryParams(props map[string]interface{}) (map[string][]SecureString, error) {
	params, ok := props[queryParamsPropertyKey]
	if !ok {
		return nil, nil
	}

	paramsMap, ok := params.(Properties)
	if !ok {
		return nil, fmt.Errorf("unsupported query params: %w", errWebhookWrongFormat)
	}

	res := map[string][]SecureString{}
	for k, v := range paramsMap {
		if ar, ok := v.([]interface{}); ok {
			for _, v := range ar {
				av, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("query params array should contains only strings: %w", errWebhookWrongFormat)
				}

				avs, err := NewSecureString(av)
				if err != nil {
					return nil, fmt.Errorf("reading query param: %w", err)
				}
				res[k] = append(res[k], avs)
			}
			continue
		}
		av, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("query params single value should be of type string: %w", errWebhookWrongFormat)
		}

		avs, err := NewSecureString(av)
		if err != nil {
			return nil, fmt.Errorf("reading query param: %w", err)
		}
		res[k] = []SecureString{avs}
	}

	return res, nil
}

func extractHeaders(props map[string]interface{}) (map[string]SecureString, error) {
	params, ok := props[HeadersPropertyKey]
	if !ok {
		return map[string]SecureString{}, nil
	}

	paramsMap, ok := params.(Properties)
	if !ok {
		return nil, fmt.Errorf("unsupported headers: %w", errWebhookWrongFormat)
	}

	res := map[string]SecureString{}
	for k, v := range paramsMap {
		vs, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("headers array should contains only strings: %w", errWebhookWrongFormat)
		}

		vss, err := NewSecureString(vs)
		if err != nil {
			return nil, fmt.Errorf("reading header: %w", err)
		}
		res[k] = vss
	}

	return res, nil
}
