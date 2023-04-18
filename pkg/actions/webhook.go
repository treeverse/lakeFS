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

func NewWebhook(h ActionHook, action *Action, cfg Config, e *http.Server) (Hook, error) {
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
			Config:     cfg,
			Endpoint:   e,
		},
		Timeout:     requestTimeout,
		URL:         webhookURL,
		QueryParams: queryParams,
		Headers:     headers,
	}, nil
}

func (w *Webhook) Run(ctx context.Context, record graveler.HookRecord, buf *bytes.Buffer) (err error) {
	// post event information as json to webhook endpoint
	logging.FromContext(ctx).
		WithField("hook_type", "webhook").
		WithField("event_type", record.EventType).
		Debug("hook action executing")

	eventData, err := marshalEventInformation(w.ActionName, w.ID, record)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(buf, "Request:\n%s %s\n", http.MethodPost, w.URL)
	reqReader := bytes.NewReader(eventData)
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
			_, _ = fmt.Fprintf(buf, "%s: %s\n", k, v.String())
		}
	}

	buf.WriteString("Headers:\n")
	for k, v := range w.Headers {
		req.Header.Add(k, v.val)
		_, _ = fmt.Fprintf(buf, "%s: %s\n", k, v.String())
	}
	req.URL.RawQuery = q.Encode()

	_, _ = fmt.Fprintf(buf, "Request Body:\n%s\n\n", eventData)

	statusCode, err := doHTTPRequestWithLog(ctx, req, buf, w.Timeout)
	if err != nil {
		return err
	}

	// check status code
	if statusCode < 200 || statusCode >= 300 {
		return fmt.Errorf("%w (status code: %d)", errWebhookRequestFailed, statusCode)
	}
	return nil
}

// doHTTPRequestWithLog helper that uses 'doHTTPRequestResponseWithLog' without response parse
func doHTTPRequestWithLog(ctx context.Context, req *http.Request, buf *bytes.Buffer, timeout time.Duration) (n int, err error) {
	return doHTTPRequestResponseWithLog(ctx, req, nil, buf, timeout)
}

// doHTTPRequestResponseWithLog execute a http request with specified timeout. Output variable 'respJSON', if set, used to json decode the response.
// returns the response status code or -1 on error
func doHTTPRequestResponseWithLog(ctx context.Context, req *http.Request, respJSON interface{}, buf *bytes.Buffer, timeout time.Duration) (int, error) {
	req = req.WithContext(ctx)

	client := &http.Client{
		Timeout: timeout,
	}
	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	_, _ = fmt.Fprintf(buf, "\nRequest duration: %s\n", elapsed)
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
		_, _ = fmt.Fprintf(buf, "Failed dumping response: %s", err)
	}
	if respJSON != nil {
		err = json.NewDecoder(resp.Body).Decode(&respJSON)
		if err != nil {
			return -1, err
		}
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
