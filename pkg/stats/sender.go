package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrSendError        = errors.New("stats: send error")
	ErrNoInstallationID = fmt.Errorf("installation ID is missing: %w", ErrSendError)
)

type Sender interface {
	SendEvent(ctx context.Context, event *InputEvent) error
	UpdateMetadata(ctx context.Context, m Metadata) error
	UpdateCommPrefs(ctx context.Context, commPrefs *CommPrefsData) error
}

type TimeFn func() time.Time

type HTTPSender struct {
	addr   string
	client *http.Client
}

type LoggerAdapter struct {
	logging.Logger
}

func (l *LoggerAdapter) Printf(msg string, args ...interface{}) {
	l.Debugf(msg, args...)
}

func NewHTTPSender(addr string, log logging.Logger) *HTTPSender {
	retryClient := retryablehttp.NewClient()
	retryClient.Logger = &LoggerAdapter{Logger: log}
	return &HTTPSender{
		addr:   addr,
		client: retryClient.StandardClient(),
	}
}

// IsSuccessStatusCode returns true for status code 2xx
func IsSuccessStatusCode(statusCode int) bool {
	return statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices
}

func (s *HTTPSender) UpdateMetadata(ctx context.Context, m Metadata) error {
	if len(m.InstallationID) == 0 {
		return ErrNoInstallationID
	}
	serialized, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to serialize account metadata: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, s.addr+"/installation", bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("could not create HTTP request: %s: %w", err, ErrSendError)
	}
	req = req.WithContext(ctx)
	res, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not make HTTP request: %s: %w", err, ErrSendError)
	}
	defer func() { _ = res.Body.Close() }()
	if !IsSuccessStatusCode(res.StatusCode) {
		return fmt.Errorf("request failed - status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

func (s *HTTPSender) SendEvent(ctx context.Context, event *InputEvent) error {
	serialized, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("could not serialize event: %s: %w", err, ErrSendError)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.addr+"/events", bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("could not create HTTP request: %s: %w", err, ErrSendError)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not make HTTP request: %s: %w", err, ErrSendError)
	}
	defer func() { _ = res.Body.Close() }()
	if !IsSuccessStatusCode(res.StatusCode) {
		return fmt.Errorf("request failed - status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

func (s *HTTPSender) UpdateCommPrefs(ctx context.Context, commPrefs *CommPrefsData) error {
	serialized, err := json.Marshal(commPrefs)
	if err != nil {
		return fmt.Errorf("could not serialize comm prefs: %s: %w", err, ErrSendError)
	}

	req, err := http.NewRequest(http.MethodPost, s.addr+"/comm_prefs", bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("could not create HTTP request: %s: %w", err, ErrSendError)
	}
	req = req.WithContext(ctx)
	res, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not make HTTP request: %s: %w", err, ErrSendError)
	}
	defer func() { _ = res.Body.Close() }()
	if !IsSuccessStatusCode(res.StatusCode) {
		return fmt.Errorf("request failed - status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

type dummySender struct {
	logging.Logger
}

func (s *dummySender) SendEvent(_ context.Context, event *InputEvent) error {
	if s.Logger == nil || !s.IsTracing() {
		return nil
	}
	s.WithFields(logging.Fields{
		"installation_id": event.InstallationID,
		"process_id":      event.ProcessID,
		"event_time":      event.Time,
		"metrics":         fmt.Sprintf("%+v", event.Metrics),
	}).Trace("dummy sender received metrics")
	return nil
}

func (s *dummySender) UpdateMetadata(_ context.Context, m Metadata) error {
	if s.Logger == nil || !s.IsTracing() {
		return nil
	}
	s.WithFields(logging.Fields{
		"metadata": fmt.Sprintf("%+v", m),
	}).Trace("dummy sender received metadata")
	return nil
}

func (s *dummySender) UpdateCommPrefs(_ context.Context, commPrefs *CommPrefsData) error {
	if s.Logger == nil || !s.IsTracing() {
		return nil
	}
	s.WithFields(logging.Fields{
		"email":           commPrefs.Email,
		"featureUpdates":  commPrefs.FeatureUpdates,
		"securityUpdates": commPrefs.SecurityUpdates,
		"installationID":  commPrefs.InstallationID,
	}).Trace("dummy sender received comm prefs")
	return nil
}
