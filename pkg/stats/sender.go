package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrSendError        = errors.New("stats: send error")
	ErrNoInstallationID = fmt.Errorf("installation ID is missing: %w", ErrSendError)
)

type Sender interface {
	SendEvent(ctx context.Context, installationID, processID string, m []Metric) error
	UpdateMetadata(ctx context.Context, m Metadata) error
	UpdateCommPrefs(ctx context.Context, email, installationID string, featureUpdates, securityUpdates bool) error
}

type TimeFn func() time.Time

type HTTPSender struct {
	addr        string
	sendTimeout time.Duration
	timeFunc    TimeFn
	client      http.Client
}

func NewHTTPSender(addr string, timeout time.Duration, timeFunc TimeFn) *HTTPSender {
	client := http.Client{
		Timeout: timeout,
	}
	return &HTTPSender{
		addr:        addr,
		sendTimeout: timeout,
		timeFunc:    timeFunc,
		client:      client,
	}
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
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("could not make HTTP request: %s: %w", err, ErrSendError)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad status code received. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

func (s *HTTPSender) SendEvent(ctx context.Context, installationID, processID string, metrics []Metric) error {
	event := &InputEvent{
		InstallationID: installationID,
		ProcessID:      processID,
		Time:           s.timeFunc().Format(time.RFC3339),
		Metrics:        metrics,
	}
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
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad status code received. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

func (s *HTTPSender) UpdateCommPrefs(ctx context.Context, email, installationID string, featureUpdates, securityUpdates bool) error {
	commPrefs := &CommPrefsData{
		InstallationID:  installationID,
		Email:           email,
		FeatureUpdates:  featureUpdates,
		SecurityUpdates: securityUpdates,
	}
	serialized, err := json.Marshal(commPrefs)
	if err != nil {
		return fmt.Errorf("could not serialize comm prefs: %s: %w", err, ErrSendError)
	}

	req, err := http.NewRequest(http.MethodPost, s.addr+"/comm_prefs", bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("could not create HTTP request: %s: %w", err, ErrSendError)
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("could not make HTTP request: %s: %w", err, ErrSendError)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad status code received. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

type dummySender struct {
	Log logging.Logger
}

func (s *dummySender) SendEvent(_ context.Context, installationID, processID string, metrics []Metric) error {
	if s.Log == nil || !s.Log.IsTracing() {
		return nil
	}
	s.Log.WithFields(logging.Fields{
		"installation_id": installationID,
		"process_id":      processID,
		"metrics":         fmt.Sprintf("%+v", metrics),
	}).Trace("dummy sender received metrics")
	return nil
}

func (s *dummySender) UpdateMetadata(_ context.Context, m Metadata) error {
	if s.Log == nil || !s.Log.IsTracing() {
		return nil
	}
	s.Log.WithFields(logging.Fields{
		"metadata": fmt.Sprintf("%+v", m),
	}).Trace("dummy sender received metadata")
	return nil
}

func (s *dummySender) UpdateCommPrefs(ctx context.Context, email, installationID string, featureUpdates, securityUpdates bool) error {
	if s.Log == nil || !s.Log.IsTracing() {
		return nil
	}
	s.Log.WithFields(logging.Fields{
		"email":           email,
		"featureUpdates":  featureUpdates,
		"securityUpdates": securityUpdates,
	}).Trace("dummy sender received comm prefs")
	return nil
}
