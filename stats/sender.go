package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/logging"
)

var (
	ErrSendError        = errors.New("stats: send error")
	ErrNoInstallationID = fmt.Errorf("installation ID is missing: %w", ErrSendError)
)

type Sender interface {
	SendEvent(ctx context.Context, installationID, cloudProviderAccountID string, processID string, m []Metric) error
	UpdateMetadata(ctx context.Context, m Metadata) error
}

type TimeFn func() time.Time

type HTTPSender struct {
	timeFunc TimeFn
	addr     string
}

func NewHTTPSender(addr string, timeFunc TimeFn) *HTTPSender {
	return &HTTPSender{
		timeFunc: timeFunc,
		addr:     addr,
	}
}

func (s *HTTPSender) UpdateMetadata(ctx context.Context, m Metadata) error {
	if len(m.InstallationID) == 0 {
		return ErrNoInstallationID
	}
	serialized, err := json.MarshalIndent(m, "", "  ")
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

func (s *HTTPSender) SendEvent(ctx context.Context, installationID, cloudProviderAccountID, processID string, metrics []Metric) error {
	if len(installationID) == 0 {
		return ErrNoInstallationID
	}

	event := &InputEvent{
		InstallationID:         installationID,
		CloudProviderAccountID: cloudProviderAccountID,
		ProcessID:              processID,
		Time:                   s.timeFunc().Format(time.RFC3339),
		Metrics:                metrics,
	}
	serialized, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("could not serialize event: %s: %w", err, ErrSendError)
	}

	req, err := http.NewRequest(http.MethodPost, s.addr+"/events", bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("could not create HTTP request: %s: %w", err, ErrSendError)
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
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

type dummySender struct{}

func (s *dummySender) SendEvent(_ context.Context, installationID, cloudProviderAccountID, processID string, metrics []Metric) error {
	logging.Default().WithFields(logging.Fields{
		"installation_id":           installationID,
		"cloud_provider_account_id": cloudProviderAccountID,
		"process_id":                processID,
		"metrics":                   spew.Sdump(metrics),
	}).Trace("dummy sender received metrics")
	return nil
}

func (s *dummySender) UpdateMetadata(ctx context.Context, m Metadata) error {
	logging.Default().WithFields(logging.Fields{
		"metadata": spew.Sdump(m),
	}).Trace("dummy sender received metadata")
	return nil
}

func NewDummySender() Sender {
	return &dummySender{}
}
