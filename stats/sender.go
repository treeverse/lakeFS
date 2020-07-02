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

var ErrSendError = errors.New("stats: send error")

type Sender interface {
	Send(ctx context.Context, installationId, processId string, m []Metric) error
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
	serialized, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize account metadata: %v", err)
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

	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad status code received. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

func (s *HTTPSender) Send(ctx context.Context, installationID, processID string, metrics []Metric) error {
	event := &InputEvent{
		InstallationID: installationID,
		ProcessID:      processID,
		Time:           s.timeFunc().Format(time.RFC3339),
		Metrics:        metrics,
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

	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad status code received. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

type dummySender struct{}

func (s *dummySender) Send(ctx context.Context, installationID, processID string, metrics []Metric) error {
	logging.Default().WithFields(logging.Fields{
		"installation_id": installationID,
		"process_id":      processID,
		"metrics":         spew.Sdump(metrics),
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
