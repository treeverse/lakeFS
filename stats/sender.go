package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var ErrSendError = errors.New("stats: send error")

type Sender interface {
	Send(ctx context.Context, m []Metric) error
}

type TimeFn func() time.Time

type HTTPSender struct {
	timeFunc  TimeFn
	userID    string
	processID string
	addr      string
}

func NewHTTPSender(userID, processID, addr string, timeFunc TimeFn) *HTTPSender {
	return &HTTPSender{
		timeFunc:  timeFunc,
		userID:    userID,
		processID: processID,
		addr:      addr,
	}
}

func (s *HTTPSender) Send(ctx context.Context, metrics []Metric) error {
	event := &InputEvent{
		Email:     s.userID,
		ProcessId: s.processID,
		Time:      s.timeFunc().Format(time.RFC3339),
		Metrics:   metrics,
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
		return fmt.Errorf("bad status code recieved. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

type dummySender struct{}

func (s *dummySender) Send(ctx context.Context, metrics []Metric) error {
	return nil
}

func NewDummySender() Sender {
	return &dummySender{}
}
