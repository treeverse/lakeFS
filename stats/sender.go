package stats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

var ErrSendError = errors.New("stats: send error")

type Sender interface {
	Send(event InputEvent) error
}

type HTTPSender struct {
	addr string
}

func NewHTTPSender(addr string) *HTTPSender {
	return &HTTPSender{addr}
}

func (s *HTTPSender) Send(event InputEvent) error {
	serialized, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("could not serialize event: %s: %w", err, ErrSendError)
	}

	res, err := http.DefaultClient.Post(s.addr+"/events", "application/json", bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("could not make HTTP request: %s: %w", err, ErrSendError)
	}

	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad status code recieved. status=%d: %w", res.StatusCode, ErrSendError)
	}
	return nil
}

type dummySender struct{}

func (s *dummySender) Send(event InputEvent) error {
	return nil
}

func NewDummySender() Sender {
	return &dummySender{}
}
