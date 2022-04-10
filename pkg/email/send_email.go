package email

import (
	"errors"
	"time"

	"golang.org/x/time/rate"
	"gopkg.in/gomail.v2"
)

type Emailer struct {
	Params  EmailParams
	Dialer  *gomail.Dialer
	Limiter *rate.Limiter
}

var ErrRateLimitExceeded = errors.New("rate limit exceeded")

type EmailParams struct {
	SMTPHost           string
	Port               int
	Username           string
	Password           string
	Sender             string
	LimitEveryDuration time.Duration
	Burst              int
}

func NewEmailer(e EmailParams) *Emailer {
	dialer := gomail.NewDialer(e.SMTPHost, e.Port, e.Username, e.Password)
	limiter := rate.NewLimiter(rate.Every(e.LimitEveryDuration), e.Burst)
	return &Emailer{
		Params:  e,
		Dialer:  dialer,
		Limiter: limiter,
	}
}

func (e *Emailer) SendEmail(receivers []string, subject string, body string, attachmentFilePath []string) error {
	msg := gomail.NewMessage()
	msg.SetHeader("From", e.Params.Sender)
	msg.SetHeader("To", receivers...)
	msg.SetHeader("Subject", subject)
	msg.SetBody("text/html", body)
	for _, f := range attachmentFilePath {
		msg.Attach(f)
	}
	return e.Dialer.DialAndSend(msg)
}

func (e *Emailer) SendEmailWithLimit(receivers []string, subject string, body string, attachmentFilePath []string) error {
	if !e.Limiter.Allow() {
		return ErrRateLimitExceeded
	}
	return e.SendEmail(receivers, subject, body, attachmentFilePath)
}
