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

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

const (
	defaultRateLimit = 1.0 / 60.0
	defaultBurstRate = 10
)

type EmailParams struct {
	SMTPHost   string
	Port       int
	Username   string
	Password   string
	Sender     string
	LimitEvery float64
	Burst      int
}

func NewEmailer(e EmailParams) Emailer {
	d := gomail.NewDialer(e.SMTPHost, e.Port, e.Username, e.Password)
	if e.LimitEvery == 0 {
		e.LimitEvery = defaultRateLimit
	}
	if e.Burst == 0 {
		e.Burst = defaultBurstRate
	}
	limit := float64(time.Second) / float64(e.LimitEvery)
	l := rate.NewLimiter(rate.Limit(limit), e.Burst)
	return Emailer{
		Params:  e,
		Dialer:  d,
		Limiter: l,
	}
}

func (e Emailer) SendEmail(receivers []string, subject string, body string, attachmentFilePath []string) error {
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

func (e Emailer) SendEmailWithLimit(receivers []string, subject string, body string, attachmentFilePath []string) error {
	if !e.Limiter.Allow() {
		err := ErrRateLimitExceeded
		return err
	}
	return e.SendEmail(receivers, subject, body, attachmentFilePath)
}
