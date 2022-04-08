package email

import (
	"golang.org/x/time/rate"
	"gopkg.in/gomail.v2"
)

type Emailer struct {
	Params  EmailParams
	Dialer  *gomail.Dialer
	Limiter *rate.Limiter
}

const (
	defaultRateLimit = 1.0 / 60.0
	defaultBurstRate = 10
)

type EmailParams struct {
	SMTPHost  string
	Port      int
	Username  string
	Password  string
	Sender    string
	RateLimit float64
	Burst     int
}

func NewEmailer(e EmailParams) Emailer {
	d := gomail.NewDialer(e.SMTPHost, e.Port, e.Username, e.Password)
	if e.RateLimit == 0 {
		e.RateLimit = defaultRateLimit
	}
	if e.Burst == 0 {
		e.Burst = defaultBurstRate
	}
	l := rate.NewLimiter(rate.Limit(e.RateLimit), e.Burst)
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
