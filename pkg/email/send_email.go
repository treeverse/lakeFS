package email

import (
	"errors"
	"time"

	"golang.org/x/time/rate"
	"gopkg.in/gomail.v2"
)

type Emailer struct {
	Params  Params
	Dialer  *gomail.Dialer
	Limiter *rate.Limiter
}

var ErrRateLimitExceeded = errors.New("rate limit exceeded")

type Params struct {
	SMTPHost           string
	SMTPPort           int
	UseSSL             bool
	Username           string
	Password           string
	LocalName          string
	Sender             string
	LimitEveryDuration time.Duration
	Burst              int
	LakefsBaseURL      string
}

func NewEmailer(p Params) *Emailer {
	dialer := gomail.NewDialer(p.SMTPHost, p.SMTPPort, p.Username, p.Password)
	dialer.SSL = p.UseSSL
	dialer.LocalName = p.LocalName
	limiter := rate.NewLimiter(rate.Every(p.LimitEveryDuration), p.Burst)
	return &Emailer{
		Params:  p,
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

func (e *Emailer) SendResetPasswordEmail(receivers []string, params map[string]string) error {
	body, err := buildEmailByTemplate(resetEmailTemplate, e.Params.LakefsBaseURL, ResetPasswordURLPath, params)
	if err != nil {
		return err
	}
	return e.SendEmailWithLimit(receivers, resetPasswordEmailSubject, body, nil)
}

func (e *Emailer) SendInviteUserEmail(receivers []string, params map[string]string) error {
	body, err := buildEmailByTemplate(inviteUserTemplate, e.Params.LakefsBaseURL, InviteUserURLPath, params)
	if err != nil {
		return err
	}
	return e.SendEmailWithLimit(receivers, inviteUserWEmailSubject, body, nil)
}
