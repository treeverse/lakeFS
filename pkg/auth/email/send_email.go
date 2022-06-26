package email

import (
	"errors"
	"fmt"
	"net/mail"
	"time"

	"golang.org/x/time/rate"
	"gopkg.in/gomail.v2"
)

var (
	ErrRateLimitExceeded     = errors.New("rate limit exceeded")
	ErrNoSMTPHostConfigured  = errors.New("no smtp host configured")
	ErrNoSenderConfigured    = errors.New("no sender configured")
	ErrNoRecipientConfigured = errors.New("no recipient configured")
	ErrSenderMisconfigured   = errors.New("sender misconfigured")
)

type DialAndSender interface {
	DialAndSend(m ...*gomail.Message) error
}

type Emailer struct {
	Params  Params
	Dialer  DialAndSender
	Limiter *rate.Limiter
}

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

func NewEmailer(p Params) (*Emailer, error) {
	if p.SMTPHost != "" {
		_, err := mail.ParseAddress(p.Sender)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrSenderMisconfigured, err)
		}
	}
	dialer := gomail.NewDialer(p.SMTPHost, p.SMTPPort, p.Username, p.Password)
	dialer.SSL = p.UseSSL
	dialer.LocalName = p.LocalName
	limiter := rate.NewLimiter(rate.Every(p.LimitEveryDuration), p.Burst)
	return &Emailer{
		Params:  p,
		Dialer:  dialer,
		Limiter: limiter,
	}, nil
}

func (e *Emailer) SendEmail(receivers []string, subject string, body string, attachmentFilePath []string) error {
	if e.Params.SMTPHost == "" {
		return ErrNoSMTPHostConfigured
	}
	if e.Params.Sender == "" {
		return ErrNoSenderConfigured
	}
	if len(receivers) == 0 {
		return ErrNoRecipientConfigured
	}
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
	body, err := buildEmailByTemplate(resetEmailTemplate, e.Params.LakefsBaseURL, resetPasswordURLPath, params)
	if err != nil {
		return err
	}
	return e.SendEmailWithLimit(receivers, resetPasswordEmailSubject, body, nil)
}

func (e *Emailer) SendInviteUserEmail(receivers []string, params map[string]string) error {
	body, err := buildEmailByTemplate(inviteUserTemplate, e.Params.LakefsBaseURL, inviteUserURLPath, params)
	if err != nil {
		return err
	}
	return e.SendEmailWithLimit(receivers, inviteUserWEmailSubject, body, nil)
}
