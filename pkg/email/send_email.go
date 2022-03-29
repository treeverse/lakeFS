package email

import (
	"gopkg.in/gomail.v2"
)

type Emailer struct {
	SMTPHost string
	Port     int
	Username string
	Password string
	Sender   string
	Dialer   *gomail.Dialer
}

type EmailParams struct {
	SMTPHost string
	Port     int
	Username string
	Password string
	Sender   string
}

func NewEmailer(e EmailParams) Emailer {
	d := gomail.NewDialer(e.SMTPHost, e.Port, e.Username, e.Password)
	return Emailer{
		SMTPHost: e.SMTPHost,
		Port:     e.Port,
		Username: e.Username,
		Password: e.Password,
		Sender:   e.Sender,
		Dialer:   d,
	}
}

func (e Emailer) SendEmail(receivers []string, subject string, body string, attachmentFilePath []string) error {
	msg := gomail.NewMessage()
	msg.SetHeader("From", e.Sender)
	msg.SetHeader("To", receivers...)
	msg.SetHeader("Subject", subject)
	msg.SetBody("text/html", body)
	for _, f := range attachmentFilePath {
		msg.Attach(f)
	}
	// d := gomail.NewDialer(e.SMTPHost, e.Port, e.Username, e.Password)
	return e.Dialer.DialAndSend(msg)
}
