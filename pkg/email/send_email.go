package email

import (
	"gopkg.in/gomail.v2"
)

type Email struct {
	SMTPHost string
	Port     int
	Username string
	Password string
	Sender   string
}

func (e Email) SendEmail(receivers []string, subject string, body string, attachmentFilePath ...string) error {
	msg := gomail.NewMessage()
	msg.SetHeader("From", e.Sender)
	msg.SetHeader("To", receivers...)
	msg.SetHeader("Subject", subject)
	msg.SetBody("text/html", body)
	for _, f := range attachmentFilePath {
		msg.Attach(f)
	}
	d := gomail.NewDialer(e.SMTPHost, e.Port, e.Username, e.Password)
	return d.DialAndSend(msg)
}
