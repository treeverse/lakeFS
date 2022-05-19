package email_test

import (
	_ "embed"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/email"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/time/rate"
)

var (
	//go:embed invite_user.golden
	inviteUserGolden string

	//go:embed reset_password.golden
	resetPasswordGolden string
)

func TestNewEmailer_Sender(t *testing.T) {
	p := email.Params{
		SMTPHost: "foo",
		Sender:   "bar",
	}
	_, err := email.NewEmailer(p)
	if err == nil {
		t.Errorf("expected err:, got:%s", err)
	}
}

func TestSendEmail_Params(t *testing.T) { // table testing here
	p := email.Params{}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	tests := []struct {
		name     string
		smtpHost string
		sender   string
		receiver []string
		expected error
	}{
		{name: "test smtp_host", smtpHost: "", sender: "bar", receiver: []string{"baz"}, expected: email.ErrNoSMTPHostConfigured},
		{name: "test sender", smtpHost: "foo", sender: "", receiver: []string{"baz"}, expected: email.ErrNoSenderConfigured},
		{name: "test recipient", smtpHost: "foo", sender: "bar", receiver: []string{}, expected: email.ErrNoRecipientConfigured},
	}
	for _, tt := range tests {
		emailer.Params.SMTPHost, emailer.Params.Sender = tt.smtpHost, tt.sender
		err = emailer.SendEmail(tt.receiver, "bar", "baz", nil)
		if !errors.Is(err, tt.expected) {
			t.Errorf("expected err: %s got err: %s", tt.expected, err)
		}
	}
}

func TestSendEmail_Headers(t *testing.T) {
	p := email.Params{
		SMTPHost: "abc@test.com",
		Sender:   "test@test.com",
	}
	d := email.Dialer{}
	emailer := email.Emailer{
		Params: p,
		Dialer: &d,
	}
	subject := "testSubject"
	receiver := "receiver@test.com"
	emailer.SendEmail([]string{receiver}, subject, "", nil)
	if len(d.Message) == 0 {
		t.Fatalf("message length %d, expected 1", len(d.Message))
	}
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{name: "test from header", header: "From", expected: p.Sender},
		{name: "test To header", header: "To", expected: receiver},
		{name: "test Subject header", header: "Subject", expected: subject},
	}
	for _, tt := range tests {
		header := d.Message[0].GetHeader(tt.header)[0]
		if header != tt.expected {
			t.Errorf("got header: %s, expected header: %s", header, tt.expected)
		}
	}
}

func TestSendResetPasswordAndInviteUserEmail(t *testing.T) {
	// Burst value is set to 0, so any call to SendEmailWithLimit should result in ErrRateLimitExceeded
	p := email.Params{Burst: 0, LimitEveryDuration: time.Minute}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	err = emailer.SendInviteUserEmail([]string{"foo"}, map[string]string{})
	if !errors.Is(err, email.ErrRateLimitExceeded) {
		t.Errorf("expected error: %s got error: %s", email.ErrRateLimitExceeded, err)
	}
	err = emailer.SendResetPasswordEmail([]string{"foo"}, map[string]string{})
	if !errors.Is(err, email.ErrRateLimitExceeded) {
		t.Errorf("expected error %s", email.ErrRateLimitExceeded)
	}
}

func TestTemplates(t *testing.T) {
	p := email.Params{
		SMTPHost:           "abc@test.com",
		Sender:             "test@test.com",
		LimitEveryDuration: time.Minute,
		Burst:              10,
	}
	d := email.Dialer{}
	lim := rate.NewLimiter(rate.Every(p.LimitEveryDuration), p.Burst)
	emailer := email.Emailer{
		Params:  p,
		Dialer:  &d,
		Limiter: lim,
	}
	m := map[string]string{
		"paramx": "foo",
		"paramy": "bar",
	}
	receivers := []string{"receiver@test.com"}

	tests := []struct {
		name         string
		op           func(receivers []string, params map[string]string) error
		expectedBody string
	}{
		{name: "reset password template", op: emailer.SendResetPasswordEmail, expectedBody: resetPasswordGolden},
		{name: "invite user template", op: emailer.SendInviteUserEmail, expectedBody: inviteUserGolden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.op(receivers, m)
			testutil.Must(t, err)
			var builder strings.Builder
			if len(d.Message) != 1 {
				t.Fatalf("message length %d, expected 1", len(d.Message))
			}
			_, err = d.Message[0].WriteTo(&builder)
			testutil.Must(t, err)
			msg := strings.NewReplacer("\r\n", "\n").Replace(builder.String())
			if !strings.Contains(msg, tt.expectedBody) {
				t.Errorf("got email '%s', expected template to match %s", msg, tt.expectedBody)
			}
		})
	}
}
