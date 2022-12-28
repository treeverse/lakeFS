package email_test

import (
	_ "embed"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/time/rate"
	"gopkg.in/gomail.v2"
)

var (
	//go:embed testdata/invite_user.golden
	inviteUserGolden string

	//go:embed testdata/reset_password.golden
	resetPasswordGolden string
)

type MockDialer struct {
	Message []*gomail.Message
}

func (d *MockDialer) DialAndSend(m ...*gomail.Message) error {
	d.Message = m
	return nil
}

func TestNewEmailer(t *testing.T) {
	tests := []struct {
		name     string
		smtpHost string
		sender   string
		expected error
	}{
		{name: "missing_smtp_host", smtpHost: "", sender: "bar", expected: nil},
		{name: "missing_sender", smtpHost: "foo", sender: "", expected: email.ErrSenderMisconfigured},
		{name: "missing_smtp_host_and_sender", smtpHost: "", sender: "", expected: nil},
		{name: "misconfigured_sender", smtpHost: "test.com", sender: "bar", expected: email.ErrSenderMisconfigured},
		{name: "noraml", smtpHost: "test.com", sender: "alice@testing.com", expected: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := email.Params{
				SMTPHost: tt.smtpHost,
				Sender:   tt.sender,
			}
			emailer, err := email.NewEmailer(p)
			if err == nil && emailer == nil {
				t.Errorf("expected emailer, got nil pointer")
			}
			if !errors.Is(err, tt.expected) {
				t.Errorf("expected err: %s got err: %s", tt.expected, err)
			}
		})
	}
}

func TestSendEmail_Params(t *testing.T) {
	p := email.Params{}
	emailer, err := email.NewEmailer(p)
	emailer.Dialer = &MockDialer{}
	testutil.Must(t, err)
	tests := []struct {
		name     string
		smtpHost string
		sender   string
		receiver []string
		expected error
	}{
		{name: "normal", smtpHost: "test@test.com", sender: "alice@test.com", receiver: []string{"bob@test.com"}, expected: nil},
		{name: "missing_smtp_host", smtpHost: "", sender: "alice@test.com", receiver: []string{"bob@test.com"}, expected: email.ErrNoSMTPHostConfigured},
		{name: "missing_sender", smtpHost: "test@test.com", sender: "", receiver: []string{"bob@test.com"}, expected: email.ErrNoSenderConfigured},
		{name: "missing_recipient", smtpHost: "test@test.com", sender: "alice@test.com", receiver: []string{}, expected: email.ErrNoRecipientConfigured},
		{name: "missing_smtp_host_and_sender", smtpHost: "", sender: "", receiver: []string{"bob@test.com"}, expected: email.ErrNoSMTPHostConfigured},
		{name: "missing_smtp_host_and_recipient", smtpHost: "", sender: "alice@test.com", receiver: []string{}, expected: email.ErrNoSMTPHostConfigured},
		{name: "missing_all", smtpHost: "", sender: "", receiver: []string{}, expected: email.ErrNoSMTPHostConfigured},
		{name: "missing_sender_and_recipient", smtpHost: "test@test.com", sender: "", receiver: []string{}, expected: email.ErrNoSenderConfigured},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emailer.Params.SMTPHost, emailer.Params.Sender = tt.smtpHost, tt.sender
			err = emailer.SendEmail(tt.receiver, "bar", "baz", nil)
			if !errors.Is(err, tt.expected) {
				t.Errorf("expected err: %s got err: %s", tt.expected, err)
			}
		})
	}
}

func TestSendEmail_Headers(t *testing.T) {
	p := email.Params{
		SMTPHost: "abc@test.com",
		Sender:   "test@test.com",
	}
	d := MockDialer{}
	emailer := email.Emailer{
		Params: p,
		Dialer: &d,
	}
	const subject = "testSubject"
	const receiver = "receiver@test.com"
	err := emailer.SendEmail([]string{receiver}, subject, "", nil)
	testutil.Must(t, err)
	if len(d.Message) != 1 {
		t.Fatalf("message length %d, expected 1", len(d.Message))
	}
	headers := map[string]string{
		"From":    p.Sender,
		"To":      receiver,
		"Subject": subject,
	}
	for k, v := range headers {
		header := d.Message[0].GetHeader(k)[0]
		if header != v {
			t.Errorf("got header: %s, expected header: %s", header, v)
		}
	}
}

func TestSendEmailWithLimit(t *testing.T) {
	p := email.Params{SMTPHost: "test.com", Sender: "alice@testing.com"}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	d := &MockDialer{}
	emailer.Dialer = d
	tests := []struct {
		name       string
		limitEvery time.Duration
		burst      int
		expect     error
	}{
		{name: "normal", limitEvery: rate.InfDuration, burst: math.MaxInt, expect: nil},
		{name: "zero_burst", limitEvery: rate.InfDuration, burst: 0, expect: email.ErrRateLimitExceeded},
		{name: "zero_time", limitEvery: 0, burst: math.MaxInt, expect: nil},
		{name: "zero_burst_and_time", limitEvery: 0, burst: 0, expect: email.ErrRateLimitExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emailer.Limiter = rate.NewLimiter(rate.Limit(tt.limitEvery), tt.burst)
			err := emailer.SendEmailWithLimit([]string{"receiver@test.com"}, "foo", "", nil)
			if err == nil && len(d.Message) < 1 {
				t.Errorf("expected message to be sent")
			}
			if !errors.Is(err, tt.expect) {
				t.Errorf("got err: %s, expected err %s", err, tt.expect)
			}
		})
	}
}

func TestSendResetPasswordAndInviteUserEmail(t *testing.T) {
	// Burst value is set to 0, so any call to SendEmailWithLimit should result in ErrRateLimitExceeded
	// thus verifying that SendEmailWithLimit was called
	p := email.Params{Burst: 0, LimitEveryDuration: time.Minute}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	receivers := []string{"foo"}
	params := map[string]string{}
	t.Run("invite_user", func(t *testing.T) {
		err := emailer.SendInviteUserEmail(receivers, params)
		if !errors.Is(err, email.ErrRateLimitExceeded) {
			t.Errorf("expected error: %s got error: %s", email.ErrRateLimitExceeded, err)
		}
	})
	t.Run("reset_password", func(t *testing.T) {
		err := emailer.SendResetPasswordEmail(receivers, params)
		if !errors.Is(err, email.ErrRateLimitExceeded) {
			t.Errorf("expected error: %s got error: %s", email.ErrRateLimitExceeded, err)
		}
	})
}

func TestEmailerSendEmailBody(t *testing.T) {
	p := email.Params{
		SMTPHost:           "abc@test.com",
		Sender:             "test@test.com",
		LimitEveryDuration: time.Minute,
		Burst:              math.MaxInt,
	}
	d := MockDialer{}
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
		{name: "reset_password", op: emailer.SendResetPasswordEmail, expectedBody: resetPasswordGolden},
		{name: "invite_user", op: emailer.SendInviteUserEmail, expectedBody: inviteUserGolden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.op(receivers, m)
			testutil.Must(t, err)
			if len(d.Message) != 1 {
				t.Fatalf("message length %d, expected 1", len(d.Message))
			}
			var builder strings.Builder
			_, err = d.Message[0].WriteTo(&builder)
			testutil.Must(t, err)
			msg := strings.ReplaceAll(builder.String(), "\r\n", "\n")
			if !strings.Contains(msg, tt.expectedBody) {
				t.Errorf("got email '%s', expected to match %s", msg, tt.expectedBody)
			}
		})
	}
}
