package email_test

import (
	"errors"
	"html/template"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/email"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestNewEmailer_DialerAndLimiter(t *testing.T) {
	p := email.Params{}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	if emailer.Dialer == nil {
		t.Errorf("got nil email dialer")
	}
	if emailer.Limiter == nil {
		t.Errorf("got nil email limiter")
	}
}

func TestNewEmailer_Sender(t *testing.T) {
	p := email.Params{
		SMTPHost: "foo",
		Sender:   "bar",
	}
	_, err := email.NewEmailer(p)
	if err == nil {
		t.Errorf("got a proper emailer, expected %s", email.ErrNoSenderConfigured)
	}
}

func TestSendEmail(t *testing.T) {
	p := email.Params{}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	err = emailer.SendEmail([]string{"foo"}, "bar", "baz", nil)
	if !errors.Is(err, email.ErrNoSMTPHostConfigured) {
		t.Errorf("expected err %s", email.ErrNoSMTPHostConfigured)
	}
	emailer.Params.SMTPHost = "foo"
	err = emailer.SendEmail([]string{"foo"}, "bar", "baz", nil)
	if !errors.Is(err, email.ErrNoSenderConfigured) {
		t.Errorf("email sent, expected err %s", email.ErrNoSenderConfigured)
	}
	emailer.Params.Sender = "bar"
	err = emailer.SendEmail([]string{}, "bar", "baz", nil)
	if !errors.Is(err, email.ErrNoRecipientConfigured) {
		t.Errorf("email sent, expected err %s", email.ErrNoRecipientConfigured)
	}
	err = emailer.SendEmail([]string{"foo"}, "bar", "baz", nil)
	if err == nil {
		t.Errorf("email sent, expected error: dial tcp :0: connect: can't assign requested address")
	}
}

func TestSendEmailWithLimit(t *testing.T) {
	p := email.Params{Burst: 0, LimitEveryDuration: time.Minute}
	emailer, err := email.NewEmailer(p)
	testutil.Must(t, err)
	err = emailer.SendEmailWithLimit([]string{"foo"}, "bar", "baz", nil)
	if !errors.Is(err, email.ErrRateLimitExceeded) {
		t.Errorf("expected error %s", email.ErrRateLimitExceeded)
	}
}

func TestBuildEmailByTemplate(t *testing.T) {
	d := map[string]string{
		"paramx": "testparamx",
		"paramy": "testparamy",
	}
	_, err := email.BuildEmailByTemplate(email.ResetEmailTemplate, "foo", "bar", d)
	testutil.Must(t, err)
	_, err = email.BuildEmailByTemplate(email.InviteUserTemplate, "foo", "bar", d)
	testutil.Must(t, err)
	testCases := []string{
		"", "@@@@", "////\\\\\\", "[[[[]]]]]", "12345", "-12345", "-01010101",
		"averyveryextralongstringWITHCAPSandcommas,withsomespecialcharacters*#@!^&()",
	}
	for _, test := range testCases {
		d["paramx"], d["paramy"] = test, test
		template := template.Must(template.New(test).Parse(test))
		_, err = email.BuildEmailByTemplate(template, test, test, d)
		testutil.Must(t, err)
	}
}
