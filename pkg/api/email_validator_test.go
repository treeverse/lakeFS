package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_validateEmail(t *testing.T) {
	cases := []struct {
		name        string
		email       string
		expectedErr error
	}{
		// invalid
		{name: "empty", email: "", expectedErr: ErrEmailRequired},
		{name: "no at sign", email: "notanemail", expectedErr: ErrInvalidEmailAddress},
		{name: "single label domain", email: "user@cromulent", expectedErr: ErrInvalidEmailAddress},
		{name: "made-up TLD", email: "user@there.is.no.such-tld", expectedErr: ErrInvalidEmailAddress},
		{name: "disposable domain", email: "user@mailinator.com", expectedErr: ErrInvalidEmailAddress},

		// free email providers
		{name: "free email", email: "user@gmail.com", expectedErr: ErrFreeEmailAddress},

		// valid — different TLD families
		{name: "dotcom", email: "user@acme.com", expectedErr: nil},
		{name: "multi-label TLD", email: "user@company.co.uk", expectedErr: nil},
		{name: "dev TLD", email: "user@golang.dev", expectedErr: nil},
		{name: "museum TLD", email: "user@gophers.in.space.museum", expectedErr: nil},
		{name: "private managed domain", email: "user@foo.dyndns.org", expectedErr: nil},

		// edge cases
		{name: "plus addressing", email: "user+tag@acme.co", expectedErr: nil},
		{name: "uppercase domain", email: "USER@ACME.CO", expectedErr: nil},
		{name: "display name rejected", email: "Alice <alice@example.com>", expectedErr: ErrInvalidEmailAddress},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateEmail(c.email)
			if c.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, c.expectedErr)
			}
		})
	}
}
