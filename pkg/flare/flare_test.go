package flare

import (
	"bufio"
	"bytes"
	"crypto/sha512"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvVarHandler(t *testing.T) {
	testCases := []struct {
		Name            string
		PrefixOverrides []string
		PrefixAdditions []string
		EnvVars         []string
		Expected        string
	}{
		{
			Name:     "no env vars",
			EnvVars:  []string{},
			Expected: "",
		},
		{
			Name: "single env var with prefix",
			EnvVars: []string{
				"LAKEFS_TEST_ENV_VAR=test",
			},
			Expected: `LAKEFS_TEST_ENV_VAR=test
`,
		},
		{
			Name: "multiple env vars with prefix",
			EnvVars: []string{
				"LAKEFS_TEST_ENV_VAR=test",
				"LAKEFS_OTHER_TEST_ENV_VAR=test2",
			},
			Expected: `LAKEFS_TEST_ENV_VAR=test
LAKEFS_OTHER_TEST_ENV_VAR=test2
`,
		},
		{
			Name: "postgres connection string",
			EnvVars: []string{
				"LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=postgresql://lakefs:lakefs@localhost:5432/postgres?sslmode=disable",
			},
			Expected: `LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=<REDACTED>
`,
		},
		{
			Name: "env var with secret",
			EnvVars: []string{
				"LAKEFS_SOME_API_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
			},
			Expected: `LAKEFS_SOME_API_KEY=<REDACTED>
`,
		},
		{
			Name: "multiple env vars with secrets",
			EnvVars: []string{
				"LAKEFS_DB_CONNECTION_STRING=postgresql://lakefs:password@localhost:5432/lakefe_db",
				"LAKEFS_AWS_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"LAKEFS_AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
			},
			Expected: `LAKEFS_DB_CONNECTION_STRING=<REDACTED>
LAKEFS_AWS_SECRET_KEY=<REDACTED>
LAKEFS_AWS_ACCESS_KEY_ID=<REDACTED>
`,
		},
		{
			Name: "low-entropy value",
			EnvVars: []string{
				"LAKEFS_AUTH_ENCRYPT_SECRET_KEY=12e3wadasd",
			},
			Expected: `LAKEFS_AUTH_ENCRYPT_SECRET_KEY=<REDACTED>
`,
		},
		{
			Name: "high-entropy value",
			EnvVars: []string{
				"LAKEFS_AUTH_ENCRYPT_SECRET_KEY=h8vkOauR6Ptt2cvM8WEVsaexZ1IsX55s",
			},
			Expected: `LAKEFS_AUTH_ENCRYPT_SECRET_KEY=<REDACTED>
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			flr, err := NewFlare(
				WithSecretReplacerFunc(func(value string) string {
					return "<REDACTED>"
				}),
				WithEnv(tc.EnvVars),
			)
			assert.NoError(t, err)

			b := new(bytes.Buffer)
			bw := bufio.NewWriter(b)
			flr.processEnvVars(bw)
			bw.Flush()
			assert.Equal(t, tc.Expected, b.String())
		})
	}
}

func TestEnvVarBlacklist(t *testing.T) {
	testCases := []struct {
		Name      string
		Blacklist []string
		EnvVars   []string
		Expected  string
	}{
		{
			Name:      "empty blacklist",
			Blacklist: []string{},
			EnvVars: []string{
				"LAKEFS_TEST_ENV_VAR=test",
			},
			Expected: `LAKEFS_TEST_ENV_VAR=test
`,
		},
		{
			Name:      "single blacklisted",
			Blacklist: []string{"LAKEFS_TEST"},
			EnvVars: []string{
				"LAKEFS_TEST=test",
			},
			Expected: `LAKEFS_TEST=<REDACTED>
`,
		},
		{
			Name:      "Blacklisted and non-blacklisted",
			Blacklist: []string{"LAKEFS_TEST"},
			EnvVars: []string{
				"LAKEFS_TEST=test",
				"LAKEFS_TEST_OTHER=test2",
			},
			Expected: `LAKEFS_TEST=<REDACTED>
LAKEFS_TEST_OTHER=test2
`,
		},
	}

	replacerFunc := func(value string) string {
		return "<REDACTED>"
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			flr, err := NewFlare(
				WithEnvVarBlacklist(tc.Blacklist),
				WithSecretReplacerFunc(replacerFunc),
				WithEnv(tc.EnvVars),
			)
			assert.NoError(t, err)
			b := new(bytes.Buffer)
			bw := bufio.NewWriter(b)
			flr.processEnvVars(bw)
			bw.Flush()
			assert.Equal(t, tc.Expected, b.String())
		})
	}
}

func TestDefaultReplacerFunc(t *testing.T) {
	testCases := []struct {
		Name    string
		EnvVars []string
	}{
		{
			Name: "single env var",
			EnvVars: []string{
				"LAKEFS_AUTH_ENCRYPT_SECRET_KEY=12e3wadasd",
			},
		},
		{
			Name: "multiple env vars",
			EnvVars: []string{
				"LAKEFS_DB_CONNECTION_STRING=postgresql://lakefs:password@localhost:5432/lakefe_db",
				"LAKEFS_AWS_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"LAKEFS_AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			flr, err := NewFlare(WithEnv(tc.EnvVars))
			assert.NoError(t, err)

			expected := ""
			b := new(bytes.Buffer)
			bw := bufio.NewWriter(b)
			for _, kv := range tc.EnvVars {
				hasher := sha512.New()
				v := strings.SplitN(kv, "=", 2)
				hasher.Write([]byte(v[1]))
				expected = expected +
					fmt.Sprintf("%s=%x\n", v[0], hasher.Sum(nil))
			}
			flr.processEnvVars(bw)
			bw.Flush()
			assert.Equal(t, expected, b.String())
		})
	}
}
