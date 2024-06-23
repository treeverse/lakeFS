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

type EnvVarKV struct {
	Key   string
	Value string
}

func TestEnvVarHandler(t *testing.T) {
	testCases := []struct {
		Name            string
		PrefixOverrides []string
		PrefixAdditions []string
		EnvVars         []EnvVarKV
		Expected        string
	}{
		{
			Name:     "no env vars",
			EnvVars:  []EnvVarKV{},
			Expected: "",
		},
		{
			Name: "single env var with prefix",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_TEST_ENV_VAR",
					Value: "test",
				},
			},
			Expected: `LAKEFS_TEST_ENV_VAR=test
`,
		},
		{
			Name: "multiple env vars with prefix",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_TEST_ENV_VAR",
					Value: "test",
				},
				{
					Key:   "LAKEFS_OTHER_TEST_ENV_VAR",
					Value: "test2",
				},
			},
			Expected: `LAKEFS_TEST_ENV_VAR=test
LAKEFS_OTHER_TEST_ENV_VAR=test2
`,
		},
		{
			Name: "postgres connection string",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING",
					Value: "postgresql://lakefs:lakefs@localhost:5432/postgres?sslmode=disable",
				},
			},
			Expected: `LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=<REDACTED>
`,
		},
		{
			Name: "env var with secret",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_SOME_API_KEY",
					Value: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
				},
			},
			Expected: `LAKEFS_SOME_API_KEY=<REDACTED>
`,
		},
		{
			Name: "multiple env vars with secrets",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_DB_CONNECTION_STRING",
					Value: "postgresql://lakefs:password@localhost:5432/lakefe_db",
				},
				{
					Key:   "LAKEFS_AWS_SECRET_KEY",
					Value: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
				{
					Key:   "LAKEFS_AWS_ACCESS_KEY_ID",
					Value: "AKIAIOSFODNN7EXAMPLE",
				},
			},
			Expected: `LAKEFS_DB_CONNECTION_STRING=<REDACTED>
LAKEFS_AWS_SECRET_KEY=<REDACTED>
LAKEFS_AWS_ACCESS_KEY_ID=<REDACTED>
`,
		},
		{
			Name: "low-entropy value",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_AUTH_ENCRYPT_SECRET_KEY",
					Value: "12e3wadasd",
				},
			},
			Expected: `LAKEFS_AUTH_ENCRYPT_SECRET_KEY=<REDACTED>
`,
		},
		{
			Name: "high-entropy value",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_AUTH_ENCRYPT_SECRET_KEY",
					Value: "h8vkOauR6Ptt2cvM8WEVsaexZ1IsX55s",
				},
			},
			Expected: `LAKEFS_AUTH_ENCRYPT_SECRET_KEY=<REDACTED>
`,
		},
	}

	flr, err := NewFlare(WithSecretReplacerFunc(func(value string) string {
		return "<REDACTED>"
	}))
	assert.NoError(t, err)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			b := new(bytes.Buffer)
			bw := bufio.NewWriter(b)
			for _, kv := range tc.EnvVars {
				t.Setenv(kv.Key, kv.Value)
			}
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
		EnvVars   []EnvVarKV
		Expected  string
	}{
		{
			Name:      "empty blacklist",
			Blacklist: []string{},
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_TEST_ENV_VAR",
					Value: "test",
				},
			},
			Expected: `LAKEFS_TEST_ENV_VAR=test
`,
		},
		{
			Name:      "single blacklisted",
			Blacklist: []string{"LAKEFS_TEST"},
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_TEST",
					Value: "test",
				},
			},
			Expected: `LAKEFS_TEST=<REDACTED>
`,
		},
		{
			Name:      "Blacklisted and non-blacklisted",
			Blacklist: []string{"LAKEFS_TEST"},
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_TEST",
					Value: "test",
				},
				{
					Key:   "LAKEFS_TEST_OTHER",
					Value: "test2",
				},
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
			flr, err := NewFlare(WithEnvVarBlacklist(tc.Blacklist), WithSecretReplacerFunc(replacerFunc))
			assert.NoError(t, err)
			b := new(bytes.Buffer)
			bw := bufio.NewWriter(b)
			for _, kv := range tc.EnvVars {
				t.Setenv(kv.Key, kv.Value)
			}
			flr.processEnvVars(bw)
			bw.Flush()
			assert.Equal(t, tc.Expected, b.String())
		})
	}
}

func TestDefaultReplacerFunc(t *testing.T) {
	testCases := []struct {
		Name    string
		EnvVars []EnvVarKV
	}{
		{
			Name: "single env var",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_AUTH_ENCRYPT_SECRET_KEY",
					Value: "12e3wadasd",
				},
			},
		},
		{
			Name: "multiple env vars",
			EnvVars: []EnvVarKV{
				{
					Key:   "LAKEFS_DB_CONNECTION_STRING",
					Value: "postgresql://lakefs:password@localhost:5432/lakefe_db",
				},
				{
					Key:   "LAKEFS_AWS_SECRET_KEY",
					Value: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
				{
					Key:   "LAKEFS_AWS_ACCESS_KEY_ID",
					Value: "AKIAIOSFODNN7EXAMPLE",
				},
			},
		},
	}

	flr, err := NewFlare()
	assert.NoError(t, err)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			expected := make([]EnvVarKV, 0, len(tc.EnvVars))
			b := new(bytes.Buffer)
			bw := bufio.NewWriter(b)
			for _, kv := range tc.EnvVars {
				t.Setenv(kv.Key, kv.Value)
				hasher := sha512.New()
				hasher.Write([]byte(kv.Value))
				expected = append(expected, EnvVarKV{
					Key:   kv.Key,
					Value: fmt.Sprintf("%x", hasher.Sum(nil)),
				})
			}
			flr.processEnvVars(bw)
			bw.Flush()
			expectedString := fmt.Sprintf("%s%s", strings.Join(joinKeyValue(expected), "\n"), "\n")
			assert.Equal(t, expectedString, b.String())
		})
	}
}

func joinKeyValue(kv []EnvVarKV) []string {
	res := make([]string, 0, len(kv))
	for _, itm := range kv {
		res = append(res, fmt.Sprintf("%s=%s", itm.Key, itm.Value))
	}
	return res
}
