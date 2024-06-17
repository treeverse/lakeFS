package flare

import (
	"bufio"
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var logTimeOffset = time.FixedZone("GMT+3", int((3 * time.Hour).Seconds()))

type LogFileHandlerTestCase struct {
	Name         string
	InFile       string
	ExpectedFile string
	StartDate    time.Time
	EndDate      time.Time
}

func TestPlainTextLogFileHandler(t *testing.T) {
	testCases := []LogFileHandlerTestCase{
		{
			Name:         "no filter",
			InFile:       "testdata/plain_text_logs_in.txt",
			ExpectedFile: "testdata/plain_text_logs_out_no_filter.txt",
			StartDate:    time.Time{},
			EndDate:      time.Time{},
		},
		{
			Name:         "with start date",
			InFile:       "testdata/plain_text_logs_in.txt",
			ExpectedFile: "testdata/plain_text_logs_out_start_date.txt",
			StartDate:    time.Date(2024, 06, 06, 00, 00, 00, 0, logTimeOffset),
			EndDate:      time.Time{},
		},
		{
			Name:         "with end date",
			InFile:       "testdata/plain_text_logs_in.txt",
			ExpectedFile: "testdata/plain_text_logs_out_end_date.txt",
			StartDate:    time.Time{},
			EndDate:      time.Date(2024, 06, 05, 23, 52, 00, 0, logTimeOffset),
		},
		{
			Name:         "with start and end dates",
			InFile:       "testdata/plain_text_logs_in.txt",
			ExpectedFile: "testdata/plain_text_logs_out_start_and_end_date.txt",
			StartDate:    time.Date(2024, 06, 06, 12, 48, 52, 0, logTimeOffset),
			EndDate:      time.Date(2024, 06, 06, 12, 48, 54, 0, logTimeOffset),
		},
	}

	testLogFileHandler(t, LogFormatPlainText, testCases)
}

func TestJSONLogFileHandler(t *testing.T) {
	testCases := []LogFileHandlerTestCase{
		{
			Name:         "no filter",
			InFile:       "testdata/json_logs_in.json",
			ExpectedFile: "testdata/json_logs_out_no_filter.json",
			StartDate:    time.Time{},
			EndDate:      time.Time{},
		},
		{
			Name:         "with start date",
			InFile:       "testdata/json_logs_in.json",
			ExpectedFile: "testdata/json_logs_out_start_date.json",
			StartDate:    time.Date(2024, 06, 06, 00, 00, 00, 0, logTimeOffset),
			EndDate:      time.Time{},
		},
		{
			Name:         "with end date",
			InFile:       "testdata/json_logs_in.json",
			ExpectedFile: "testdata/json_logs_out_end_date.json",
			StartDate:    time.Time{},
			EndDate:      time.Date(2024, 06, 06, 00, 00, 00, 0, logTimeOffset),
		},
		{
			Name:         "with start and end dates",
			InFile:       "testdata/json_logs_in.json",
			ExpectedFile: "testdata/json_logs_out_start_and_end_date.json",
			StartDate:    time.Date(2024, 06, 06, 18, 29, 50, 0, logTimeOffset),
			EndDate:      time.Date(2024, 06, 06, 18, 31, 45, 0, logTimeOffset),
		},
	}

	testLogFileHandler(t, LogFormatJSON, testCases)
}

func testLogFileHandler(t *testing.T, logFormat LogFormat, testCases []LogFileHandlerTestCase) {
	t.Helper()
	flare, err := NewFlare(logFormat)
	assert.NoError(t, err)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			expected, err := os.ReadFile(tc.ExpectedFile)
			assert.NoError(t, err)
			expectedString := string(expected[:])
			var start, end *time.Time
			if !tc.StartDate.IsZero() {
				start = &tc.StartDate
			}
			if !tc.EndDate.IsZero() {
				end = &tc.EndDate
			}
			in, err := os.Open(tc.InFile)
			assert.NoError(t, err)
			defer in.Close()

			sb := strings.Builder{}
			scanner := bufio.NewScanner(in)
			first := true

			for scanner.Scan() {
				line := scanner.Text()
				pLine, err := flare.handleLogLine(line, start, end)
				assert.NoError(t, err)

				if pLine == "" {
					continue
				}

				if !first {
					sb.WriteString("\n")
				}
				sb.WriteString(pLine)
				first = false
			}

			assert.Equal(t, expectedString, sb.String())
		})
	}
}

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

	flr, err := NewFlare(LogFormatPlainText)
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
