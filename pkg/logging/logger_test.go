package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSetOutputs(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		currentOut := defaultLogger.Out
		err := SetOutputs(nil, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if defaultLogger.Out != currentOut {
			t.Error("Logger output should not change by default")
		}
	})

	t.Run("stdout", func(t *testing.T) {
		err := SetOutputs([]string{"-"}, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if defaultLogger.Out != os.Stdout {
			t.Error("Logger output should be stdout")
		}
	})

	t.Run("stderr", func(t *testing.T) {
		err := SetOutputs([]string{"="}, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if defaultLogger.Out != os.Stderr {
			t.Error("Logger output should be stderr")
		}
	})

	t.Run("write_two_files", func(t *testing.T) {
		logDir := t.TempDir()
		log1 := filepath.Join(logDir, "file1.log")
		log2 := filepath.Join(logDir, "file2.log")
		err := SetOutputs([]string{log1, log2}, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		const content = "hello log"
		_, err = io.WriteString(defaultLogger.Out, content)
		if err != nil {
			t.Fatal("Failed to write to log output with two outputs", err)
		}

		log1Content, err := os.ReadFile(log1)
		if err != nil {
			t.Fatal("Failed to read log1 content", err)
		}
		if string(log1Content) != content {
			t.Fatalf("Log1 content '%s', is not as expected: '%s'", string(log1Content), content)
		}

		log2Content, err := os.ReadFile(log2)
		if err != nil {
			t.Fatal("Failed to read log1 content", err)
		}
		if string(log2Content) != content {
			t.Fatalf("Log1 content '%s', is not as expected: '%s'", string(log2Content), content)
		}
	})
}

func TestDurationFormatting(t *testing.T) {
	type TestCase struct {
		// OutputFormat is passed to SetOutputFormat.
		OutputFormat string
		// FormatDurations returns a list of substrings that will
		// appear in the output file when logging duration.
		FormatDurations func(label string, duration time.Duration) []string
		// FormatInt returns a substring that will appear in the
		// output file when logging number.
		FormatInt func(label string, value int) string
	}
	cases := []TestCase{{
		OutputFormat: "text",
		FormatDurations: func(label string, duration time.Duration) []string {
			return []string{
				fmt.Sprint(label, "_str", "=", duration.String()),
				fmt.Sprint(label, "=", int64(duration)),
			}
		},
		FormatInt: func(label string, value int) string {
			return fmt.Sprint(label, "=", value)
		},
	}, {
		OutputFormat: "json",
		FormatDurations: func(label string, duration time.Duration) []string {
			// Useful only inside this test.  Does not protect
			// special chars in label.
			return []string{
				fmt.Sprintf("\"%s_str\":\"%s\"", label, duration.String()),
				fmt.Sprintf("\"%s\":%d", label, duration),
			}
		},
		FormatInt: func(label string, value int) string {
			// Useful only inside this test.  Does not protect
			// special characters in label.
			return fmt.Sprintf("\"%s\":%d", label, value)
		},
	}}

	for _, tc := range cases {
		duration := 17*time.Hour + 19*time.Minute
		t.Run(tc.OutputFormat, func(t *testing.T) {
			logDir := t.TempDir()
			log := filepath.Join(logDir, "file.log")
			err := SetOutputs([]string{log}, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			SetOutputFormat(tc.OutputFormat)

			// Tests both WithField and WithFields
			ContextUnavailable().
				WithField("xyzzy", duration).
				WithFields(Fields{
					"bar":    duration,
					"scalar": 17,
					"baz":    duration,
				}).
				Info("log")
			err = CloseWriters()
			if err != nil {
				t.Fatalf("Close writers: %s", err)
			}

			r, err := os.Open(log)
			if err != nil {
				t.Fatalf("Open %s to read contents: %s", log, err)
			}
			contents, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("Read %s contents: %s", log, err)
			}

			for _, base := range []string{"xyzzy", "bar", "baz"} {
				for _, expected := range tc.FormatDurations(base, duration) {
					if !bytes.Contains(contents, []byte(expected)) {
						t.Errorf("Log contents do not contain expected substring %s:\n%s", expected, string(contents))
					}
				}
			}
			expected := tc.FormatInt("scalar", 17)
			if !bytes.Contains(contents, []byte(expected)) {
				t.Errorf("Log contents do not contain expected substring %s:\n%s", expected, string(contents))
			}
		})
	}
}

func TestLogCallerTrimmer(t *testing.T) {
	tests := []struct {
		name             string
		file             string
		function         string
		expectedFile     string
		expectedFunction string
	}{
		{
			name:             "standard lakefs directory",
			file:             "/home/user/work/lakefs/pkg/logging/logger.go",
			function:         "github.com/treeverse/lakefs/pkg/logging.TestFunc",
			expectedFile:     "pkg/logging/logger.go",
			expectedFunction: "pkg/logging.TestFunc",
		},
		{
			name:             "lakefs-demo directory",
			file:             "/home/user/work/lakefs-demo/pkg/logging/logger.go",
			function:         "github.com/treeverse/lakefs-demo/pkg/logging.TestFunc",
			expectedFile:     "pkg/logging/logger.go",
			expectedFunction: "pkg/logging.TestFunc",
		},
		{
			name:             "lakefs_foo directory with underscore",
			file:             "/home/user/work/lakefs_foo/pkg/api/handler.go",
			function:         "github.com/treeverse/lakefs_foo/pkg/api.Handler",
			expectedFile:     "pkg/api/handler.go",
			expectedFunction: "pkg/api.Handler",
		},
		{
			name:             "uppercase LakeFS in path",
			file:             "/home/user/work/LakeFS/pkg/block/adapter.go",
			function:         "github.com/treeverse/lakefs/pkg/block.Adapter",
			expectedFile:     "pkg/block/adapter.go",
			expectedFunction: "pkg/block.Adapter",
		},
		{
			name:             "no lakefs in path",
			file:             "/home/user/other/project/main.go",
			function:         "github.com/other/project.Main",
			expectedFile:     "home/user/other/project/main.go", // leading separator gets trimmed
			expectedFunction: "github.com/other/project.Main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := &runtime.Frame{
				File:     tt.file,
				Line:     42,
				Function: tt.function,
			}

			gotFunction, gotFile := logCallerTrimmer(frame)

			expectedFileWithLine := fmt.Sprintf("%s:42", tt.expectedFile)
			if gotFile != expectedFileWithLine {
				t.Errorf("file = %q, want %q", gotFile, expectedFileWithLine)
			}
			if gotFunction != tt.expectedFunction {
				t.Errorf("function = %q, want %q", gotFunction, tt.expectedFunction)
			}
		})
	}
}

// requireLoggerFields verifies a logger outputs the expected fields.  It sets up JSON output to
// a temp file, logs a message using the provided logger, parses the JSON output, and verifies
// all additionalFields are present with correct values.
func requireLoggerFields(t *testing.T, logger Logger, additionalFields Fields) {
	t.Helper()

	logDir := t.TempDir()
	logFile := filepath.Join(logDir, "test.log")
	err := SetOutputs([]string{logFile}, 0, 0)
	if err != nil {
		t.Fatalf("SetOutputs: %s", err)
	}
	SetOutputFormat("json")

	logger.Info("test message")

	err = CloseWriters()
	if err != nil {
		t.Fatalf("CloseWriters: %s", err)
	}

	contents, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("ReadFile %s: %s", logFile, err)
	}

	var logEntry map[string]any
	if err := json.Unmarshal(contents, &logEntry); err != nil {
		t.Fatalf("Unmarshal log entry: %s\nContents: %s", err, string(contents))
	}

	for key, expectedValue := range additionalFields {
		actualValue, exists := logEntry[key]
		if !exists {
			t.Errorf("Expected field %q not found in log entry. Log: %s", key, string(contents))
			continue
		}
		if fmt.Sprintf("%v", actualValue) != fmt.Sprintf("%v", expectedValue) {
			t.Errorf("Field %q: got %v, want %v", key, actualValue, expectedValue)
		}
	}
}

func TestContextLogging(t *testing.T) {
	type testCase struct {
		name string
		// contextFields are the fields to put on context.
		contextFields Fields
		// logFields are the fields to put directly on the logger.
		logFields Fields
		// expectedFields overrides the default expected fields when set.
		// Use this when contextFields and logFields have overlapping keys.
		expectedFields Fields
	}

	cases := []testCase{
		{
			name:          "no fields",
			contextFields: nil,
			logFields:     nil,
		},
		{
			name: "context field only",
			contextFields: Fields{
				RequestIDFieldKey: "req-123",
			},
			logFields: nil,
		},
		{
			name: "multiple context fields",
			contextFields: Fields{
				RequestIDFieldKey:  "req-456",
				RepositoryFieldKey: "my-repo",
				UserFieldKey:       "alice",
			},
			logFields: nil,
		},
		{
			name:          "logger field only",
			contextFields: nil,
			logFields: Fields{
				"custom_key": "custom_value",
			},
		},
		{
			name: "both context and logger fields",
			contextFields: Fields{
				UserFieldKey: "bob",
			},
			logFields: Fields{
				"existing_key": "existing_value",
			},
		},
		{
			name: "overlapping field",
			contextFields: Fields{
				UserFieldKey: "from_context",
			},
			logFields: Fields{
				UserFieldKey: "from_logger",
			},
		},
	}

	// expectedFieldsLogFirst returns combined fields with logFields first, then contextFields.
	// Context fields override logger fields for same key.
	expectedFieldsLogFirst := func(tc testCase) Fields {
		if tc.expectedFields != nil {
			return tc.expectedFields
		}
		result := Fields{}
		maps.Copy(result, tc.logFields)
		maps.Copy(result, tc.contextFields)
		return result
	}

	// expectedFieldsContextFirst returns combined fields with contextFields first, then logFields.
	// Logger fields override context fields for same key.
	expectedFieldsContextFirst := func(tc testCase) Fields {
		if tc.expectedFields != nil {
			return tc.expectedFields
		}
		result := Fields{}
		maps.Copy(result, tc.contextFields)
		maps.Copy(result, tc.logFields)
		return result
	}

	t.Run("FromContext", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.contextFields != nil {
					ctx = AddFields(ctx, tc.contextFields)
				}
				logger := FromContext(ctx)
				if tc.logFields != nil {
					logger = logger.WithFields(tc.logFields)
				}
				// FromContext creates logger from context, then WithFields adds logFields.
				// So logFields are applied after contextFields: logFields win on conflict.
				requireLoggerFields(t, logger, expectedFieldsContextFirst(tc))
			})
		}
	})

	t.Run("WithContext", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.contextFields != nil {
					ctx = AddFields(ctx, tc.contextFields)
				}
				logger := ContextUnavailable()
				if tc.logFields != nil {
					logger = logger.WithFields(tc.logFields)
				}
				logger = logger.WithContext(ctx)
				// WithContext applies contextFields on top of existing logger fields.
				// So contextFields are applied after logFields: contextFields win on conflict.
				requireLoggerFields(t, logger, expectedFieldsLogFirst(tc))
			})
		}
	})
}

func TestGetFieldsFromContext(t *testing.T) {
	t.Run("context with no fields", func(t *testing.T) {
		ctx := context.Background()
		fields := GetFieldsFromContext(ctx)
		if fields != nil {
			t.Errorf("Expected nil fields, got %v", fields)
		}
	})

	t.Run("context with fields", func(t *testing.T) {
		ctx := AddFields(context.Background(), Fields{
			RequestIDFieldKey: "req-789",
			UserFieldKey:      "dave",
		})
		fields := GetFieldsFromContext(ctx)
		if fields == nil {
			t.Fatal("Expected fields, got nil")
		}
		if fields[RequestIDFieldKey] != "req-789" {
			t.Errorf("RequestIDFieldKey: got %v, want %v", fields[RequestIDFieldKey], "req-789")
		}
		if fields[UserFieldKey] != "dave" {
			t.Errorf("UserFieldKey: got %v, want %v", fields[UserFieldKey], "dave")
		}
	})
}
