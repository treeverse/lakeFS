package logging

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
			return []string{fmt.Sprint(label, "_str", "=", duration.String()),
				fmt.Sprint(label, "_nsecs", "=", int64(duration)),
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
			return []string{fmt.Sprintf("\"%s_str\":\"%s\"", label, duration.String()),
				fmt.Sprintf("\"%s_nsecs\":%d", label, duration),
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
				WithFields(Fields{"bar": duration,
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
