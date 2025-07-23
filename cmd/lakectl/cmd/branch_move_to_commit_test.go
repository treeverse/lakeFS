package cmd

import (
	"testing"
)

func TestValidateInputs(t *testing.T) {
	tests := []struct {
		name      string
		branchURI string
		commitRef string
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "valid inputs",
			branchURI: "lakefs://repo/branch",
			commitRef: "abc123",
			wantErr:   false,
		},
		{
			name:      "valid inputs with complex commit ref",
			branchURI: "lakefs://repo/branch",
			commitRef: "feature/my-branch~2",
			wantErr:   false,
		},
		{
			name:      "empty branch URI",
			branchURI: "",
			commitRef: "abc123",
			wantErr:   true,
			errMsg:    "branch URI cannot be empty",
		},
		{
			name:      "empty commit ref",
			branchURI: "lakefs://repo/branch",
			commitRef: "",
			wantErr:   true,
			errMsg:    "commit reference cannot be empty",
		},
		{
			name:      "invalid branch URI scheme",
			branchURI: "http://repo/branch",
			commitRef: "abc123",
			wantErr:   true,
			errMsg:    "branch URI must start with 'lakefs://'",
		},
		{
			name:      "branch URI with newline",
			branchURI: "lakefs://repo/branch\n",
			commitRef: "abc123",
			wantErr:   true,
			errMsg:    "branch URI contains invalid characters",
		},
		{
			name:      "commit ref with newline",
			branchURI: "lakefs://repo/branch",
			commitRef: "abc\n123",
			wantErr:   true,
			errMsg:    "commit reference contains invalid characters",
		},
		{
			name:      "branch URI with null byte",
			branchURI: "lakefs://repo/branch\x00",
			commitRef: "abc123",
			wantErr:   true,
			errMsg:    "branch URI contains invalid characters",
		},
		{
			name:      "commit ref with null byte",
			branchURI: "lakefs://repo/branch",
			commitRef: "abc\x00123",
			wantErr:   true,
			errMsg:    "commit reference contains invalid characters",
		},
		{
			name:      "commit ref with invalid characters",
			branchURI: "lakefs://repo/branch",
			commitRef: "abc@#$%123",
			wantErr:   true,
			errMsg:    "commit reference contains invalid characters (allowed: alphanumeric, ., _, /, ~, -)",
		},
		{
			name:      "commit ref with spaces",
			branchURI: "lakefs://repo/branch",
			commitRef: "abc 123",
			wantErr:   true,
			errMsg:    "commit reference contains invalid characters (allowed: alphanumeric, ., _, /, ~, -)",
		},
		{
			name:      "branch URI too long",
			branchURI: "lakefs://repo/" + generateLongString(1000),
			commitRef: "abc123",
			wantErr:   true,
			errMsg:    "branch URI too long",
		},
		{
			name:      "commit ref too long",
			branchURI: "lakefs://repo/branch",
			commitRef: generateLongString(501),
			wantErr:   true,
			errMsg:    "commit reference too long",
		},
		{
			name:      "valid commit ref with all allowed characters",
			branchURI: "lakefs://repo/branch",
			commitRef: "abc123._/~-DEF456",
			wantErr:   false,
		},
		{
			name:      "valid git commit hash",
			branchURI: "lakefs://repo/branch",
			commitRef: "1234567890abcdef1234567890abcdef12345678",
			wantErr:   false,
		},
		{
			name:      "valid branch reference",
			branchURI: "lakefs://repo/branch",
			commitRef: "main",
			wantErr:   false,
		},
		{
			name:      "valid tag reference",
			branchURI: "lakefs://repo/branch",
			commitRef: "v1.0.0",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInputs(tt.branchURI, tt.commitRef)
			if tt.wantErr {
				if err == nil {
					t.Errorf("validateInputs() expected error but got none")
					return
				}
				if tt.errMsg != "" {
					// Check if error message contains expected substring for dynamic messages
					if tt.errMsg == "branch URI too long" {
						expectedMsg := "branch URI too long (max 1000 characters)"
						if err.Error() != expectedMsg {
							t.Errorf("validateInputs() error = %v, want %v", err, expectedMsg)
						}
					} else if tt.errMsg == "commit reference too long" {
						expectedMsg := "commit reference too long (max 500 characters)"
						if err.Error() != expectedMsg {
							t.Errorf("validateInputs() error = %v, want %v", err, expectedMsg)
						}
					} else if err.Error() != tt.errMsg {
						t.Errorf("validateInputs() error = %v, want %v", err, tt.errMsg)
					}
				}
			} else {
				if err != nil {
					t.Errorf("validateInputs() unexpected error = %v", err)
				}
			}
		})
	}
}

// generateLongString creates a string of specified length for testing
func generateLongString(length int) string {
	result := make([]byte, length)
	for i := range result {
		result[i] = 'a'
	}
	return string(result)
}

func TestValidateInputs_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		branchURI string
		commitRef string
		wantErr   bool
	}{
		{
			name:      "branch URI exactly at max length",
			branchURI: "lakefs://repo/" + generateLongString(986), // "lakefs://repo/" = 14 chars, 986 + 14 = 1000
			commitRef: "abc123",
			wantErr:   false,
		},
		{
			name:      "commit ref exactly at max length",
			branchURI: "lakefs://repo/branch",
			commitRef: generateLongString(500),
			wantErr:   false,
		},
		{
			name:      "branch URI one char over max",
			branchURI: "lakefs://repo/" + generateLongString(987), // 987 + 14 = 1001
			commitRef: "abc123",
			wantErr:   true,
		},
		{
			name:      "commit ref one char over max",
			branchURI: "lakefs://repo/branch",
			commitRef: generateLongString(501),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInputs(tt.branchURI, tt.commitRef)
			if tt.wantErr && err == nil {
				t.Errorf("validateInputs() expected error but got none")
			} else if !tt.wantErr && err != nil {
				t.Errorf("validateInputs() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateInputs_SuspiciousCharacters(t *testing.T) {
	suspiciousChars := []struct {
		char string
		name string
	}{
		{"\n", "newline"},
		{"\r", "carriage return"},
		{"\t", "tab"},
		{"\x00", "null byte"},
		{"\x1f", "unit separator"},
	}

	for _, sc := range suspiciousChars {
		t.Run("branch URI with "+sc.name, func(t *testing.T) {
			err := validateInputs("lakefs://repo/branch"+sc.char, "abc123")
			if err == nil {
				t.Errorf("validateInputs() expected error for branch URI with %s", sc.name)
			}
		})

		t.Run("commit ref with "+sc.name, func(t *testing.T) {
			err := validateInputs("lakefs://repo/branch", "abc"+sc.char+"123")
			if err == nil {
				t.Errorf("validateInputs() expected error for commit ref with %s", sc.name)
			}
		})
	}
}