package cmd

import (
	"testing"
)

func TestValidateListenAddress(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid address with all interfaces",
			input:   "0.0.0.0:8000",
			wantErr: false,
		},
		{
			name:    "valid address with localhost",
			input:   "127.0.0.1:8000",
			wantErr: false,
		},
		{
			name:    "valid address with hostname",
			input:   "localhost:8000",
			wantErr: false,
		},
		{
			name:    "valid address with empty host",
			input:   ":8000",
			wantErr: false,
		},
		{
			name:    "invalid - missing port",
			input:   "0.0.0.0",
			wantErr: true,
		},
		{
			name:    "invalid - port out of range",
			input:   "0.0.0.0:70000",
			wantErr: true,
		},
		{
			name:    "invalid - negative port",
			input:   "0.0.0.0:-1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateListenAddress(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateListenAddress(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateURL(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid http URL",
			input:   "http://localhost:8000",
			wantErr: false,
		},
		{
			name:    "valid https URL",
			input:   "https://example.com/path",
			wantErr: false,
		},
		{
			name:    "invalid - empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid - no scheme",
			input:   "localhost:8000",
			wantErr: true,
		},
		{
			name:    "invalid - ftp scheme",
			input:   "ftp://example.com",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateURL(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateURL(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidatePostgresConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid postgres URL",
			input:   "postgres://user:pass@localhost:5432/db",
			wantErr: false,
		},
		{
			name:    "valid postgresql URL",
			input:   "postgresql://user:pass@localhost:5432/db?sslmode=disable",
			wantErr: false,
		},
		{
			name:    "valid postgres URL with no auth",
			input:   "postgres://localhost:5432/db",
			wantErr: false,
		},
		{
			name:    "invalid - empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid - wrong scheme",
			input:   "mysql://user:pass@localhost:3306/db",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePostgresConnectionString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePostgresConnectionString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidatePath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid absolute path",
			input:   "/var/log/lakefs.log",
			wantErr: false,
		},
		{
			name:    "valid home path",
			input:   "~/lakefs/data",
			wantErr: false,
		},
		{
			name:    "invalid - empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid - relative path",
			input:   "relative/path",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePath(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePath(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestGenerateSecureSecret(t *testing.T) {
	secret1, err := generateSecureSecret(32)
	if err != nil {
		t.Fatalf("generateSecureSecret() error = %v", err)
	}

	if len(secret1) != 64 { // hex encoding doubles the length
		t.Errorf("generateSecureSecret() length = %d, want 64", len(secret1))
	}

	// Test uniqueness
	secret2, err := generateSecureSecret(32)
	if err != nil {
		t.Fatalf("generateSecureSecret() error = %v", err)
	}

	if secret1 == secret2 {
		t.Error("generateSecureSecret() should generate unique secrets")
	}
}

func TestIsValidHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     bool
	}{
		{
			name:     "valid simple hostname",
			hostname: "localhost",
			want:     true,
		},
		{
			name:     "valid FQDN",
			hostname: "server.example.com",
			want:     true,
		},
		{
			name:     "valid with hyphen",
			hostname: "my-server.example.com",
			want:     true,
		},
		{
			name:     "invalid - starts with hyphen",
			hostname: "-server.example.com",
			want:     false,
		},
		{
			name:     "invalid - ends with hyphen",
			hostname: "server-.example.com",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidHostname(tt.hostname)
			if got != tt.want {
				t.Errorf("isValidHostname(%q) = %v, want %v", tt.hostname, got, tt.want)
			}
		})
	}
}
