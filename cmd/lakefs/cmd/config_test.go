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

func TestNewConfigState(t *testing.T) {
	state := newConfigState()

	if state == nil {
		t.Fatal("newConfigState() returned nil")
	}

	if state.viper == nil {
		t.Error("newConfigState() viper is nil")
	}

	if len(state.categories) == 0 {
		t.Error("newConfigState() categories is empty")
	}

	// Check required categories exist
	requiredCategories := []string{categoryDatabase, categoryBlockstore, categorySecurity}
	for _, cat := range requiredCategories {
		if _, ok := state.categories[cat]; !ok {
			t.Errorf("newConfigState() missing required category %q", cat)
		}
		if !state.categories[cat].Required {
			t.Errorf("category %q should be required", cat)
		}
	}

	// Check optional categories exist
	optionalCategories := []string{categoryListen, categoryLogging, categoryTLS, categoryGateway, categoryStats}
	for _, cat := range optionalCategories {
		if _, ok := state.categories[cat]; !ok {
			t.Errorf("newConfigState() missing optional category %q", cat)
		}
		if state.categories[cat].Required {
			t.Errorf("category %q should be optional", cat)
		}
	}
}

func TestConfigStateAllRequiredConfigured(t *testing.T) {
	state := newConfigState()

	// Initially, no required categories are configured
	if state.allRequiredConfigured() {
		t.Error("allRequiredConfigured() should return false when no categories are configured")
	}

	// Configure all required categories
	state.categories[categoryDatabase].Configured = true
	state.categories[categoryBlockstore].Configured = true
	state.categories[categorySecurity].Configured = true

	if !state.allRequiredConfigured() {
		t.Error("allRequiredConfigured() should return true when all required categories are configured")
	}
}

func TestConfigStateGetMissingRequired(t *testing.T) {
	state := newConfigState()

	missing := state.getMissingRequired()
	if len(missing) != 3 {
		t.Errorf("getMissingRequired() should return 3 items, got %d", len(missing))
	}

	// Configure one required category
	state.categories[categoryDatabase].Configured = true

	missing = state.getMissingRequired()
	if len(missing) != 2 {
		t.Errorf("getMissingRequired() should return 2 items, got %d", len(missing))
	}
}

func TestConfigStateGetCategoryStatus(t *testing.T) {
	state := newConfigState()

	// Test required but not configured
	status := state.getCategoryStatus(state.categories[categoryDatabase])
	if status != statusRequired {
		t.Errorf("getCategoryStatus() for required unconfigured = %q, want %q", status, statusRequired)
	}

	// Test configured
	state.categories[categoryDatabase].Configured = true
	status = state.getCategoryStatus(state.categories[categoryDatabase])
	if status != statusConfigured {
		t.Errorf("getCategoryStatus() for configured = %q, want %q", status, statusConfigured)
	}

	// Test optional
	status = state.getCategoryStatus(state.categories[categoryLogging])
	if status != statusOptional {
		t.Errorf("getCategoryStatus() for optional = %q, want %q", status, statusOptional)
	}
}

func TestFormatStatus(t *testing.T) {
	tests := []struct {
		status string
		want   string
	}{
		{statusRequired, "[REQUIRED]  "},
		{statusConfigured, "[configured]"},
		{statusOptional, "[optional]  "},
		{statusIncomplete, "[INCOMPLETE]"},
		{"unknown", "[          ]"},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			got := formatStatus(tt.status)
			if got != tt.want {
				t.Errorf("formatStatus(%q) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestMaskSecret(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		want   string
	}{
		{
			name:   "long secret",
			secret: "1234567890abcdef",
			want:   "1234...cdef",
		},
		{
			name:   "short secret",
			secret: "12345678",
			want:   "***",
		},
		{
			name:   "very short secret",
			secret: "123",
			want:   "***",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskSecret(tt.secret)
			if got != tt.want {
				t.Errorf("maskSecret(%q) = %q, want %q", tt.secret, got, tt.want)
			}
		})
	}
}

func TestMaskConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		wantContains string
		wantNotContain string
	}{
		{
			name:           "with password",
			connStr:        "postgres://user:secretpassword@localhost:5432/db",
			wantContains:   "***",
			wantNotContain: "secretpassword",
		},
		{
			name:           "without password",
			connStr:        "postgres://localhost:5432/db",
			wantContains:   "localhost",
			wantNotContain: "",
		},
		{
			name:           "empty string",
			connStr:        "",
			wantContains:   "",
			wantNotContain: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskConnectionString(tt.connStr)
			if tt.wantContains != "" && !containsString(got, tt.wantContains) {
				t.Errorf("maskConnectionString(%q) = %q, should contain %q", tt.connStr, got, tt.wantContains)
			}
			if tt.wantNotContain != "" && containsString(got, tt.wantNotContain) {
				t.Errorf("maskConnectionString(%q) = %q, should not contain %q", tt.connStr, got, tt.wantNotContain)
			}
		})
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (substr == "" || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestUpdateCategoryStatus(t *testing.T) {
	state := newConfigState()

	// Set some values in viper
	state.viper.Set("database.type", "postgres")
	state.viper.Set("blockstore.type", "s3")
	state.viper.Set("auth.encrypt.secret_key", "validkey123456789")
	state.viper.Set("blockstore.signing.secret_key", "validkey987654321")
	state.viper.Set("listen_address", "0.0.0.0:8000")

	// Update category status
	state.updateCategoryStatus()

	// Check that categories are marked as configured
	if !state.categories[categoryDatabase].Configured {
		t.Error("database category should be configured")
	}
	if !state.categories[categoryBlockstore].Configured {
		t.Error("blockstore category should be configured")
	}
	if !state.categories[categorySecurity].Configured {
		t.Error("security category should be configured")
	}
	if !state.categories[categoryListen].Configured {
		t.Error("listen category should be configured")
	}
}

func TestUpdateCategoryStatusWithDefaultSecrets(t *testing.T) {
	state := newConfigState()

	// Set default/invalid secrets that should not count as configured
	state.viper.Set("auth.encrypt.secret_key", "THIS_MUST_BE_CHANGED_IN_PRODUCTION")
	state.viper.Set("blockstore.signing.secret_key", "OVERRIDE_THIS_SIGNING_SECRET_DEFAULT")

	state.updateCategoryStatus()

	// Security should NOT be configured with default secrets
	if state.categories[categorySecurity].Configured {
		t.Error("security category should NOT be configured with default secrets")
	}
}
