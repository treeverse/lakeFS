package actions

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// SecureString is a string that may be populated from an environment variable.
// If constructed with a string of the form {{ ENV.EXAMPLE_VARIABLE }}, the value is populated from EXAMPLE_VARIABLE and
// is considered a secret. Otherwise the value is taken from the string as-is, and is not considered a secret.
type SecureString struct {
	val    string
	secret bool
}

// Returns the string for non-secrets, or asterisks otherwise.
func (s SecureString) String() string {
	if s.secret {
		return "*******"
	}
	return s.val
}

var envVarRegex = regexp.MustCompile(`{{ ?ENV\..*? ?}}`)

// NewSecureString creates a new SecureString, reading env var if needed.
// If the string is not of the form {{ ENV.EXAMPLE_VARIABLE }}, the value is not considered a secret.
// If the string is of the form {{ ENV.EXAMPLE_VARIABLE }}, the value is populated from EXAMPLE_VARIABLE and
// is considered a secret.
// If the environment variable is not found, an error is returned.
// IF envEnabled is false, the value we evaluate is an empty string.
func NewSecureString(s string, envGetter EnvGetter) (SecureString, error) {
	matches := 0
	var err error
	ret := envVarRegex.ReplaceAllStringFunc(s, func(origin string) string {
		if err != nil {
			return ""
		}
		matches++
		raw := strings.Trim(origin, "{} ")
		source, envVarName, ok := strings.Cut(raw, ".")
		if !ok || source != "ENV" {
			return origin
		}

		val, ok := envGetter.Lookup(envVarName)
		if !ok {
			err = fmt.Errorf("%s not found: %w", envVarName, errMissingEnvVar)
			return ""
		}
		return val
	})
	if err != nil {
		return SecureString{}, err
	}
	if matches == 0 {
		return SecureString{val: s, secret: false}, nil
	}

	return SecureString{val: ret, secret: true}, nil
}

type EnvGetter interface {
	Lookup(name string) (string, bool)
}

type EnvironmentVariableGetter struct {
	Enabled bool
	Prefix  string
}

// NewEnvironmentVariableGetter creates a new EnvironmentVariableGetter.
// If envEnabled is false, the value we evaluate is an empty string.
// If filterPrefix is not empty, we only evaluate environment variables that start with this prefix.
func NewEnvironmentVariableGetter(envEnabled bool, prefix string) *EnvironmentVariableGetter {
	return &EnvironmentVariableGetter{
		Enabled: envEnabled,
		Prefix:  prefix,
	}
}

// Lookup retrieves the value of the environment variable named
// by the key. If the variable is present in the environment, the
// value (which may be empty) is returned and the boolean is true.
// This function doesn't provide a way to extract variables that can be used by viper.
func (o *EnvironmentVariableGetter) Lookup(name string) (string, bool) {
	if !o.Enabled {
		return "", true
	}
	if o.Prefix != "" && !strings.HasPrefix(name, o.Prefix) {
		return "", true
	}
	return os.LookupEnv(name)
}
