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
func NewSecureString(s string) (SecureString, error) {
	matches := 0
	var err error
	ret := envVarRegex.ReplaceAllStringFunc(s, func(origin string) string {
		if err != nil {
			return ""
		}
		matches++
		raw := strings.Trim(origin, "{} ")
		parts := strings.SplitN(raw, ".", 2) //nolint: gomnd
		if len(parts) != 2 || parts[0] != "ENV" {
			return origin
		}

		envVarName := parts[1]
		val, ok := os.LookupEnv(envVarName)
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
