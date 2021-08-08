package actions

import (
	"os"
	"regexp"
)

// SecureString is a string tha may or may not contain a secret.
// Secret is considered any value that was read from an environment variable, in the following syntax:
// "{{ ENV.VAR_KEY }}"
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

var envVarRegex = regexp.MustCompile(`{{ ENV\.(.*) }}`)

// Creates a new SecureString, reading env var if needed.
func NewFromString(s string) (SecureString, error) {
	matches := envVarRegex.FindStringSubmatch(s)
	if len(matches) == 0 {
		return SecureString{val: s, secret: false}, nil
	}
	envVarName := s[len("{{ ENV.") : len(s)-len(" }}")]
	val, ok := os.LookupEnv(envVarName)
	if !ok {
		return SecureString{}, errMissingEnvVar
	}

	return SecureString{val: val, secret: true}, nil
}
