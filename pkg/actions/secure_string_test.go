package actions

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSecureString(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		err         error
		envVarToSet map[string]string
		expectedOut SecureString
	}{
		{
			name:        "No env var",
			input:       "this is just a string",
			expectedOut: SecureString{val: "this is just a string", secret: false},
		},
		{
			name:        "simple env var",
			input:       "{{ ENV.SIMPLE_FIRST }}",
			envVarToSet: map[string]string{"SIMPLE_FIRST": "some value"},
			expectedOut: SecureString{val: "some value", secret: true},
		},
		{
			name:        "no spaces env var",
			input:       "{{ENV.NO_SPACES_FIRST}}",
			envVarToSet: map[string]string{"NO_SPACES_FIRST": "this is some value"},
			expectedOut: SecureString{val: "this is some value", secret: true},
		},
		{
			name:        "wrapped with text",
			input:       "this {{ ENV.WRAPPED_FIRST }} value",
			envVarToSet: map[string]string{"WRAPPED_FIRST": "is another"},
			expectedOut: SecureString{val: "this is another value", secret: true},
		},
		{
			name:        "multiple vars and text",
			input:       "let me count: {{ ENV.MULTIPLE_FIRST }}, {{ENV.MULTIPLE_SECOND}}, {{ ENV.MULTIPLE_THIRD }}",
			envVarToSet: map[string]string{"MULTIPLE_FIRST": "one", "MULTIPLE_SECOND": "two", "MULTIPLE_THIRD": "three"},
			expectedOut: SecureString{val: "let me count: one, two, three", secret: true},
		},
		{
			name:        "not an env var",
			input:       "{{ NV.NOT_AN_ENV_VAR }}",
			envVarToSet: map[string]string{"NOT_AN_ENV_VAR": "one"},
			expectedOut: SecureString{val: "{{ NV.NOT_AN_ENV_VAR }}", secret: false},
		},
		{
			name:        "missing env var",
			input:       "{{ ENV.MISSING_FIRST }}",
			envVarToSet: map[string]string{"SIMPLE_FIRST": "some value"},
			expectedOut: SecureString{},
			err:         errMissingEnvVar,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.envVarToSet {
				require.NoError(t, os.Setenv(k, v))
			}

			out, err := NewSecureString(tt.input)
			if tt.err == nil {
				require.Nil(t, err)
			} else {
				require.True(t, errors.Is(err, tt.err))
			}
			require.Equal(t, tt.expectedOut, out)
		})
	}
}
