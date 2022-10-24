package esti

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandVariables(t *testing.T) {
	type test struct {
		// In is the input string to expand.
		In string
		// Vars are the variable contents to expand into In.
		Vars map[string]string
		// ExpectedOut is the expected output string if no error is expected.
		ExpectedOut string
		// ExpectedErr is the expected error.  It is tested using errors.Is.
		ExpectedErr error
	}

	s := "This is a string with ${number} ${vars} to expand. This $part $should ${not $expand"
	expected := "This is a string with 2 elements to expand. This $part $should ${not $expand"

	tests := []test{
		{In: s, Vars: map[string]string{}, ExpectedOut: "", ExpectedErr: errVariableNotFound},
		{In: s, Vars: map[string]string{"vars": "elements"}, ExpectedOut: "", ExpectedErr: errVariableNotFound},
		{In: s, Vars: map[string]string{"vars": "elements", "numbers": "2"}, ExpectedOut: "", ExpectedErr: errVariableNotFound},
		{In: s, Vars: map[string]string{"vars": "elements", "numbers": "2", "number": "2"}, ExpectedOut: expected, ExpectedErr: nil},
		{In: s, Vars: map[string]string{"vars": "elements", "numbers": "2", "number": "2", "should": "could", "not": "definitely"}, ExpectedOut: expected, ExpectedErr: nil},
	}

	for _, tc := range tests {
		expanded, err := expandVariables(tc.In, tc.Vars)
		if tc.ExpectedErr != nil {
			require.ErrorIs(t, err, tc.ExpectedErr, "Unexpected error from expandVariables")
		} else {
			require.NoError(t, err, "Unexpected error during expandVariables")
			require.Equal(t, tc.ExpectedOut, expanded, "Unexpected result from expandVariables")
		}
	}
}
