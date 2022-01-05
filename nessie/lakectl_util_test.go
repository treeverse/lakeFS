package nessie

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Utilities tests
func TestRunShellCommand(t *testing.T) {
	checkTerminal := "if [ -t 1 ] ; then echo -n terminal; else echo -n pipe; fi"

	output, err := runShellCommand(checkTerminal, true)
	require.NoError(t, err, "Failed to run shell command in terminal context")
	require.Equal(t, "terminal", string(output), "terminal was not detected properly")

	output, err = runShellCommand(checkTerminal, false)
	require.NoError(t, err, "Failed to run shell command out of terminal context")
	require.Equal(t, "pipe", string(output), "pipe was not detected properly")
}

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
