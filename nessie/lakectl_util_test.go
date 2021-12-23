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
	s := "This is a string with ${number} ${vars} to expand. This $part $should ${not $expand"
	expected := "This is a string with 2 elements to expand. This $part $should ${not $expand"
	varMap := make(map[string]string)
	expanded, err := expandVariables(s, varMap)
	require.ErrorIs(t, err, errVariableNotFound, "Expected error for empty mapping did not happen")

	varMap["vars"] = "elements"
	expanded, err = expandVariables(s, varMap)
	require.ErrorIs(t, err, errVariableNotFound, "Expected error for empty mapping did not happen")

	// Setting wrong variable name. This should not affect the string
	varMap["numbers"] = "2"
	expanded, err = expandVariables(s, varMap)
	require.ErrorIs(t, err, errVariableNotFound, "Expected error for empty mapping did not happen")

	varMap["number"] = "2"
	expanded, err = expandVariables(s, varMap)
	require.NoError(t, err, "Unexpected error during expandVariables")
	require.Equal(t, expected, expanded, "Unexpected result from expandVariables")

	// Verify that only exact var pattern is recognized as var
	varMap["should"] = "could"
	varMap["not"] = "definitely"
	expanded, err = expandVariables(s, varMap)
	require.NoError(t, err, "Unexpected error during expandVariables")
	require.Equal(t, expected, expanded, "Unexpected result from expandVariables")
}
