package git

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateEntriesForIgnore_NoBackslashOnWindows(t *testing.T) {
	// Create a valid temporary directory to ignore
	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "input")
	require.NoError(t, os.Mkdir(inputDir, 0755))

	// Ignore the temporary directory
	entries, err := createEntriesForIgnore(tmpDir, []string{inputDir}, false)
	require.NoError(t, err)

	// Check that the output matches git conventions and does not contain backslashes
	require.Equal(t, []string{"input/*"}, entries)
	require.NotContains(t, entries[0], "\\")
}
