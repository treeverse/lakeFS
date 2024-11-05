package internal_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	giterror "github.com/treeverse/lakefs/pkg/git/errors"
	"github.com/treeverse/lakefs/pkg/git/internal"
)

var testErr = errors.New("this is a test generated error")

func TestHandleOutput(t *testing.T) {
	testCases := []struct {
		Name          string
		Output        string
		Error         error
		ExpectedError error
	}{
		{
			Name:          "not git repository",
			Output:        "fatal: not a git repository (or any of the parent directories): .git",
			Error:         testErr,
			ExpectedError: giterror.ErrNotARepository,
		},
		{
			Name:          "not a git repository - Mount",
			Output:        "fatal: Not a git repository (or any parent up to mount point /home/my_home)\nStopping at filesystem boundary (GIT_DISCOVERY_ACROSS_FILESYSTEM not set).",
			Error:         testErr,
			ExpectedError: giterror.ErrNotARepository,
		},
		{
			Name:          "other error",
			Output:        "Some other error happened",
			Error:         testErr,
			ExpectedError: testErr,
		},
		{
			Name:          "remote not found",
			Output:        "prefix, Remote nOt founD, suffix",
			Error:         testErr,
			ExpectedError: giterror.ErrRemoteNotFound,
		},
		{
			Name:   "no error",
			Output: "This is a valid response",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			str, err := internal.HandleOutput(tt.Output, tt.Error)
			require.ErrorIs(t, err, tt.ExpectedError)
			if tt.ExpectedError != nil {
				require.Equal(t, "", str)
			} else {
				require.Equal(t, tt.Output, str)
			}

		})
	}
}
