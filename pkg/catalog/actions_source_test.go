package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionFileNames(t *testing.T) {
	t.Run("skips a nested directory", func(t *testing.T) {
		// The directory itself comes back as a common level, and loading it as an
		// action file failed every commit on the branch (#10576).
		entries := []*DBEntry{
			{Path: "_lakefs_actions/check.yaml"},
			{Path: "_lakefs_actions/archive/", CommonLevel: true},
			{Path: "_lakefs_actions/nested/check.yaml"},
		}

		require.Equal(t, []string{
			"_lakefs_actions/check.yaml",
			"_lakefs_actions/nested/check.yaml",
		}, actionFileNames(entries))
	})

	t.Run("keeps every file when there is no directory", func(t *testing.T) {
		entries := []*DBEntry{
			{Path: "_lakefs_actions/a.yaml"},
			{Path: "_lakefs_actions/b.yaml"},
		}

		require.Equal(t, []string{
			"_lakefs_actions/a.yaml",
			"_lakefs_actions/b.yaml",
		}, actionFileNames(entries))
	})

	t.Run("empty page", func(t *testing.T) {
		require.Empty(t, actionFileNames(nil))
	})
}
