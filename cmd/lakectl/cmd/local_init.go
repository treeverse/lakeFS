package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	localInitMinArgs = 1
	localInitMaxArgs = 2
)

func localInit(ctx context.Context, dir string, remote *uri.URI, force, updateIgnore bool) (*local.Index, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		DieErr(err)
	}
	exists, err := local.IndexExists(dir)
	if err != nil {
		return nil, err
	}
	if exists && !force {
		return nil, fs.ErrExist
	}

	// dereference
	head := resolveCommitOrDie(ctx, getClient(), remote.Repository, remote.Ref)
	idx, err := local.WriteIndex(dir, remote, head)
	if err != nil {
		return nil, err
	}

	if updateIgnore {
		ignoreFile, err := git.Ignore(dir, []string{dir}, []string{filepath.Join(dir, local.IndexFileName)}, local.IgnoreMarker)
		if err == nil {
			fmt.Println("Location added to", ignoreFile)
		} else if !(errors.Is(err, git.ErrNotARepository) || errors.Is(err, git.ErrNoGit)) {
			return nil, err
		}
	}

	return idx, nil
}

var localInitCmd = &cobra.Command{
	Use:   "init <path uri> [directory]",
	Short: "set a local directory to sync with a lakeFS path.",
	Args:  cobra.RangeArgs(localInitMinArgs, localInitMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote, localPath := getLocalArgs(args, true, false)
		force := Must(cmd.Flags().GetBool(localForceFlagName))
		updateIgnore := Must(cmd.Flags().GetBool(localGitIgnoreFlagName))
		_, err := localInit(cmd.Context(), localPath, remote, force, updateIgnore)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				DieFmt("directory '%s' already linked to a lakeFS path, run command with --force to overwrite", localPath)
			}
			DieErr(err)
		}

		fmt.Printf("Successfully linked local directory '%s' with remote '%s'\n", localPath, remote)
	},
}

//nolint:gochecknoinits
func init() {
	withForceFlag(localInitCmd, "Overwrites if directory already linked to a lakeFS path")
	withGitIgnoreFlag(localInitCmd)
	localCmd.AddCommand(localInitCmd)
}
