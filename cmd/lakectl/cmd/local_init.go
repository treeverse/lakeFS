package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	initMinArgs = 1
	initMaxArgs = 2
)

var localInitCmd = &cobra.Command{
	Use:   "init [directory] <lakeFS path URI>",
	Short: "set a local directory to sync with a lakeFS ref and path",
	Args:  cobra.RangeArgs(initMinArgs, initMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		var remote *uri.URI
		dir := "."
		if len(args) == initMaxArgs {
			dir = args[0]
			remote = MustParsePathURI("remote ref", args[1])
		} else {
			remote = MustParsePathURI("remote ref", args[0])
		}

		localPath, err := filepath.Abs(dir)
		if err != nil {
			DieErr(err)
		}

		stat, err := os.Stat(localPath)
		switch {
		case errors.Is(err, os.ErrNotExist):
			if confirmation, err := Confirm(cmd.Flags(), fmt.Sprintf("Directory '%s' doesn't exist, create", localPath)); err != nil || !confirmation {
				Die("local init aborted", 1)
			}
			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				DieFmt("Failed to create dir: %s", localPath)
			}
		case err != nil:
			DieErr(err)
		case !stat.IsDir():
			DieErr(fmt.Errorf("not a directory: %w", os.ErrInvalid))
		}

		if IndexExists(localPath) {
			if confirmation, err := Confirm(cmd.Flags(),
				fmt.Sprintf("Directory '%s' already linked to a lakefs path, do you wish to override the configuration", localPath)); err != nil || !confirmation {
				Die("local init aborted", 1)
			}
		}

		// dereference
		head := resolveCommitOrDie(cmd.Context(), getClient(), remote.Repository, remote.Ref)
		err = WriteIndex(localPath, remote, head)
		if err != nil {
			DieErr(err)
		}

		if git.IsGitRepository(localPath) {
			ignoreFile, err := git.Ignore(localPath, []string{localPath, IndexFileName}, []string{IndexFileName}, IgnoreMarker)
			if err != nil {
				DieErr(err)
			}
			Fmt("location added to %s\n", ignoreFile)
		}

		Fmt("Successfully linked local directory '%s' with remote '%s'\n", localPath, remote.String())
	},
}

//nolint:gochecknoinits
func init() {
	localCmd.AddCommand(localInitCmd)
	AssignAutoConfirmFlag(localInitCmd.Flags())
}
