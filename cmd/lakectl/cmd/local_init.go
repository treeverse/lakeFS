package cmd

import (
	"context"
	"errors"
	"fmt"
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

func localInit(ctx context.Context, dir string, remote *uri.URI, force bool) *local.Index {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		DieErr(err)
	}
	exists, err := local.IndexExists(dir)
	if err != nil {
		DieErr(err)
	}
	if exists && !force {
		DieFmt("directory '%s' already linked to a lakefs path, run command with --force to overwrite", dir)
	}

	// dereference
	head := resolveCommitOrDie(ctx, getClient(), remote.Repository, remote.Ref)
	idx, err := local.WriteIndex(dir, remote, head)
	if err != nil {
		DieErr(err)
	}

	ignoreFile, err := git.Ignore(dir, []string{dir}, []string{filepath.Join(dir, local.IndexFileName)}, local.IgnoreMarker)
	if err == nil {
		fmt.Println("location added to", ignoreFile)
	} else if !errors.Is(err, git.ErrNotARepository) {
		DieErr(err)
	}

	return idx
}

var localInitCmd = &cobra.Command{
	Use:   "init <path uri> [directory]",
	Short: "set a local directory to sync with a lakeFS path",
	Args:  cobra.RangeArgs(localInitMinArgs, localInitMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote := MustParsePathURI("path", args[0])
		dir := "."
		if len(args) == localInitMaxArgs {
			dir = args[1]
		}
		localPath, err := filepath.Abs(dir)
		if err != nil {
			DieErr(err)
		}
		force := Must(cmd.Flags().GetBool("force"))

		localInit(cmd.Context(), localPath, remote, force)

		fmt.Printf("Successfully linked local directory '%s' with remote '%s'\n", localPath, remote)
	},
}

//nolint:gochecknoinits
func init() {
	localInitCmd.Flags().Bool("force", false, "Overwrites if directory already linked to a lakeFS path")
	localCmd.AddCommand(localInitCmd)
}
