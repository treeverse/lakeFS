package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/git"
)

const (
	initMinArgs = 1
	initMaxArgs = 2
)

var localInitCmd = &cobra.Command{
	Use:   "init <path uri> [directory]",
	Short: "set a local directory to sync with a lakeFS path",
	Args:  cobra.RangeArgs(initMinArgs, initMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote := MustParsePathURI("path", args[0])
		dir := "."
		if len(args) == initMaxArgs {
			dir = args[1]
		}
		flagSet := cmd.Flags()
		force := MustBool(flagSet.GetBool("force"))

		localPath, err := filepath.Abs(dir)
		if err != nil {
			DieErr(err)
		}

		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			DieErr(err)
		}
		if IndexExists(localPath) && !force {
			Die(fmt.Sprintf("directory '%s' already linked to a lakefs path, run command with --force to overwrite", localPath), 1)
		}

		// dereference
		head := resolveCommitOrDie(cmd.Context(), getClient(), remote.Repository, remote.Ref)
		err = WriteIndex(localPath, remote, head)
		if err != nil {
			DieErr(err)
		}

		ignoreFile, err := git.Ignore(localPath, []string{localPath, IndexFileName}, []string{IndexFileName}, IgnoreMarker)
		if err != nil && !errors.Is(err, git.ErrNotARepository) {
			DieErr(err)
		} else if err == nil {
			fmt.Println("location added to", ignoreFile)
		}

		fmt.Printf("Successfully linked local directory '%s' with remote '%s'\n", localPath, remote)
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(localInitCmd.Flags())
	localInitCmd.Flags().Bool("force", false, "Overwrites if directory already linked to a lakeFS path")
	localCmd.AddCommand(localInitCmd)
}
