package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	localInitMinArgs = 1
	localInitMaxArgs = 2
)

func localInit(ctx context.Context, dir string, remote *uri.URI, force, updateIgnore bool) (string, error) {
	client := getClient()
	if len(remote.GetPath()) > 0 { // Verify path is not an existing object
		stat, err := client.StatObjectWithResponse(ctx, remote.Repository, remote.Ref, &api.StatObjectParams{
			Path: *remote.Path,
		})
		switch {
		case err != nil:
			DieErr(err)
		case stat.JSON200 != nil:
			DieFmt("lakeFS path %s is an existing object and cannot be used as a reference source", remote.String())
		case stat.JSON404 == nil:
			dieOnResponseError(stat, err)
		}

		if !strings.HasSuffix(remote.GetPath(), uri.PathSeparator) { // Ensure we treat this path as a directory
			*remote.Path += uri.PathSeparator
		}
	}

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		DieErr(err)
	}
	exists, err := local.IndexExists(dir)
	if err != nil {
		return "", err
	}
	if exists && !force {
		return "", fs.ErrExist
	}

	// dereference
	head := resolveCommitOrDie(ctx, getClient(), remote.Repository, remote.Ref)
	_, err = local.WriteIndex(dir, remote, head, "")
	if err != nil {
		return "", err
	}

	if updateIgnore {
		ignoreFile, err := git.Ignore(dir, []string{dir}, []string{filepath.Join(dir, local.IndexFileName)}, local.IgnoreMarker)
		if err == nil {
			fmt.Println("Location added to", ignoreFile)
		} else if !(errors.Is(err, git.ErrNotARepository) || errors.Is(err, git.ErrNoGit)) {
			return "", err
		}
	}

	return head, nil
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
