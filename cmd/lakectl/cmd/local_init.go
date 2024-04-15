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
	"github.com/treeverse/lakefs/pkg/api/apigen"
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

	// dereference first in case remote doesn't exist or not reachable
	head := resolveCommitOrDie(ctx, client, remote.Repository, remote.Ref)

	remotePath := remote.GetPath()
	if remotePath != "" && !strings.HasSuffix(remotePath, uri.PathSeparator) { // Verify path is not an existing object
		stat, err := client.StatObjectWithResponse(ctx, remote.Repository, remote.Ref, &apigen.StatObjectParams{
			Path: *remote.Path,
		})
		switch {
		case err != nil:
			DieErr(err)
		case stat.JSON200 != nil:
			DieFmt("lakeFS path %s is an existing object and cannot be used as a reference source", remote.String())
		case stat.JSON404 == nil:
			dieOnResponseError(stat, err)
		default: // Ensure we treat this path as a directory
			*remote.Path += uri.PathSeparator
		}
	}

	// when updating git, we need to verify that we are not trying to work on the git root directory
	if updateIgnore {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			gitDir, err := git.GetRepositoryPath(dir)
			if err != nil {
				// report an error only if it's not a git repository
				if !errors.Is(err, git.ErrNotARepository) && !errors.Is(err, git.ErrNoGit) {
					return "", err
				}
			} else if gitDir == dir {
				DieFmt("Directory '%s' is a git repository, please specify a sub-directory", dir)
			}
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
	Use:   "init <path URI> [directory]",
	Short: "set a local directory to sync with a lakeFS path.",
	Args:  cobra.RangeArgs(localInitMinArgs, localInitMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote, localPath := getSyncArgs(args, true, false)
		force := Must(cmd.Flags().GetBool(localForceFlagName))
		updateIgnore := Must(cmd.Flags().GetBool(localGitIgnoreFlagName))

		warnOnCaseInsensitiveDirectory(localPath)

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
