package cmd

import (
	"fmt"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/local"
	"golang.org/x/sync/errgroup"
)

var localPullCmd = &cobra.Command{
	Use:   "pull [directory]",
	Short: "Fetch latest changes from lakeFS.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		_, localPath := getSyncArgs(args, false, false)
		force := Must(cmd.Flags().GetBool(localForceFlagName))
		syncFlags := getSyncFlags(cmd, client)
		idx, err := local.ReadIndex(localPath)
		if err != nil {
			DieErr(err)
		}

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}

		dieOnInterruptedOperation(LocalOperation(idx.ActiveOperation), force)

		currentBase := remote.WithRef(idx.AtHead)
		// make sure no local changes
		localChange := localDiff(cmd.Context(), client, currentBase, idx.LocalPath())
		if len(localChange) > 0 && !force {
			DieFmt("there are %d uncommitted changes. Either commit them first or use --force to revert local changes",
				len(localChange))
		}

		// write new index
		newHead := resolveCommitOrDie(cmd.Context(), client, remote.Repository, remote.Ref)
		_, err = local.WriteIndex(idx.LocalPath(), remote, newHead, "")
		if err != nil {
			DieErr(err)
		}
		newBase := remote.WithRef(newHead)
		d := make(chan apigen.Diff, maxDiffPageSize)
		var wg errgroup.Group
		wg.Go(func() error {
			return diff.StreamRepositoryDiffs(cmd.Context(), client, currentBase, newBase, swag.StringValue(currentBase.Path), d, false)
		})
		c := make(chan *local.Change, filesChanSize)
		wg.Go(func() error {
			defer close(c)
			for dif := range d {
				c <- &local.Change{
					Source: local.ChangeSourceRemote,
					Path:   strings.TrimPrefix(dif.Path, currentBase.GetPath()),
					Type:   local.ChangeTypeFromString(dif.Type),
				}
			}
			return nil
		})
		sigCtx := localHandleSyncInterrupt(cmd.Context(), idx, string(pullOperation))
		s := local.NewSyncManager(sigCtx, client, getHTTPClient(), syncFlags)
		err = s.Sync(idx.LocalPath(), newBase, c)
		if err != nil {
			DieErr(err)
		}
		if err := wg.Wait(); err != nil {
			DieErr(err)
		}

		fmt.Printf("\nSuccessfully synced changes!\n")
		Write(localSummaryTemplate, struct {
			Operation string
			local.Tasks
		}{
			Operation: "Pull",
			Tasks:     s.Summary(),
		})
	},
}

//nolint:gochecknoinits
func init() {
	withForceFlag(localPullCmd, "Reset any uncommitted local change")
	withSyncFlags(localPullCmd)
	localCmd.AddCommand(localPullCmd)
}
