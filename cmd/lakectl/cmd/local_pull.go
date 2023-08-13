package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/local"
	"golang.org/x/sync/errgroup"
)

var localPullCmd = &cobra.Command{
	Use:   "pull [directory]",
	Short: "Fetch latest changes from lakeFS.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		_, localPath := getLocalArgs(args, false, false)
		force := Must(cmd.Flags().GetBool("force"))
		syncFlags := getLocalSyncFlags(cmd)
		idx, err := local.ReadIndex(localPath)
		if err != nil {
			DieErr(err)
		}

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}

		currentBase := remote.WithRef(idx.AtHead)
		client := getClient()
		// make sure no local changes
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		localChange := localDiff(ctx, client, currentBase, idx.LocalPath())
		if len(localChange) > 0 && !force {
			DieFmt("there are %d uncommitted changes. Either commit them first or use --force to revert local changes",
				len(localChange))
		}

		// write new index
		newHead := resolveCommitOrDie(ctx, client, remote.Repository, remote.Ref)
		idx, err = local.WriteIndex(idx.LocalPath(), remote, newHead)
		if err != nil {
			DieErr(err)
		}
		newBase := remote.WithRef(newHead)
		d := make(chan api.Diff, maxDiffPageSize)
		var wg errgroup.Group
		wg.Go(func() error {
			return diff.StreamRepositoryDiffs(ctx, client, currentBase, newBase, swag.StringValue(currentBase.Path), d, false)
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
		s := local.NewSyncManager(ctx, client, syncFlags.parallelism, syncFlags.presign)
		err = s.Sync(idx.LocalPath(), newBase, c)
		if errors.Is(ctx.Err(), context.Canceled) {
			Die("Operation was canceled. Local data may be incomplete.", 1)
		}
		if err != nil {
			DieErr(err)
		}
		if err := wg.Wait(); err != nil {
			DieErr(err)
		}
		summary := s.Summary()
		fmt.Printf("Successfully synced changes!.\nTotal objects downloaded:\t%d\nTotal objects removed:\t%d\n", summary.Downloaded, summary.Removed)
	},
}

//nolint:gochecknoinits
func init() {
	localPullCmd.Flags().Bool("force", false, "Reset any uncommitted local change")
	withLocalSyncFlags(localPullCmd)
	localCmd.AddCommand(localPullCmd)
}
