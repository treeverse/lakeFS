package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/local"
	"golang.org/x/sync/errgroup"
)

var localCheckoutCmd = &cobra.Command{
	Use:   "checkout [directory]",
	Short: "Sync local directory with the remote state.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		_, localPath := getLocalArgs(args, false)
		syncFlags := getLocalSyncFlags(cmd)
		specifiedRef := Must(cmd.Flags().GetString("ref"))
		idx, err := local.ReadIndex(localPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				DieFmt("directory %s is not linked to a lakeFS path", localPath)
			}
			DieErr(err)
		}

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}

		currentBase := remote.WithRef(idx.AtHead)
		client := getClient()
		diffs := localDiff(cmd.Context(), client, currentBase, idx.LocalPath())
		syncMgr := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		// confirm on local changes
		if len(diffs) > 0 {
			fmt.Println("Uncommitted changes exist, the operation will revert all changes on local directory.")
			confirmation, err := Confirm(cmd.Flags(), "Proceed")
			if err != nil || !confirmation {
				Die("command aborted", 1)
			}

			c := make(chan *local.Change, filesChanSize)
			// revert changes!
			go func() {
				defer close(c)
				for _, change := range local.Undo(diffs) {
					c <- change
				}
			}()
			err = syncMgr.Sync(idx.LocalPath(), currentBase, c)
			if err != nil {
				DieErr(err)
			}
		}

		if specifiedRef != "" {
			newRemote := remote.WithRef(specifiedRef)
			newHead := resolveCommitOrDie(cmd.Context(), client, newRemote.Repository, newRemote.Ref)
			newBase := newRemote.WithRef(newHead)
			// write new index
			_, err = local.WriteIndex(idx.LocalPath(), newRemote, newHead)
			if err != nil {
				DieErr(err)
			}

			var wg errgroup.Group
			d := make(chan api.Diff, maxDiffPageSize)
			wg.Go(func() error {
				return diff.StreamRepositoryDiffs(cmd.Context(), client, currentBase, newBase, swag.StringValue(currentBase.Path), d, true)
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

			err = syncMgr.Sync(idx.LocalPath(), newBase, c)
			if err != nil {
				DieErr(err)
			}
			err = wg.Wait()
			if err != nil {
				DieErr(err)
			}
		}
		summary := syncMgr.Summary()
		fmt.Printf("Checkout Summary:\nDownloaded:\t%d\nRemoved:\t%d\n", summary.Downloaded, summary.Removed)
	},
}

//nolint:gochecknoinits
func init() {
	localCheckoutCmd.Flags().StringP("ref", "r", "", "Checkout the given source branch or reference")
	AssignAutoConfirmFlag(localCheckoutCmd.Flags())
	withLocalSyncFlags(localCheckoutCmd)
	localCmd.AddCommand(localCheckoutCmd)
}
