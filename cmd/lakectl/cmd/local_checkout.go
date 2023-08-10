package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/treeverse/lakefs/pkg/local"
)

var localCheckoutCmd = &cobra.Command{
	Use:   "checkout [directory]",
	Short: "Sync local directory with the remote state.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		_, localPath := getLocalArgs(args, false)
		syncFlags := getLocalSyncFlags(cmd)
		specifiedRef := Must(cmd.Flags().GetString("ref"))

		checkout(cmd.Context(), localPath, syncFlags, specifiedRef, cmd.Flags())
	},
}

func checkout(ctx context.Context, localPath string, syncFlags syncFlags, specifiedRef string, flags *pflag.FlagSet) {
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
	diffs := local.Undo(localDiff(ctx, client, currentBase, idx.LocalPath()))
	syncMgr := local.NewSyncManager(ctx, client, syncFlags.parallelism, syncFlags.presign)
	// confirm on local changes
	if len(diffs) > 0 {
		fmt.Println("Uncommitted changes exist, the operation will revert all changes on local directory.")
		confirmation, err := Confirm(flags, "Proceed")
		if err != nil || !confirmation {
			Die("command aborted", 1)
		}
	}

	if specifiedRef != "" && specifiedRef != idx.AtHead {
		newRemote := remote.WithRef(specifiedRef)
		newHead := resolveCommitOrDie(ctx, client, newRemote.Repository, newRemote.Ref)
		newBase := newRemote.WithRef(newHead)
		// write new index
		_, err = local.WriteIndex(idx.LocalPath(), newRemote, newHead)
		if err != nil {
			DieErr(err)
		}

		newDiffs := local.Undo(localDiff(ctx, client, newBase, idx.LocalPath()))
		diffs = diffs.MergeWith(newDiffs, local.MergeStrategyOther)
		currentBase = newBase
	}
	c := make(chan *local.Change, filesChanSize)
	go func() {
		defer close(c)
		for _, dif := range diffs {
			c <- &local.Change{
				Source: local.ChangeSourceRemote,
				Path:   strings.TrimPrefix(dif.Path, currentBase.GetPath()),
				Type:   dif.Type,
			}
		}
	}()
	err = syncMgr.Sync(idx.LocalPath(), currentBase, c)
	if err != nil {
		DieErr(err)
	}

	summary := syncMgr.Summary()
	fmt.Printf("Checkout Summary:\nDownloaded:\t%d\nRemoved:\t%d\n", summary.Downloaded, summary.Removed)
}

//nolint:gochecknoinits
func init() {
	localCheckoutCmd.Flags().StringP("ref", "r", "", "Checkout the given source branch or reference")
	AssignAutoConfirmFlag(localCheckoutCmd.Flags())
	withLocalSyncFlags(localCheckoutCmd)
	localCmd.AddCommand(localCheckoutCmd)
}
