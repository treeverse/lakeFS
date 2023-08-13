package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/treeverse/lakefs/pkg/local"
)

var localCheckoutCmd = &cobra.Command{
	Use:   "checkout [directory]",
	Short: "Sync local directory with the remote state.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		syncFlags := getLocalSyncFlags(cmd)
		specifiedRef := Must(cmd.Flags().GetString("ref"))
		all := Must(cmd.Flags().GetBool("all"))
		_, localPath := getLocalArgs(args, false, all)
		if !all {
			localCheckout(cmd.Context(), localPath, syncFlags, specifiedRef, cmd.Flags(), true)
			return
		}
		fmt.Println("the operation will revert all changes in all directories that are linked with lakeFS.")
		confirmation, err := Confirm(cmd.Flags(), "Proceed")
		if err != nil || !confirmation {
			Die("command aborted", 1)
		}
		dirs, err := local.FindIndices(localPath)
		if err != nil {
			DieErr(err)
		}
		for _, d := range dirs {
			localCheckout(cmd.Context(), filepath.Join(localPath, d), syncFlags, specifiedRef, cmd.Flags(), false)
		}
	},
}

func localCheckout(ctx context.Context, localPath string, syncFlags syncFlags, specifiedRef string, flags *pflag.FlagSet, confirmByFlag bool) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
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
	if confirmByFlag && len(diffs) > 0 {
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
		_, err = local.WriteIndex(idx.LocalPath(), remote, newHead)
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
	if errors.Is(ctx.Err(), context.Canceled) {
		Die("Operation was canceled, local data may be incomplete", 1)
	}
	if err != nil {
		DieErr(err)
	}

	summary := syncMgr.Summary()
	fmt.Printf("Checkout Summary:\nDownloaded:\t%d\nRemoved:\t%d\n", summary.Downloaded, summary.Removed)
}

//nolint:gochecknoinits
func init() {
	localCheckoutCmd.Flags().StringP("ref", "r", "", "Checkout the given source branch or reference")
	localCheckoutCmd.Flags().Bool("all", false, "Checkout given source branch or reference for all linked directories")
	localCheckoutCmd.MarkFlagsMutuallyExclusive("ref", "all")
	AssignAutoConfirmFlag(localCheckoutCmd.Flags())
	withLocalSyncFlags(localCheckoutCmd)
	localCmd.AddCommand(localCheckoutCmd)
}
