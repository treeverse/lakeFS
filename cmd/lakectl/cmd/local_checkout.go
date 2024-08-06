package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/local"
)

var localCheckoutCmd = &cobra.Command{
	Use:   "checkout [directory]",
	Short: "Sync local directory with the remote state.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		specifiedRef := Must(cmd.Flags().GetString("ref"))
		all := Must(cmd.Flags().GetBool("all"))
		_, localPath := getSyncArgs(args, false, all)
		if !all {
			localCheckout(cmd, localPath, specifiedRef, true)
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
			localCheckout(cmd, filepath.Join(localPath, d), specifiedRef, false)
		}
	},
}

func localCheckout(cmd *cobra.Command, localPath string, specifiedRef string, confirmByFlag bool) {
	client := getClient()
	syncFlags := getSyncFlags(cmd, client)
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
	diffs := local.Undo(localDiff(cmd.Context(), client, currentBase, idx.LocalPath()))
	sigCtx := localHandleSyncInterrupt(cmd.Context(), idx, string(checkoutOperation))
	syncMgr := local.NewSyncManager(sigCtx, client, getHTTPClient(), local.Config{
		SyncFlags:           syncFlags,
		SkipNonRegularFiles: cfg.Local.SkipNonRegularFiles,
		IncludePerm:         cfg.Experimental.Local.POSIXPerm.Enabled,
	})
	// confirm on local changes
	if confirmByFlag && len(diffs) > 0 {
		fmt.Println("Uncommitted changes exist, the operation will revert all changes on local directory.")
		confirmation, err := Confirm(cmd.Flags(), "Proceed")
		if err != nil || !confirmation {
			Die("command aborted", 1)
		}
	}

	if specifiedRef != "" {
		resolvedRef := MustParseRefURI("ref", specifiedRef)
		if resolvedRef.Repository != remote.Repository {
			DieFmt("invalid uri, ref repository doesn't match")
		}
		newRemote := remote.WithRef(resolvedRef.Ref)
		newHead := resolveCommitOrDie(cmd.Context(), client, newRemote.Repository, newRemote.Ref)
		if newHead != idx.AtHead {
			newBase := newRemote.WithRef(newHead)

			// write new index
			_, err = local.WriteIndex(idx.LocalPath(), remote, newHead, "")
			if err != nil {
				DieErr(err)
			}

			newDiffs := local.Undo(localDiff(cmd.Context(), client, newBase, idx.LocalPath()))
			diffs = diffs.MergeWith(newDiffs, local.MergeStrategyOther)
			currentBase = newBase
		}
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

	Write(localSummaryTemplate, struct {
		Operation string
		local.Tasks
	}{
		Operation: "Checkout",
		Tasks:     syncMgr.Summary(),
	})
}

//nolint:gochecknoinits
func init() {
	localCheckoutCmd.Flags().StringP("ref", "r", "", "Checkout the given reference")
	localCheckoutCmd.Flags().Bool("all", false, "Checkout given source branch or reference for all linked directories")
	localCheckoutCmd.MarkFlagsMutuallyExclusive("ref", "all")
	AssignAutoConfirmFlag(localCheckoutCmd.Flags())
	withSyncFlags(localCheckoutCmd)
	localCmd.AddCommand(localCheckoutCmd)
}
