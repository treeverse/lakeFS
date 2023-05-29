package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	defaultSyncParallelism  = 25
	defaultLocalSyncPresign = true
)

var localCmd = &cobra.Command{
	Use:   "local",
	Short: "sync local directories with remote lakeFS locations",
}

func readLocalIndex(path string) *local.Index {
	idx, err := local.ReadIndex(path)
	if err != nil {
		DieErr(err)
	}
	return idx
}

func localDiff(ctx context.Context, client api.ClientWithResponsesInterface, remote *uri.URI, path string) local.Changes {
	currentRemoteState, err := local.ListRemote(ctx, client, remote)
	if err != nil {
		DieErr(err)
	}
	changes, err := local.DiffLocal(currentRemoteState, path)
	if err != nil {
		DieErr(err)
	}
	return changes
}

var localPullCmd = &cobra.Command{
	Use:   "pull [directory]",
	Short: "Fetch latest changes for the given directory",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		path := "."
		if len(args) == 1 {
			path = args[0]
		}
		syncFlags := getLocalSyncFlags(cmd)
		force := MustBool(cmd.Flags().GetBool("force"))

		// modify index
		idx := readLocalIndex(path)
		client := getClient()
		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}
		currentBase, err := idx.GetBaseURI()
		if err != nil {
			DieErr(err)
		}

		// make sure no local changes
		changes := localDiff(cmd.Context(), client, currentBase, idx.LocalPath())
		if len(changes) > 0 && !force {
			DieFmt("you have %d uncommitted changes. Either commit them first, or use --force to overwrite local changes",
				len(changes))
		}

		// write new index
		newHead, err := local.DereferenceURI(cmd.Context(), client, remote)
		err = local.WriteIndex(idx.LocalPath(), remote, newHead)
		if err != nil {
			DieErr(err)
		}
		newBase := local.WithRef(remote, newHead)
		diff, err := local.DiffRemotes(cmd.Context(), client, currentBase, newBase) // stuff changed on remote
		mgr := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		err = mgr.Sync(idx.LocalPath(), newBase, diff)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("synced %d changes!\n", len(diff))
	},
}

var localCheckoutCmd = &cobra.Command{
	Use:   "checkout [directory]",
	Short: "sync the local directory with the remote state, overwriting any local change, if any",
	Args:  cobra.RangeArgs(0, 2),
	Run: func(cmd *cobra.Command, args []string) {
		path := "."
		if len(args) > 0 {
			path = args[0]
		}
		syncFlags := getLocalSyncFlags(cmd)
		specifiedRef := MustString(cmd.Flags().GetString("ref"))
		all := MustBool(cmd.Flags().GetBool("all"))
		if all {
			if specifiedRef != "" {
				DieFmt("choose either --all or --ref")
			}
			indices, err := local.FindIndices(path)
			if err != nil {
				DieErr(err)
			}
			for _, idxPath := range indices {
				idx := readLocalIndex(idxPath)
				Fmt("checking out directory: %s\n", idx.LocalPath())
				client := getClient()
				remote, err := idx.GetCurrentURI()
				if err != nil {
					DieErr(err)
				}
				remoteBase := local.WithRef(remote, idx.Head)
				Fmt("diff 'local://%s' <--> '%s'...\n", idx.LocalPath(), remoteBase)
				diff := localDiff(cmd.Context(), client, remoteBase, idx.LocalPath())

				// reverse changes!
				s := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
				err = s.Sync(idx.LocalPath(), remoteBase, local.Undo(diff))
				if err != nil {
					DieErr(err)
				}
			}
			return
		}

		idx := readLocalIndex(path)
		client := getClient()
		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}
		currentBase := local.WithRef(remote, idx.Head)

		if specifiedRef != "" {
			// make sure no local changes
			changes := localDiff(cmd.Context(), client, currentBase, idx.LocalPath())
			if len(changes) > 0 {
				DieFmt("you have %d uncommitted changes. Either commit them first, or use --force to discard them")
			}

			// write new index
			newRemote := local.WithRef(remote, specifiedRef)
			newHead, err := local.DereferenceURI(cmd.Context(), client, newRemote)
			err = local.WriteIndex(idx.LocalPath(), newRemote, newHead)
			if err != nil {
				DieErr(err)
			}
			newBase := local.WithRef(newRemote, newHead)
			diff, err := local.DiffRemotes(cmd.Context(), client, currentBase, newBase, local.DiffTypeTwoWay)
			mgr := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
			err = mgr.Sync(idx.LocalPath(), local.WithRef(newRemote, newHead), diff)
			if err != nil {
				DieErr(err)
			}
			fmt.Printf("synced %d changes!\n", len(diff))
			return
		}
		// otherwise, checkout the version that appears in index
		Fmt("diff 'local://%s' <--> '%s'...\n", idx.LocalPath(), currentBase)
		diff := localDiff(cmd.Context(), client, currentBase, idx.LocalPath())

		// reverse changes!
		s := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		err = s.Sync(idx.LocalPath(), currentBase, local.Undo(diff))
		if err != nil {
			DieErr(err)
		}

	},
}

var localCommitCmd = &cobra.Command{
	Use:   "commit [directory]",
	Short: "upload & commit local changes to the lakeFS branch it tracks",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		path := "."
		if len(args) > 0 {
			path = args[0]
		}
		syncFlags := getLocalSyncFlags(cmd)
		message := MustString(cmd.Flags().GetString("message"))
		idx := readLocalIndex(path)
		client := getClient()

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}
		remoteBase := local.WithRef(remote, idx.Head)
		Fmt("diff 'local://%s' <--> '%s'...\n", idx.LocalPath(), remoteBase)
		diff := localDiff(cmd.Context(), client, remoteBase, idx.LocalPath())

		// remote diff
		// TODO: create a branch, commit THERE, and merge into remote to get a conflict if any.
		currentHead, err := local.DereferenceURI(cmd.Context(), client, remote)
		if err != nil {
			DieErr(err)
		}
		Fmt("diff '%s' <--> '%s'...\n", remoteBase, local.WithRef(remote, currentHead))
		remoteDiff, err := local.DiffRemotes(cmd.Context(), client, remoteBase, local.WithRef(remote, currentHead))
		if err != nil {
			DieErr(err)
		}

		if len(diff) == 0 {
			Fmt("\nNo local changes found.\n")
			return
		}

		diff = diff.MergeWith(remoteDiff)

		// sync them
		s := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		err = s.Sync(idx.LocalPath(), remote, diff)
		if err != nil {
			DieErr(err)
		}

		// add kv pairs if any
		kvPairs, err := getKV(cmd, metaFlagName)
		if err != nil {
			DieErr(err)
		}
		// add git context to kv pairs, if any
		if local.IsGitRepository(idx.LocalPath()) {
			gitRef, err := local.GetGitCurrentCommit(idx.LocalPath())
			if err == nil {
				md, err := local.GitMetadataFor(idx.LocalPath(), gitRef)
				if err == nil {
					for k, v := range md {
						kvPairs[k] = v
					}
				}
			}
		}

		// commit!
		response, err := client.CommitWithResponse(cmd.Context(), remote.Repository, remote.Ref, &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: message,
			Metadata: &api.CommitCreation_Metadata{
				AdditionalProperties: kvPairs,
			},
		})
		if err != nil {
			DieErr(err)
		}
		if response.StatusCode() != http.StatusCreated {
			DieFmt("Unexpected error committing changes: HTTP %d", response.StatusCode())
		}

		newHead := response.JSON201.Id
		err = local.WriteIndex(idx.LocalPath(), remote, newHead)
		if err != nil {
			DieErr(err)
		}

		// sync again with the new ID
		newBase := local.WithRef(remote, newHead)
		newRemoteState, err := local.ListRemote(cmd.Context(), client, newBase)
		if err != nil {
			DieErr(err)
		}
		resyncDiff, err := local.DiffLocal(newRemoteState, idx.LocalPath())
		if err != nil {
			DieErr(err)
		}
		mgr := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		err = mgr.Sync(idx.LocalPath(), newBase, resyncDiff)
		if err != nil {
			DieErr(err)
		}

		fmt.Printf("synced %d changes!\n", len(diff))
	},
}

var localStatusCmd = &cobra.Command{
	Use:   "status [directory]",
	Short: "show modifications (both remote and local) to the directory and the remote location it tracks",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		localOnly := MustBool(cmd.Flags().GetBool("local"))
		path := "."
		if len(args) > 0 {
			path = args[0]
		}
		idx := readLocalIndex(path)
		client := getClient()

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}
		remoteBase := local.WithRef(remote, idx.Head)
		Fmt("diff 'local://%s' <--> '%s'...\n", idx.LocalPath(), remoteBase)
		diff := localDiff(cmd.Context(), client, remoteBase, idx.LocalPath())

		// compare both
		if !localOnly {
			Fmt("diff '%s' <--> '%s'...\n", remoteBase, remote)
			remoteDiff, err := local.DiffRemotes(cmd.Context(), client, remoteBase, remote)
			if err != nil {
				DieErr(err)
			}
			diff = diff.MergeWith(remoteDiff)
		}

		if len(diff) == 0 {
			Fmt("\nNo diff found.\n")
			return
		}

		t := table.NewWriter()
		t.SetStyle(table.StyleDouble)
		t.AppendHeader(table.Row{"source", "change", "path"})
		for _, d := range diff {
			var color text.Color
			switch d.Type {
			case "changed":
				color = text.FgGreen
			case "added":
				color = text.FgGreen
			case "removed":
				color = text.FgRed
			case "conflict":
				color = text.BgRed
			}
			r := table.Row{
				color.Sprint(d.Source),
				color.Sprint(d.Type),
				color.Sprint(d.Path),
			}
			t.AppendRow(r)
		}
		Fmt("\n%s\n\n", t.Render())
	},
}

var localListCmd = &cobra.Command{
	Use:   "list",
	Short: "find and list directories that are synced with lakeFS",
	Run: func(cmd *cobra.Command, args []string) {
		path := "."
		if len(args) > 0 {
			path = args[0]
		}
		dirs, err := local.FindIndices(path)
		if err != nil {
			DieErr(err)
		}

		t := table.NewWriter()
		t.SetStyle(table.StyleDouble)
		t.AppendHeader(table.Row{"directory", "remote URI", "last synced HEAD"})
		for _, d := range dirs {
			idx, err := local.ReadIndex(d)
			if err != nil {
				DieErr(err)
			}
			remote, err := idx.GetCurrentURI()
			if err != nil {
				DieErr(err)
			}
			t.AppendRow(table.Row{d, remote, idx.Head})
		}
		Fmt("\n%s\n\n", t.Render())
	},
}

var localAddCmd = &cobra.Command{
	Use:   "add [directory] <lakeFS path URI>",
	Short: "set a local directory to sync with a lakeFS ref and path",
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		var remote *uri.URI
		path := "."
		if len(args) == 2 {
			path = args[0]
			remote = MustParsePathURI("remote ref", args[1])
		} else {
			remote = MustParsePathURI("remote ref", args[0])
		}
		localPath, err := filepath.Abs(path)
		if err != nil {
			DieErr(err)
		}
		// dereference
		head, err := local.DereferenceURI(cmd.Context(), getClient(), remote)
		if err != nil {
			DieErr(err)
		}
		err = local.WriteIndex(localPath, remote, head)
		if err != nil {
			DieErr(err)
		}
		err = local.GitIgnore(localPath)
		if err != nil && !errors.Is(err, local.ErrNotARepository) {
			DieErr(err)
		} else if err == nil {
			Fmt("location added to .gitignore\n")
		}
	},
}

var localCloneCmd = &cobra.Command{
	Use:   "clone <lakeFS path URI> <directory>",
	Short: "create a local copy of the given lakeFS path",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		remote := MustParsePathURI("uri", args[0])
		localPath, err := filepath.Abs(args[1])
		if err != nil {
			DieErr(err)
		}
		syncFlags := getLocalSyncFlags(cmd)

		err = local.CreateDirectoryTree(localPath)
		if err != nil {
			DieErr(err)
		}

		// dereference
		head, err := local.DereferenceURI(cmd.Context(), client, remote)
		if err != nil {
			DieErr(err)
		}

		stableRemote := local.WithRef(remote, head)

		files, err := local.ListRemote(cmd.Context(), client, stableRemote)
		if err != nil {
			DieErr(err)
		}
		// construct changes
		c := make(local.Changes, len(files))
		for i, f := range files {
			// remove the prefix from each path
			path := strings.TrimPrefix(f.Path, remote.GetPath())
			path = strings.TrimPrefix(path, local.PathSeparator)
			c[i] = &local.Change{
				Source: "remote",
				Path:   path,
				Type:   "added",
			}
		}

		// sync them
		s := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		err = s.Sync(localPath, stableRemote, c)
		if err != nil {
			DieErr(err)
		}
		err = local.WriteIndex(localPath, remote, head)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("synced %d changes!\n", len(c))
		err = local.GitIgnore(localPath)
		if err != nil && !errors.Is(err, local.ErrNotARepository) {
			DieErr(err)
		} else if err == nil {
			Fmt("location added to .gitignore\n")
		}
	},
}

func withParallelismFlag(cmd *cobra.Command) {
	cmd.Flags().IntP("parallelism", "p", defaultSyncParallelism,
		"max concurrent operations to perform")
}

func withPresignFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("presign", defaultLocalSyncPresign,
		"use pre-signed URLs when downloading/uploading data (recommended)")
}

func withLocalSyncFlags(cmd *cobra.Command) {
	withParallelismFlag(cmd)
	withPresignFlag(cmd)
}

type syncFlags struct {
	parallelism int
	presign     bool
}

func getLocalSyncFlags(cmd *cobra.Command) syncFlags {
	parallelism := MustInt(cmd.Flags().GetInt("parallelism"))
	presign := MustBool(cmd.Flags().GetBool("presign"))
	return syncFlags{parallelism: parallelism, presign: presign}
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(localCmd)

	localCmd.AddCommand(localListCmd)

	localCmd.AddCommand(localCloneCmd)
	withLocalSyncFlags(localCloneCmd)

	localCmd.AddCommand(localStatusCmd)
	localStatusCmd.Flags().BoolP("local", "l", false, "don't compare against remote changes")

	localCmd.AddCommand(localCommitCmd)
	localCommitCmd.Flags().StringP("message", "m", "", "commit message to use")
	withLocalSyncFlags(localCommitCmd)
	localCommitCmd.Flags().StringSlice(metaFlagName, []string{}, "key value pair in the form of key=value")

	localCmd.AddCommand(localPullCmd)
	localPullCmd.Flags().BoolP("force", "f", false, "pull, resetting any uncommitted local change")
	withLocalSyncFlags(localPullCmd)

	localCmd.AddCommand(localAddCmd)
	withLocalSyncFlags(localAddCmd)

	localCmd.AddCommand(localCheckoutCmd)
	AssignAutoConfirmFlag(localCheckoutCmd.Flags())
	withLocalSyncFlags(localCheckoutCmd)
	localCheckoutCmd.Flags().BoolP("all", "a", false, "checkout all tracked directories under current location")
	localCheckoutCmd.Flags().StringP("ref", "r", "", "checkout the given source branch or reference")
}
