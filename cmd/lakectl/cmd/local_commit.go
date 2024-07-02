package cmd

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	asciiCharAfterSlash = "0"
)

func findConflicts(changes local.Changes) (conflicts []string) {
	for _, c := range changes {
		if c.Type == local.ChangeTypeConflict {
			conflicts = append(conflicts, c.Path)
		}
	}
	return
}

func hasExternalChange(ctx context.Context, client *apigen.ClientWithResponses, remote *uri.URI, idx *local.Index) bool {
	currentURI, err := idx.GetCurrentURI()
	if err != nil {
		DieErr(err)
	}

	// Get first uncommitted change. If there are no changes or it's outside the local prefix, we're done
	dirtyResp, err := client.DiffBranchWithResponse(ctx, remote.Repository, remote.Ref, &apigen.DiffBranchParams{
		Amount: apiutil.Ptr(apigen.PaginationAmount(1)),
	})
	DieOnErrorOrUnexpectedStatusCode(dirtyResp, err, http.StatusOK)

	if len(dirtyResp.JSON200.Results) == 0 {
		return false
	}
	if slices.ContainsFunc(dirtyResp.JSON200.Results, func(diff apigen.Diff) bool {
		return diff.PathType == "object" && !strings.HasPrefix(diff.Path, *currentURI.Path)
	}) {
		return true
	}

	// Get the first uncommitted change after the prefix. If it exists, we're also done
	// For example, if the prefix is "test-data/", the next item in lexicographic order will be "test-data0"
	// because "/" is ordinal 47 and "0" is ordinal 48
	nextPrefix := fmt.Sprintf("%s%s", filepath.Clean(*currentURI.Path), asciiCharAfterSlash)
	dirtyResp, err = client.DiffBranchWithResponse(ctx, remote.Repository, remote.Ref, &apigen.DiffBranchParams{
		Amount: apiutil.Ptr(apigen.PaginationAmount(1)),
		After:  apiutil.Ptr(apigen.PaginationAfter(nextPrefix)),
	})
	DieOnErrorOrUnexpectedStatusCode(dirtyResp, err, http.StatusOK)

	// The above gives us SeekGT. Since we need SeekGE, we do another stat for exact match
	statResp, err := client.StatObjectWithResponse(ctx, remote.Repository, remote.Ref, &apigen.StatObjectParams{
		Path: nextPrefix,
	})
	if err != nil {
		DieErr(err)
	}

	if len(dirtyResp.JSON200.Results) > 0 || statResp.StatusCode() == http.StatusOK {
		return true
	}

	return false
}

var localCommitCmd = &cobra.Command{
	Use:   "commit [directory]",
	Short: "Commit changes from local directory to the lakeFS branch it tracks.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		_, localPath := getSyncArgs(args, false, false)
		syncFlags := getSyncFlags(cmd, client)
		message, kvPairs := getCommitFlags(cmd)
		force := Must(cmd.Flags().GetBool(localForceFlagName))

		idx, err := local.ReadIndex(localPath)
		if err != nil {
			DieErr(err)
		}

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}

		if idx.ActiveOperation != "" {
			fmt.Printf("Latest 'local %s' operation was interrupted, running 'local commit' operation now might lead to data loss.\n", idx.ActiveOperation)
			confirmation, err := Confirm(cmd.Flags(), "Proceed")
			if err != nil || !confirmation {
				Die("command aborted", 1)
			}
		}

		fmt.Printf("\nGetting branch: %s\n", remote.Ref)
		resp, err := client.GetBranchWithResponse(cmd.Context(), remote.Repository, remote.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		// Diff local with current head
		baseRemote := remote.WithRef(idx.AtHead)
		changes := localDiff(cmd.Context(), client, baseRemote, idx.LocalPath())

		branchCommit := resp.JSON200.CommitId
		if branchCommit != idx.AtHead { // check for changes and conflicts with new head
			newRemote := remote.WithRef(branchCommit)
			fmt.Printf("\ndiff '%s' <--> '%s'...\n", newRemote, remote)
			d := make(chan apigen.Diff, maxDiffPageSize)
			var wg errgroup.Group
			wg.Go(func() error {
				return diff.StreamRepositoryDiffs(cmd.Context(), client, baseRemote, newRemote, swag.StringValue(remote.Path), d, false)
			})

			var remoteChanges local.Changes
			wg.Go(func() error {
				for dif := range d {
					remoteChanges = append(remoteChanges, &local.Change{
						Source: local.ChangeSourceRemote,
						Path:   strings.TrimPrefix(dif.Path, remote.GetPath()),
						Type:   local.ChangeTypeFromString(dif.Type),
					})
				}
				return nil
			})
			err = wg.Wait()
			if err != nil {
				DieErr(err)
			}

			changes = changes.MergeWith(remoteChanges, local.MergeStrategyNone)
			conflicts := findConflicts(changes)
			switch {
			case len(changes) == 0:
				fmt.Println("Local directory and remote branch are synced")
				return
			case len(conflicts) > 0:
				DieFmt("Conflicts found between local directory and remote in the following files:\n%s", strings.Join(conflicts, "\n"))
			}
		}

		if len(changes) == 0 {
			fmt.Printf("\nNo changes\n")
			return
		}

		// sync changes
		c := make(chan *local.Change, filesChanSize)
		go func() {
			defer close(c)
			for _, change := range changes {
				c <- change
			}
		}()

		hasChangesOutsideSyncedPrefix := hasExternalChange(cmd.Context(), client, remote, idx)
		if hasChangesOutsideSyncedPrefix && !force {
			DieFmt("Branch %s contains uncommitted changes outside of local path '%s'.\nTo proceed, use the --force flag.", remote.Ref, localPath)
		}

		sigCtx := localHandleSyncInterrupt(cmd.Context(), idx, string(commitOperation))
		s := local.NewSyncManager(sigCtx, client, getHTTPClient(), syncFlags, cfg.Experimental.Local.POSIXPerm.Enabled)
		err = s.Sync(idx.LocalPath(), remote, c)
		if err != nil {
			DieErr(err)
		}
		Write(localSummaryTemplate, struct {
			Operation string
			local.Tasks
		}{
			Operation: "Sync",
			Tasks:     s.Summary(),
		})
		fmt.Printf("Finished syncing changes. Perform commit on branch...\n")
		// add git context to kv pairs, if any
		if git.IsRepository(idx.LocalPath()) {
			gitRef, err := git.CurrentCommit(idx.LocalPath())
			if err == nil {
				md, err := git.MetadataFor(idx.LocalPath(), gitRef)
				if err == nil {
					for k, v := range md {
						kvPairs[k] = v
					}
				}
			}
		}

		// commit!
		response, err := client.CommitWithResponse(cmd.Context(), remote.Repository, remote.Ref, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: message,
			Metadata: &apigen.CommitCreation_Metadata{
				AdditionalProperties: kvPairs,
			},
		})
		DieOnErrorOrUnexpectedStatusCode(response, err, http.StatusCreated)
		commit := response.JSON201
		if commit == nil {
			Die("Bad response from server", 1)
		}

		branchURI := &uri.URI{
			Repository: remote.Repository,
			Ref:        remote.Ref,
		}

		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *apigen.Commit
		}{Branch: branchURI, Commit: commit})

		newHead := response.JSON201.Id
		_, err = local.WriteIndex(idx.LocalPath(), remote, newHead, "")
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	withForceFlag(localCommitCmd, "Commit changes even if remote branch includes uncommitted changes external to the synced path")
	withCommitFlags(localCommitCmd, false)
	withSyncFlags(localCommitCmd)
	localCmd.AddCommand(localCommitCmd)
}
