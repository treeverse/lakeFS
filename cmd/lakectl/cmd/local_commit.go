package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	localCommitAllowEmptyMessage = "allow-empty-message"
	localCommitMessageFlagName   = "message"
)

func findConflicts(changes local.Changes) (conflicts []string) {
	for _, c := range changes {
		if c.Type == local.ChangeTypeConflict {
			conflicts = append(conflicts, c.Path)
		}
	}
	return
}

var localCommitCmd = &cobra.Command{
	Use:   "commit [directory]",
	Short: "Commit changes from local directory to the lakeFS branch it tracks.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		localSendStats(cmd.Context(), client, "commit")
		_, localPath := getLocalArgs(args, false, false)
		syncFlags := getLocalSyncFlags(cmd, client)
		message := Must(cmd.Flags().GetString(localCommitMessageFlagName))
		allowEmptyMessage := Must(cmd.Flags().GetBool(localCommitAllowEmptyMessage))
		if message == "" && !allowEmptyMessage {
			DieFmt("Commit message empty! To commit with empty message pass --%s flag", localCommitAllowEmptyMessage)
		}
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
			d := make(chan api.Diff, maxDiffPageSize)
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
		sigCtx := localHandleSyncInterrupt(cmd.Context(), idx, string(commitOperation))
		s := local.NewSyncManager(sigCtx, client, syncFlags.parallelism, syncFlags.presign)
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
		// add kv pairs if any
		kvPairs, err := getKV(cmd, metaFlagName)
		if err != nil {
			DieErr(err)
		}
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
		response, err := client.CommitWithResponse(cmd.Context(), remote.Repository, remote.Ref, &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: message,
			Metadata: &api.CommitCreation_Metadata{
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
			Commit *api.Commit
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
	localCommitCmd.Flags().StringP(localCommitMessageFlagName, "m", "", "Commit message")
	localCommitCmd.Flags().Bool(localCommitAllowEmptyMessage, false, "Allow commit with empty message")
	localCommitCmd.MarkFlagsMutuallyExclusive(localCommitMessageFlagName, localCommitAllowEmptyMessage)
	localCommitCmd.Flags().StringSlice(metaFlagName, []string{}, "key value pair in the form of key=value")
	withLocalSyncFlags(localCommitCmd)
	localCmd.AddCommand(localCommitCmd)
}
