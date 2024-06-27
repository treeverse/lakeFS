package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	localCloneMinArgs = 1
	localCloneMaxArgs = 2
	filesChanSize     = 1000
)

var localCloneCmd = &cobra.Command{
	Use:   "clone <path URI> [directory]",
	Short: "Clone a path from a lakeFS repository into a new directory.",
	Args:  cobra.RangeArgs(localCloneMinArgs, localCloneMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		remote, localPath := getSyncArgs(args, true, false)
		syncFlags := getSyncFlags(cmd, client)
		updateIgnore := Must(cmd.Flags().GetBool(localGitIgnoreFlagName))
		empty, err := fileutil.IsDirEmpty(localPath)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				DieErr(err)
			}
		} else if !empty {
			DieFmt("directory '%s' exists and is not empty", localPath)
		}

		warnOnCaseInsensitiveDirectory(localPath)

		ctx := cmd.Context()
		head, err := localInit(ctx, localPath, remote, false, updateIgnore)
		if err != nil {
			DieErr(err)
		}
		stableRemote := remote.WithRef(head)
		// Dynamically construct changes
		ch := make(chan *local.Change, filesChanSize)
		go func() {
			defer close(ch)
			remotePath := remote.GetPath()
			var after string
			for {
				listResp, err := client.ListObjectsWithResponse(ctx, remote.Repository, stableRemote.Ref, &apigen.ListObjectsParams{
					After:        (*apigen.PaginationAfter)(swag.String(after)),
					Prefix:       (*apigen.PaginationPrefix)(remote.Path),
					UserMetadata: swag.Bool(true),
				})
				DieOnErrorOrUnexpectedStatusCode(listResp, err, http.StatusOK)
				if listResp.JSON200 == nil {
					Die("Bad response from server during list objects", 1)
				}

				for _, o := range listResp.JSON200.Results {
					relPath := strings.TrimPrefix(o.Path, remotePath)
					relPath = strings.TrimPrefix(relPath, uri.PathSeparator)

					// skip directory markers
					if relPath == "" || strings.HasSuffix(relPath, uri.PathSeparator) {
						continue
					}
					ch <- &local.Change{
						Source: local.ChangeSourceRemote,
						Path:   relPath,
						Type:   local.ChangeTypeAdded,
					}
				}
				if !listResp.JSON200.Pagination.HasMore {
					break
				}
				after = listResp.JSON200.Pagination.NextOffset
			}
		}()
		idx, err := local.ReadIndex(localPath)
		if err != nil {
			DieErr(err)
		}
		sigCtx := localHandleSyncInterrupt(ctx, idx, string(cloneOperation))
		s := local.NewSyncManager(sigCtx, client, getHTTPClient(), syncFlags, cfg.Experimental.Local.UnixPerm.Enabled)
		err = s.Sync(localPath, stableRemote, ch)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("\nSuccessfully cloned %s to %s.\n", remote, localPath)
		Write(localSummaryTemplate, struct {
			Operation string
			local.Tasks
		}{
			Operation: "Clone",
			Tasks:     s.Summary(),
		})
	},
}

//nolint:gochecknoinits
func init() {
	withGitIgnoreFlag(localCloneCmd)
	withSyncFlags(localCloneCmd)
	localCmd.AddCommand(localCloneCmd)
}
