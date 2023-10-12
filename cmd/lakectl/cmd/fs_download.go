package cmd

import (
	"net/http"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	fsDownloadCmdMinArgs = 1
	fsDownloadCmdMaxArgs = 2
)

var fsDownloadCmd = &cobra.Command{
	Use:   "download <path uri> [<destination path>]",
	Short: "Download object(s) from a given repository path",
	Args:  cobra.RangeArgs(fsDownloadCmdMinArgs, fsDownloadCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote, dest := getSyncArgs(args, true, false)
		client := getClient()
		syncFlags := getSyncFlags(cmd, client)
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		ctx := cmd.Context()
		remotePath := remote.GetPath()

		ch := make(chan *local.Change, filesChanSize)

		if !recursive {
			var objName string
			if strings.Contains(remotePath, uri.PathSeparator) {
				lastInd := strings.LastIndex(remotePath, uri.PathSeparator)
				remotePath, objName = remotePath[lastInd+len(uri.PathSeparator):], remotePath[:lastInd]
			} else {
				objName = ""
			}
			remote.Path = swag.String(objName)
			ch <- &local.Change{
				Source: local.ChangeSourceRemote,
				Path:   remotePath,
				Type:   local.ChangeTypeAdded,
			}
			close(ch)
		} else {
			if remotePath != "" && !strings.HasSuffix(remotePath, uri.PathSeparator) {
				*remote.Path += uri.PathSeparator
			}
			go func() {
				defer close(ch)
				var after string
				for {
					listResp, err := client.ListObjectsWithResponse(ctx, remote.Repository, remote.Ref, &apigen.ListObjectsParams{
						After:        (*apigen.PaginationAfter)(swag.String(after)),
						Prefix:       (*apigen.PaginationPrefix)(remote.Path),
						UserMetadata: swag.Bool(true),
					})
					DieOnErrorOrUnexpectedStatusCode(listResp, err, http.StatusOK)
					if listResp.JSON200 == nil {
						Die("Bad response from server during list objects", 1)
					}
					if len(listResp.JSON200.Results) == 0 {
						DieFmt("No objects in path: %s", remote.String())
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
		}

		s := local.NewSyncManager(ctx, client, syncFlags)
		err := s.Sync(dest, remote, ch)
		if err != nil {
			DieErr(err)
		}

		Write(localSummaryTemplate, struct {
			Operation string
			local.Tasks
		}{
			Operation: "Download",
			Tasks:     s.Summary(),
		})
	},
}

//nolint:gochecknoinits
func init() {
	withSyncFlags(fsDownloadCmd)
	withRecursiveFlag(fsDownloadCmd, "recursively download all objects under path")
	fsCmd.AddCommand(fsDownloadCmd)
}
