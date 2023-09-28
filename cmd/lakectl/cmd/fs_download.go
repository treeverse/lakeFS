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
		flagSet := cmd.Flags()
		parallelism := Must(flagSet.GetInt(localParallelismFlagName))
		preSignMode := Must(flagSet.GetBool(localPresignFlagName))

		if parallelism < 1 {
			DieFmt("Invalid value for parallel (%d), minimum is 1.\n", parallelism)
		}

		// optional destination directory
		if len(args) > 1 {
			dest = args[1]
		}

		ctx := cmd.Context()
		s := local.NewSyncManager(ctx, client, parallelism, preSignMode)
		remotePath := remote.GetPath()

		ch := make(chan *local.Change, filesChanSize)

		stat, err := client.StatObjectWithResponse(ctx, remote.Repository, remote.Ref, &apigen.StatObjectParams{
			Path: *remote.Path,
		})
		switch {
		case err != nil:
			DieErr(err)
		case stat.JSON200 != nil:
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
		default:
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
		err = s.Sync(dest, remote, ch)

		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	withSyncFlags(fsDownloadCmd)
	fsCmd.AddCommand(fsDownloadCmd)
}
