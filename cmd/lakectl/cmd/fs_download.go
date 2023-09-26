package cmd

import (
	"net/http"
	"path/filepath"
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

	fsDownloadParallelDefault = 6
)

var fsDownloadCmd = &cobra.Command{
	Use:   "download <path uri> [<destination path>]",
	Short: "Download object(s) from a given repository path",
	Args:  cobra.RangeArgs(fsDownloadCmdMinArgs, fsDownloadCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote, dest := getLocalArgs(args, true, false)
		flagSet := cmd.Flags()
		preSignMode := Must(flagSet.GetBool("pre-sign"))
		recursive := Must(flagSet.GetBool("recursive"))
		parallel := Must(flagSet.GetInt("parallel"))

		if parallel < 1 {
			DieFmt("Invalid value for parallel (%d), minimum is 1.\n", parallel)
		}

		// optional destination directory
		if len(args) > 1 {
			dest = args[1]
		}

		client := getClient()
		ctx := cmd.Context()
		s := local.NewSyncManager(ctx, client, parallel, preSignMode)
		remotePath := remote.GetPath()
		if recursive {
			// Dynamically construct changes
			ch := make(chan *local.Change, filesChanSize)
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

			err := s.Sync(dest, remote, ch)

			if err != nil {
				DieErr(err)
			}
		} else {
			objectPath, ObjectName := filepath.Split(remotePath)
			*remote.Path = objectPath

			err := s.Download(ctx, dest, remote, ObjectName)
			if err != nil {
				DieErr(err)
			}
		}
	},
}

//nolint:gochecknoinits
func init() {
	fsDownloadCmd.Flags().BoolP("recursive", "r", false, "recursively all objects under path")
	fsDownloadCmd.Flags().IntP("parallel", "p", fsDownloadParallelDefault, "max concurrent downloads")
	fsDownloadCmd.Flags().Bool("pre-sign", false, "Request pre-sign link to access the data")

	fsCmd.AddCommand(fsDownloadCmd)
}
