package cmd

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	localCloneMinArgs = 1
	localCloneMaxArgs = 2
	filesChanSize     = 1000
)

var localCloneCmd = &cobra.Command{
	Use:   "clone <path uri> [directory]",
	Short: "init local directory and download objects from lakeFS path",
	Args:  cobra.RangeArgs(localCloneMinArgs, localCloneMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote := MustParsePathURI("path", args[0])
		dir := "."
		if len(args) == localCloneMaxArgs {
			dir = args[1]
		}
		force := Must(cmd.Flags().GetBool("force"))
		syncFlags := getLocalSyncFlags(cmd)
		localPath, err := filepath.Abs(dir)
		if err != nil {
			DieErr(err)
		}
		idx := localInit(cmd.Context(), localPath, remote, force)
		stableRemote := uri.WithRef(remote, idx.AtHead)
		client := getClient()
		// Dynamically construct changes
		c := make(chan *local.Change, filesChanSize)

		go func() {
			hasMore := true
			var after string
			for hasMore {
				listResp, err := client.ListObjectsWithResponse(cmd.Context(), remote.Repository, remote.Ref, &api.ListObjectsParams{
					After:        (*api.PaginationAfter)(swag.String(after)),
					Prefix:       (*api.PaginationPrefix)(remote.Path),
					UserMetadata: swag.Bool(true),
				})
				if err != nil {
					DieErr(err)
				}
				if listResp.HTTPResponse.StatusCode != http.StatusOK {
					DieErr(fmt.Errorf("%w: HTTP %d", err, listResp.StatusCode()))
				}
				for _, o := range listResp.JSON200.Results {
					path := strings.TrimPrefix(o.Path, remote.GetPath())
					// skip directory markers
					if path == "" || (strings.HasSuffix(path, uri.PathSeparator) && swag.Int64Value(o.SizeBytes) == 0) {
						continue
					}
					path = strings.TrimPrefix(path, uri.PathSeparator)
					c <- &local.Change{
						Source: local.ChangeSourceRemote,
						Path:   path,
						Type:   local.ChangeTypeAdded,
					}
				}
				hasMore = listResp.JSON200.Pagination.HasMore
				after = listResp.JSON200.Pagination.NextOffset
			}
			close(c)
		}()

		s := local.NewSyncManager(cmd.Context(), client, syncFlags.parallelism, syncFlags.presign)
		err = s.Sync(localPath, stableRemote, c)

		if err != nil {
			DieErr(err)
		}
		fmt.Printf("Successfully cloned %s to %s.\nTotal objects downloaded: %d", remote, localPath, s.Summary().Download)
	},
}

//nolint:gochecknoinits
func init() {
	localCloneCmd.Flags().Bool("force", false, "Overwrites if directory already linked to a lakeFS path")
	withLocalSyncFlags(localCloneCmd)
	localCmd.AddCommand(localCloneCmd)
}
