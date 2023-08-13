package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
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
	Use:   "clone <path uri> [directory]",
	Short: "Clone a path from a lakeFS repository into a new directory.",
	Args:  cobra.RangeArgs(localCloneMinArgs, localCloneMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote, localPath := getLocalArgs(args, true, false)
		syncFlags := getLocalSyncFlags(cmd)

		empty, err := fileutil.IsDirEmpty(localPath)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				DieErr(err)
			}
		} else if !empty {
			DieFmt("directory '%s' exists and is not empty", localPath)
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		idx, err := localInit(cmd.Context(), localPath, remote, false)
		if err != nil {
			DieErr(err)
		}
		stableRemote := remote.WithRef(idx.AtHead)
		client := getClient()
		// Dynamically construct changes
		c := make(chan *local.Change, filesChanSize)
		go func() {
			defer close(c)
			hasMore := true
			var after string
			for hasMore {
				listResp, err := client.ListObjectsWithResponse(ctx, remote.Repository, stableRemote.Ref, &api.ListObjectsParams{
					After:        (*api.PaginationAfter)(swag.String(after)),
					Prefix:       (*api.PaginationPrefix)(remote.Path),
					UserMetadata: swag.Bool(true),
				})
				DieOnErrorOrUnexpectedStatusCode(listResp, err, http.StatusOK)

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
		}()
		s := local.NewSyncManager(ctx, client, syncFlags.parallelism, syncFlags.presign)
		err = s.Sync(localPath, stableRemote, c)
		if errors.Is(ctx.Err(), context.Canceled) {
			Die("Operation was canceled, local data may be incomplete", 1)
		}
		if err != nil {
			DieErr(err)
		}

		fmt.Printf("Successfully cloned %s to %s.\nTotal objects downloaded:\t%d\n", remote, localPath, s.Summary().Downloaded)
	},
}

//nolint:gochecknoinits
func init() {
	withLocalSyncFlags(localCloneCmd)
	localCmd.AddCommand(localCloneCmd)
}
