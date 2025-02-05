package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	fsDownloadCmdMinArgs = 1
	fsDownloadCmdMaxArgs = 2
	partSizeFlagName     = "part-size"
)

var fsDownloadCmd = &cobra.Command{
	Use:   "download <path URI> [<destination path>]",
	Short: "Download object(s) from a given repository path",
	Args:  cobra.RangeArgs(fsDownloadCmdMinArgs, fsDownloadCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		remote, dest := getSyncArgs(args, true, false)
		client := getClient()
		syncFlags := getSyncFlags(cmd, client)
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		ctx := cmd.Context()
		downloadPartSize := Must(cmd.Flags().GetInt64(partSizeFlagName))
		if downloadPartSize < helpers.MinDownloadPartSize {
			DieFmt("part size must be at least %d bytes", helpers.MinDownloadPartSize)
		}

		downloader := helpers.NewDownloader(client, syncFlags.Presign)
		downloader.PartSize = downloadPartSize

		if !recursive {
			src := uri.URI{
				Repository: remote.Repository,
				Ref:        remote.Ref,
				Path:       remote.Path,
			}
			singleObjectDownloadHelper(ctx, downloader, src, dest)
			return
		}

		ch := make(chan string, filesChanSize)
		remotePath := remote.GetPath()
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
					ch <- relPath
				}
				if !listResp.JSON200.Pagination.HasMore {
					break
				}
				after = listResp.JSON200.Pagination.NextOffset
			}
		}()

		// download files in parallel
		var wg sync.WaitGroup
		wg.Add(syncFlags.Parallelism)
		for i := 0; i < syncFlags.Parallelism; i++ {
			go func() {
				defer wg.Done()
				for relPath := range ch {
					src := uri.URI{
						Repository: remote.Repository,
						Ref:        remote.Ref,
						Path:       &relPath,
					}
					destPath := filepath.Join(dest, relPath)
					singleObjectDownloadHelper(ctx, downloader, src, destPath)
				}
			}()
		}
		wg.Wait()
	},
}

func singleObjectDownloadHelper(ctx context.Context, downloader *helpers.Downloader, src uri.URI, dest string) {
	// if dest is a directory, add the file name
	if s, _ := os.Stat(dest); s != nil && s.IsDir() {
		dest += uri.PathSeparator
	}
	remotePath := src.GetPath()
	if remotePath != "" && strings.HasSuffix(dest, uri.PathSeparator) {
		dest += filepath.Base(remotePath)
	}

	err := downloader.Download(ctx, src, dest)
	if err != nil {
		DieErr(err)
	}
	fmt.Printf("download: %s to %s\n", src.String(), dest)
}

//nolint:gochecknoinits
func init() {
	withSyncFlags(fsDownloadCmd)
	withRecursiveFlag(fsDownloadCmd, "recursively download all objects under path")
	fsDownloadCmd.Flags().Int64(partSizeFlagName, helpers.DefaultDownloadPartSize, "part size in bytes for multipart download")
	fsCmd.AddCommand(fsDownloadCmd)
}
