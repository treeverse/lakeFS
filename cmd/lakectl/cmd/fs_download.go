package cmd

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/uri"
)

type objectInfo struct {
	relPath    string // relative path within the repository, based on the source path
	objectStat *apigen.ObjectStats
}

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
		syncFlags := getSyncFlags(cmd, client, remote.Repository)
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		ctx := cmd.Context()
		downloadPartSize := Must(cmd.Flags().GetInt64(partSizeFlagName))
		if downloadPartSize < helpers.MinDownloadPartSize {
			DieFmt("part size must be at least %d bytes", helpers.MinDownloadPartSize)
		}

		downloader := helpers.NewDownloader(client, syncFlags.Presign)
		downloader.PartSize = downloadPartSize
		downloader.SymlinkSupport = cfg.Local.SymlinkSupport
		downloader.SkipNonRegularFiles = cfg.Local.SkipNonRegularFiles

		remotePath := remote.GetPath()
		if !recursive {
			// if dest is a directory, add the file name
			if s, _ := os.Stat(dest); s != nil && s.IsDir() {
				dest += string(filepath.Separator) // destination is in filesystem
			}
			if remotePath != "" && strings.HasSuffix(dest, string(filepath.Separator)) {
				dest += filepath.Base(filepath.FromSlash(remotePath))
			}

			err := downloader.Download(ctx, *remote, dest, nil)
			if err != nil {
				DieErr(err)
			}
			fmt.Printf("download: %s to %s\n", remote.String(), dest)
			return
		}

		// setup progress writer
		pw := newDownloadProgressWriter(syncFlags.NoProgress)
		// ProgressRender start render progress and return callback waiting for the progress to finish.
		go pw.Render()

		ch := make(chan objectInfo, filesChanSize)
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
					Presign:      swag.Bool(false), // faster listing without, we use stat object later if we need it
				})
				DieOnErrorOrUnexpectedStatusCode(listResp, err, http.StatusOK)
				if listResp.JSON200 == nil {
					Die("Bad response from server during list objects", 1)
				}
				if len(listResp.JSON200.Results) == 0 {
					DieFmt("No objects in path: %s", remote.String())
				}

				for _, listItem := range listResp.JSON200.Results {
					relPath := strings.TrimPrefix(listItem.Path, remotePath)
					relPath = strings.TrimPrefix(relPath, uri.PathSeparator)

					// skip directory markers
					if relPath == "" || strings.HasSuffix(relPath, uri.PathSeparator) {
						continue
					}
					ch <- objectInfo{relPath: relPath, objectStat: &listItem}
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
		var downloaded int64
		for range syncFlags.Parallelism {
			go func() {
				defer wg.Done()
				for objInfo := range ch {
					srcPath := remote.GetPath() + objInfo.relPath
					src := uri.URI{
						Repository: remote.Repository,
						Ref:        remote.Ref,
						Path:       &srcPath,
					}

					// progress tracker
					tracker := &progress.Tracker{Message: "download " + objInfo.relPath, Total: -1}
					pw.AppendTracker(tracker)
					tracker.Start()

					dest := filepath.Join(dest, objInfo.relPath)
					err := downloader.DownloadWithObjectInfo(ctx, src, dest, tracker, objInfo.objectStat)
					if err != nil {
						tracker.MarkAsErrored()
						DieErr(err)
					}
					tracker.MarkAsDone()
					atomic.AddInt64(&downloaded, 1)
				}
			}()
		}
		// wait for all downloads to finish
		wg.Wait()

		// wait for progress to finish render
		for pw.IsRenderInProgress() {
			// for manual-stop mode, stop when there are no more active trackers
			if pw.LengthActive() == 0 {
				pw.Stop()
			}
			const waitForRender = 100 * time.Millisecond
			time.Sleep(waitForRender)
		}

		Write(localSummaryTemplate, struct {
			Operation  string
			Downloaded int64
			Removed    int
			Uploaded   int
		}{
			Operation:  "Download",
			Downloaded: downloaded,
		})
	},
}

func newDownloadProgressWriter(noProgress bool) progress.Writer {
	pw := progress.NewWriter()
	pw.SetAutoStop(false)
	pw.SetSortBy(progress.SortByValue)
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.Style().Colors = progress.StyleColorsExample
	pw.Style().Options.PercentFormat = "%4.1f%%"
	if noProgress {
		pw.Style().Visibility = progress.StyleVisibility{}
	}
	return pw
}

//nolint:gochecknoinits
func init() {
	withSyncFlags(fsDownloadCmd)
	withRecursiveFlag(fsDownloadCmd, "recursively download all objects under path")
	fsDownloadCmd.Flags().Int64(partSizeFlagName, helpers.DefaultDownloadPartSize, "part size in bytes for multipart download")
	fsCmd.AddCommand(fsDownloadCmd)
}
