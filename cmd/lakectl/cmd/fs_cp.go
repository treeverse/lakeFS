package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	fsCopySummaryTemplate = `
Copy Summary:
{{ if .Copied }}Copied: {{ .Copied }} object(s){{ end }}
{{ if .Errors }}Errors: {{ .Errors }} object(s){{ end }}
`
	maxErrorMessages = 10 // Limit error output to avoid flooding
)

var fsCpCmd = &cobra.Command{
	Use:               "cp <source URI> <dest URI>",
	Short:             "Copy objects within a repository",
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		parallelism := Must(cmd.Flags().GetInt(parallelismFlagName))
		if parallelism < 1 {
			DieFmt("Invalid value for parallelism (%d), minimum is 1.", parallelism)
		}
		noProgress := getNoProgressMode(cmd)

		srcURI := MustParsePathURI("source URI", args[0])
		destURI := MustParsePathURI("dest URI", args[1])

		// Validate both URIs are in the same repository
		if srcURI.Repository != destURI.Repository {
			DieFmt("source and destination must be in the same repository")
		}

		// Validate destination is not under source for recursive operations
		if recursive {
			srcPath := *srcURI.Path
			destPath := *destURI.Path
			if !strings.HasSuffix(srcPath, uri.PathSeparator) {
				srcPath += uri.PathSeparator
			}
			if !strings.HasSuffix(destPath, uri.PathSeparator) {
				destPath += uri.PathSeparator
			}
			if strings.HasPrefix(destPath, srcPath) {
				DieFmt("destination path cannot be under source path in recursive mode")
			}
		}

		client := getClient()
		ctx := cmd.Context()

		if !recursive {
			// Single object copy
			stat, err := copyObject(ctx, client, srcURI, destURI)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}

		// Recursive copy
		copied, errors := recursiveCopyMove(ctx, client, srcURI, destURI, parallelism, noProgress, false)
		Write(fsCopySummaryTemplate, struct {
			Copied int64
			Errors int64
		}{Copied: copied, Errors: errors})

		if errors > 0 {
			os.Exit(1)
		}
	},
}

func copyObject(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI) (*apigen.ObjectStats, error) {
	resp, err := client.CopyObjectWithResponse(ctx,
		destURI.Repository,
		destURI.Ref,
		&apigen.CopyObjectParams{DestPath: *destURI.Path},
		apigen.CopyObjectJSONRequestBody{
			SrcPath: *srcURI.Path,
			SrcRef:  &srcURI.Ref,
		})
	if err != nil {
		return nil, err
	}
	if resp.JSON201 == nil {
		return nil, RetrieveError(resp, nil)
	}
	return resp.JSON201, nil
}

// copyMoveTask represents a single object to copy or move
type copyMoveTask struct {
	srcPath  string
	destPath string
}

// recursiveCopyMove handles recursive copy or move operations.
// When deleteSource is true, source objects are deleted after successful copy (move behavior).
func recursiveCopyMove(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI, parallelism int, noProgress bool, deleteSource bool) (successCount, errorCount int64) {
	srcPrefix := *srcURI.Path
	destPrefix := *destURI.Path

	// Ensure prefixes end with /
	if srcPrefix != "" && !strings.HasSuffix(srcPrefix, uri.PathSeparator) {
		srcPrefix += uri.PathSeparator
	}
	if destPrefix != "" && !strings.HasSuffix(destPrefix, uri.PathSeparator) {
		destPrefix += uri.PathSeparator
	}

	// Setup progress writer
	pw := newProgressWriter(noProgress)
	go pw.Render()

	tasks := make(chan copyMoveTask, parallelism*2)

	// Track successfully copied paths for deletion (move only)
	var copiedPaths []string
	var copiedMu sync.Mutex

	// Error handling
	errorCh := make(chan error, parallelism)
	var errorsWg sync.WaitGroup
	errorsWg.Go(func() {
		for err := range errorCh {
			errorCount++
			if errorCount <= maxErrorMessages {
				_, _ = fmt.Fprintln(os.Stderr, err)
			} else if errorCount == maxErrorMessages+1 {
				_, _ = fmt.Fprintln(os.Stderr, "(additional errors suppressed)")
			}
		}
	})

	// Worker pool
	var wg sync.WaitGroup
	wg.Add(parallelism)
	for range parallelism {
		go func() {
			defer wg.Done()
			for task := range tasks {
				tracker := &progress.Tracker{Message: "copy " + strings.TrimPrefix(task.srcPath, srcPrefix), Total: 1}
				pw.AppendTracker(tracker)
				tracker.Start()

				resp, err := client.CopyObjectWithResponse(ctx,
					destURI.Repository,
					destURI.Ref,
					&apigen.CopyObjectParams{DestPath: task.destPath},
					apigen.CopyObjectJSONRequestBody{
						SrcPath: task.srcPath,
						SrcRef:  &srcURI.Ref,
					})
				if err != nil {
					tracker.MarkAsErrored()
					errorCh <- fmt.Errorf("copy %s: %w", task.srcPath, err)
					continue
				}
				if resp.JSON201 == nil {
					tracker.MarkAsErrored()
					errorCh <- fmt.Errorf("copy %s: %w", task.srcPath, RetrieveError(resp, nil))
					continue
				}
				tracker.Increment(1)
				tracker.MarkAsDone()

				copiedMu.Lock()
				copiedPaths = append(copiedPaths, task.srcPath)
				copiedMu.Unlock()
			}
		}()
	}

	// List and enqueue objects
	var paramsDelimiter apigen.PaginationDelimiter = ""
	var after string
	pfx := apigen.PaginationPrefix(srcPrefix)
	for {
		params := &apigen.ListObjectsParams{
			Prefix:    &pfx,
			After:     apiutil.Ptr(apigen.PaginationAfter(after)),
			Delimiter: &paramsDelimiter,
		}
		resp, err := client.ListObjectsWithResponse(ctx, srcURI.Repository, srcURI.Ref, params)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		for _, obj := range resp.JSON200.Results {
			// Skip directory markers
			if strings.HasSuffix(obj.Path, uri.PathSeparator) {
				continue
			}
			// Transform path: replace source prefix with dest prefix
			relPath := strings.TrimPrefix(obj.Path, srcPrefix)
			destPath := destPrefix + relPath
			tasks <- copyMoveTask{srcPath: obj.Path, destPath: destPath}
		}

		pagination := resp.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		after = pagination.NextOffset
	}

	close(tasks)
	wg.Wait()

	// Wait for progress to finish render
	for pw.IsRenderInProgress() {
		if pw.LengthActive() == 0 {
			pw.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Delete source objects if this is a move operation
	if deleteSource && len(copiedPaths) > 0 {
		deleteErrors := deleteObjectsBatch(ctx, client, srcURI.Repository, srcURI.Ref, copiedPaths)
		for _, err := range deleteErrors {
			errorCh <- err
		}
	}

	close(errorCh)
	errorsWg.Wait()

	return int64(len(copiedPaths)), errorCount
}

func newProgressWriter(noProgress bool) progress.Writer {
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
	withRecursiveFlag(fsCpCmd, "recursively copy all objects under the specified path")
	withParallelismFlag(fsCpCmd)
	withNoProgress(fsCpCmd)
	fsCmd.AddCommand(fsCpCmd)
}
