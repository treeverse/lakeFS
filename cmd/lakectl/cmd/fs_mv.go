package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/uri"
)

const fsMoveSummaryTemplate = `
Move Summary:
{{ if .Moved }}Moved: {{ .Moved }} object(s){{ end }}
{{ if .Errors }}Errors: {{ .Errors }} object(s){{ end }}
`

var fsMvCmd = &cobra.Command{
	Use:               "mv <source URI> <dest URI>",
	Short:             "Move objects within a repository",
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		force := Must(cmd.Flags().GetBool("force"))
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
			// Single object move
			stat, err := moveObject(ctx, client, srcURI, destURI, force)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}

		// Recursive move
		moved, errors := recursiveMove(ctx, client, srcURI, destURI, force, parallelism, noProgress)
		Write(fsMoveSummaryTemplate, struct {
			Moved  int64
			Errors int64
		}{Moved: moved, Errors: errors})

		if errors > 0 {
			os.Exit(1)
		}
	},
}

func moveObject(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI, force bool) (*apigen.ObjectStats, error) {
	// First copy
	stat, err := copyObject(ctx, client, srcURI, destURI, force)
	if err != nil {
		return nil, err
	}

	// Then delete source
	resp, err := client.DeleteObjectWithResponse(ctx, srcURI.Repository, srcURI.Ref, &apigen.DeleteObjectParams{
		Path: *srcURI.Path,
	})
	if err := RetrieveError(resp, err); err != nil {
		return nil, fmt.Errorf("delete source after copy: %w", err)
	}

	return stat, nil
}

func recursiveMove(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI, force bool, parallelism int, noProgress bool) (moved, errors int64) {
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
	pw := newMoveProgressWriter(noProgress)
	go pw.Render()

	// Channel for objects to move
	type moveTask struct {
		srcPath  string
		destPath string
	}
	tasks := make(chan moveTask, parallelism*2)

	// Track successfully copied paths for deletion
	var copiedPaths []string
	var copiedMu sync.Mutex

	// Error channel
	errorCh := make(chan error, parallelism)
	var errorsWg sync.WaitGroup
	errorsWg.Add(1)
	go func() {
		defer errorsWg.Done()
		for err := range errorCh {
			fmt.Fprintln(os.Stderr, "Error:", err)
			atomic.AddInt64(&errors, 1)
		}
	}()

	// Worker pool for copying
	var wg sync.WaitGroup
	wg.Add(parallelism)
	for range parallelism {
		go func() {
			defer wg.Done()
			for task := range tasks {
				tracker := &progress.Tracker{Message: "copy " + strings.TrimPrefix(task.srcPath, srcPrefix), Total: 1}
				pw.AppendTracker(tracker)
				tracker.Start()

				forceVal := force
				resp, err := client.CopyObjectWithResponse(ctx,
					destURI.Repository,
					destURI.Ref,
					&apigen.CopyObjectParams{DestPath: task.destPath},
					apigen.CopyObjectJSONRequestBody{
						SrcPath: task.srcPath,
						SrcRef:  &srcURI.Ref,
						Force:   &forceVal,
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

				// Track successful copy for later deletion
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
			tasks <- moveTask{srcPath: obj.Path, destPath: destPath}
		}

		pagination := resp.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		after = pagination.NextOffset
	}

	close(tasks)
	wg.Wait()

	// Wait for progress to finish render before deleting
	for pw.IsRenderInProgress() {
		if pw.LengthActive() == 0 {
			pw.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Now delete all successfully copied source objects
	if len(copiedPaths) > 0 {
		deleteErrors := deleteObjectsBatch(ctx, client, srcURI.Repository, srcURI.Ref, copiedPaths)
		for _, err := range deleteErrors {
			fmt.Fprintln(os.Stderr, "Error:", err)
			atomic.AddInt64(&errors, 1)
		}
	}

	close(errorCh)
	errorsWg.Wait()

	moved = int64(len(copiedPaths))
	return moved, errors
}

// deleteObjectsBatch deletes objects in batches of deleteChunkSize
func deleteObjectsBatch(ctx context.Context, client apigen.ClientWithResponsesInterface, repository, branch string, paths []string) []error {
	var deleteErrors []error

	for i := 0; i < len(paths); i += deleteChunkSize {
		end := i + deleteChunkSize
		if end > len(paths) {
			end = len(paths)
		}
		chunk := paths[i:end]

		resp, err := client.DeleteObjectsWithResponse(ctx, repository, branch, &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{
			Paths: chunk,
		})
		if err := RetrieveError(resp, err); err != nil {
			deleteErrors = append(deleteErrors, fmt.Errorf("delete batch: %w", err))
		}
	}

	return deleteErrors
}

func newMoveProgressWriter(noProgress bool) progress.Writer {
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
	withRecursiveFlag(fsMvCmd, "recursively move all objects under the specified path")
	withParallelismFlag(fsMvCmd)
	fsMvCmd.Flags().BoolP("force", "f", false, "overwrite existing objects at destination")
	withNoProgress(fsMvCmd)
	fsCmd.AddCommand(fsMvCmd)
}
