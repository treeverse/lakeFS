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

const fsCopySummaryTemplate = `
Copy Summary:
{{ if .Copied }}Copied: {{ .Copied }} object(s){{ end }}
{{ if .Errors }}Errors: {{ .Errors }} object(s){{ end }}
`

var fsCpCmd = &cobra.Command{
	Use:               "cp <source URI> <dest URI>",
	Short:             "Copy objects within a repository",
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
			// Single object copy
			stat, err := copyObject(ctx, client, srcURI, destURI, force)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}

		// Recursive copy
		copied, errors := recursiveCopy(ctx, client, srcURI, destURI, force, parallelism, noProgress)
		Write(fsCopySummaryTemplate, struct {
			Copied int64
			Errors int64
		}{Copied: copied, Errors: errors})

		if errors > 0 {
			os.Exit(1)
		}
	},
}

func copyObject(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI, force bool) (*apigen.ObjectStats, error) {
	resp, err := client.CopyObjectWithResponse(ctx,
		destURI.Repository,
		destURI.Ref,
		&apigen.CopyObjectParams{DestPath: *destURI.Path},
		apigen.CopyObjectJSONRequestBody{
			SrcPath: *srcURI.Path,
			SrcRef:  &srcURI.Ref,
			Force:   &force,
		})
	if err != nil {
		return nil, err
	}
	if resp.JSON201 == nil {
		return nil, RetrieveError(resp, nil)
	}
	return resp.JSON201, nil
}

func recursiveCopy(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI, force bool, parallelism int, noProgress bool) (copied, errors int64) {
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
	pw := newCopyProgressWriter(noProgress)
	go pw.Render()

	// Channel for objects to copy
	type copyTask struct {
		srcPath  string
		destPath string
	}
	tasks := make(chan copyTask, parallelism*2)

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
				atomic.AddInt64(&copied, 1)
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
			tasks <- copyTask{srcPath: obj.Path, destPath: destPath}
		}

		pagination := resp.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		after = pagination.NextOffset
	}

	close(tasks)
	wg.Wait()
	close(errorCh)
	errorsWg.Wait()

	// Wait for progress to finish render
	for pw.IsRenderInProgress() {
		if pw.LengthActive() == 0 {
			pw.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}

	return copied, errors
}

func newCopyProgressWriter(noProgress bool) progress.Writer {
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
	fsCpCmd.Flags().BoolP("force", "f", false, "overwrite existing objects at destination")
	withNoProgress(fsCpCmd)
	fsCmd.AddCommand(fsCpCmd)
}
