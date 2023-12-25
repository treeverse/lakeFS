package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/uri"
)

const deleteChunkSize = 1000

var fsRmCmd = &cobra.Command{
	Use:               "rm <path URI>",
	Short:             "Delete object",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		concurrency := Must(cmd.Flags().GetInt("concurrency"))
		pathURI := MustParsePathURI("path URI", args[0])
		client := getClient()
		if !recursive {
			// Delete a single object in the main thread
			err := deleteObject(cmd.Context(), client, pathURI)
			if err != nil {
				DieErr(err)
			}
			return
		}
		// Recursive delete of (possibly) many objects.
		success := true
		var errorsWg sync.WaitGroup
		errors := make(chan error)
		errorsWg.Add(1)
		go func() {
			defer errorsWg.Done()
			for err := range errors {
				_, _ = fmt.Fprintln(os.Stderr, err)
				success = false
			}
		}()

		var deleteWg sync.WaitGroup
		paths := make(chan string)
		deleteWg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go deleteObjectWorker(cmd.Context(), client, pathURI.Repository, pathURI.Ref, paths, errors, &deleteWg)
		}

		prefix := *pathURI.Path
		var paramsDelimiter apigen.PaginationDelimiter = ""
		var from string
		pfx := apigen.PaginationPrefix(prefix)
		for {
			params := &apigen.ListObjectsParams{
				Prefix:    &pfx,
				After:     apiutil.Ptr(apigen.PaginationAfter(from)),
				Delimiter: &paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			results := resp.JSON200.Results
			for i := range results {
				paths <- results[i].Path
			}

			pagination := resp.JSON200.Pagination
			if !pagination.HasMore {
				break
			}
			from = pagination.NextOffset
		}
		close(paths)
		deleteWg.Wait()
		close(errors)
		errorsWg.Wait()
		if !success {
			os.Exit(1)
		}
	},
}

func deleteObjectWorker(ctx context.Context, client apigen.ClientWithResponsesInterface, repository, branch string, paths <-chan string, errors chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	objs := make([]string, 0, deleteChunkSize)
	for objPath := range paths {
		objs = append(objs, objPath)
		if len(objs) >= deleteChunkSize {
			resp, err := client.DeleteObjectsWithResponse(ctx, repository, branch, &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{
				Paths: objs,
			})
			err = RetrieveError(resp, err)
			if err != nil {
				rmErr := fmt.Errorf("rm objects - %w", err)
				errors <- rmErr
			}
			clear(objs)
		}
	}
	if len(objs) > 0 {
		resp, err := client.DeleteObjectsWithResponse(ctx, repository, branch, &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{
			Paths: objs,
		})
		err = RetrieveError(resp, err)
		if err != nil {
			rmErr := fmt.Errorf("rm objects - %w", err)
			errors <- rmErr
		}
	}
}

func deleteObject(ctx context.Context, client apigen.ClientWithResponsesInterface, pathURI *uri.URI) error {
	resp, err := client.DeleteObjectWithResponse(ctx, pathURI.Repository, pathURI.Ref, &apigen.DeleteObjectParams{
		Path: *pathURI.Path,
	})
	return RetrieveError(resp, err)
}

//nolint:gochecknoinits
func init() {
	const defaultConcurrency = 50
	withRecursiveFlag(fsRmCmd, "recursively delete all objects under the specified path")
	fsRmCmd.Flags().IntP("concurrency", "C", defaultConcurrency, "max concurrent single delete operations to send to the lakeFS server")

	fsCmd.AddCommand(fsRmCmd)
}
