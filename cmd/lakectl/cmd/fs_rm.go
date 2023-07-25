package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

var fsRmCmd = &cobra.Command{
	Use:               "rm <path uri>",
	Short:             "Delete object",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		recursive, _ := cmd.Flags().GetBool("recursive")
		concurrency := Must(cmd.Flags().GetInt("concurrency"))
		pathURI := MustParsePathURI("path", args[0])
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
		paths := make(chan *uri.URI)
		deleteWg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go deleteObjectWorker(cmd.Context(), client, paths, errors, &deleteWg)
		}

		prefix := *pathURI.Path
		var paramsDelimiter api.PaginationDelimiter = ""
		var from string
		pfx := api.PaginationPrefix(prefix)
		for {
			params := &api.ListObjectsParams{
				Prefix:    &pfx,
				After:     api.PaginationAfterPtr(from),
				Delimiter: &paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			results := resp.JSON200.Results
			for i := range results {
				destURI := uri.URI{
					Repository: pathURI.Repository,
					Ref:        pathURI.Ref,
					Path:       &results[i].Path,
				}
				paths <- &destURI
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

func deleteObjectWorker(ctx context.Context, client api.ClientWithResponsesInterface, paths <-chan *uri.URI, errors chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	for pathURI := range paths {
		err := deleteObject(ctx, client, pathURI)
		if err != nil {
			rmErr := fmt.Errorf("rm %s - %w", pathURI, err)
			errors <- rmErr
		}
	}
}

func deleteObject(ctx context.Context, client api.ClientWithResponsesInterface, pathURI *uri.URI) error {
	resp, err := client.DeleteObjectWithResponse(ctx, pathURI.Repository, pathURI.Ref, &api.DeleteObjectParams{
		Path: *pathURI.Path,
	})

	return RetrieveError(resp, err)
}

//nolint:gochecknoinits
func init() {
	const defaultConcurrency = 50
	fsRmCmd.Flags().BoolP("recursive", "r", false, "recursively delete all objects under the specified path")
	fsRmCmd.Flags().IntP("concurrency", "C", defaultConcurrency, "max concurrent single delete operations to send to the lakeFS server")

	fsCmd.AddCommand(fsRmCmd)
}
