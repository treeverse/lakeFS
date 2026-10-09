package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/uri"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
)

var fsCpCmd = &cobra.Command{
	Use:   "cp <source URI> <destination URI>",
	Short: "Copy object",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		concurrency := Must(cmd.Flags().GetInt("concurrency"))
		sourceURI := MustParsePathURI("source", args[0])
		destinationURI := MustParsePathURI("destination", args[1])

		if sourceURI.Repository != destinationURI.Repository {
			Die("Can only copy files in the same repo", 1)
		}

		client := getClient()

		ctx := cmd.Context()

		if !recursive {
			err := copyObject(ctx, client, sourceURI, destinationURI)
			if err != nil {
				DieErr(err)
			}
			return
		}

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

		var copyWg sync.WaitGroup
		paths := make(chan string)
		copyWg.Add(concurrency)

		for i := 0; i < concurrency; i++ {
			go copyObjectWorker(ctx, client, sourceURI, destinationURI, paths, errors, &copyWg)
		}

		prefix := *sourceURI.Path
		var paramsDelimiter apigen.PaginationDelimiter = ""
		var from string
		pfx := apigen.PaginationPrefix(prefix)
		for {
			params := &apigen.ListObjectsParams{
				Prefix:    &pfx,
				After:     apiutil.Ptr(apigen.PaginationAfter(from)),
				Delimiter: &paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), sourceURI.Repository, sourceURI.Ref, params)
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
		copyWg.Wait()
		close(errors)
		errorsWg.Wait()
		if !success {
			os.Exit(1)
		}
	},
}

func copyObjectWorker(ctx context.Context, client apigen.ClientWithResponsesInterface, sourceURI, destinationURI *uri.URI, paths <-chan string, errors chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	srcPrefix := strings.TrimSuffix(*sourceURI.Path, "/")
	dstPrefix := strings.TrimSuffix(*destinationURI.Path, "/")

	for srcObjPath := range paths {
		rel := strings.TrimPrefix(srcObjPath, srcPrefix+"/")
		destObjPath := dstPrefix
		if rel != "" {
			destObjPath = path.Join(dstPrefix, rel)
		}

		resp, err := client.CopyObjectWithResponse(ctx, sourceURI.Repository, sourceURI.Ref, &apigen.CopyObjectParams{
			DestPath: destObjPath,
		},
			apigen.CopyObjectJSONRequestBody{
				SrcPath: srcObjPath,
				SrcRef:  &sourceURI.Ref,
			},
		)
		if err = RetrieveError(resp, err); err != nil {
			errors <- fmt.Errorf("cp %q -> %q: %w", srcObjPath, destObjPath, err)
		}
	}
}

func copyObject(ctx context.Context, client apigen.ClientWithResponsesInterface, sourceURI, destinationURI *uri.URI) error {
	resp, err := client.CopyObjectWithResponse(ctx, sourceURI.Repository, sourceURI.Ref, &apigen.CopyObjectParams{
		DestPath: *destinationURI.Path,
	}, apigen.CopyObjectJSONRequestBody{
		SrcPath: *sourceURI.Path,
		SrcRef:  &sourceURI.Ref,
	})
	return RetrieveError(resp, err)
}

//nolint:gochecknoinits
func init() {
	const defaultConcurrency = 50
	withRecursiveFlag(fsCpCmd, "recursively copy all objects under the specified path")
	fsCpCmd.Flags().IntP("concurrency", "C", defaultConcurrency, "max concurrent single copy operations to send to the lakeFS server")

	fsCmd.AddCommand(fsCpCmd)
}
