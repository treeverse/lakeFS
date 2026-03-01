package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
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
			stat, err := moveObject(ctx, client, srcURI, destURI)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}

		// Recursive move (copy + delete)
		moved, errors := recursiveCopyMove(ctx, client, srcURI, destURI, parallelism, noProgress, true)
		Write(fsMoveSummaryTemplate, struct {
			Moved  int64
			Errors int64
		}{Moved: moved, Errors: errors})

		if errors > 0 {
			os.Exit(1)
		}
	},
}

func moveObject(ctx context.Context, client apigen.ClientWithResponsesInterface, srcURI, destURI *uri.URI) (*apigen.ObjectStats, error) {
	// First copy
	stat, err := copyObject(ctx, client, srcURI, destURI)
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

//nolint:gochecknoinits
func init() {
	withRecursiveFlag(fsMvCmd, "recursively move all objects under the specified path")
	withParallelismFlag(fsMvCmd)
	withNoProgress(fsMvCmd)
	fsCmd.AddCommand(fsMvCmd)
}
