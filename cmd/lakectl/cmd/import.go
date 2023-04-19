package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const importSummaryTemplate = `Import of {{ .Objects | yellow }} object(s) into "{{.Branch}}" completed.
MetaRange ID: {{.MetaRangeID|yellow}}
Commit ID: {{.Commit.Id|yellow}}
Message: {{.Commit.Message}}
Timestamp: {{.Commit.CreationDate|date}}
Parents: {{.Commit.Parents|join ", "}}
`

var importCmd = &cobra.Command{
	Use:   "import --from <object store URI> --to <lakeFS path URI> [--merge]",
	Short: "Import data from external source to an imported branch (with optional merge)",
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()
		merge := MustBool(flags.GetBool("merge"))
		noProgress := MustBool(flags.GetBool("no-progress"))
		from := MustString(flags.GetString("from"))
		to := MustString(flags.GetString("to"))
		toURI := MustParsePathURI("to", to)
		message := MustString(flags.GetString("message"))
		metadata, err := getKV(cmd, "meta")
		if err != nil {
			DieErr(err)
		}

		ctx := cmd.Context()
		client := getClient()
		verifySourceMatchConfiguredStorage(ctx, client, from)

		// verify target branch exists before we try to create and import into the associated imported branch
		if err, ok := branchExists(ctx, client, toURI.Repository, toURI.Ref); err != nil {
			DieErr(err)
		} else if !ok {
			DieFmt("Target branch '%s', does not exists!", toURI.Ref)
		}

		// setup progress bar - based on `progressbar.Default` defaults + control visibility
		bar := newImportProgressBar(!noProgress)
		var (
			sum               int
			continuationToken *string
			after             string
			ranges            = make([]api.RangeMetadata, 0)
			stagingToken      *string
		)
		for {
			rangeResp, err := client.IngestRangeWithResponse(ctx, toURI.Repository, api.IngestRangeJSONRequestBody{
				After:             after,
				ContinuationToken: continuationToken,
				FromSourceURI:     from,
				Prepend:           api.StringValue(toURI.Path),
			})
			DieOnErrorOrUnexpectedStatusCode(rangeResp, err, http.StatusCreated)
			if rangeResp.JSON201 == nil {
				Die("Bad response from server", 1)
			}
			if rangeResp.JSON201.Range != nil {
				rangeInfo := *rangeResp.JSON201.Range
				ranges = append(ranges, rangeInfo)
				sum += rangeInfo.Count
				_ = bar.Add(rangeInfo.Count)
			}

			continuationToken = rangeResp.JSON201.Pagination.ContinuationToken
			after = rangeResp.JSON201.Pagination.LastKey
			stagingToken = rangeResp.JSON201.Pagination.StagingToken
			if !rangeResp.JSON201.Pagination.HasMore {
				break
			}
		}
		_ = bar.Clear()

		// create metarange with all the ranges we created
		metaRangeResp, err := client.CreateMetaRangeWithResponse(ctx, toURI.Repository, api.CreateMetaRangeJSONRequestBody{
			Ranges: ranges,
		})
		DieOnErrorOrUnexpectedStatusCode(metaRangeResp, err, http.StatusCreated)
		if metaRangeResp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		importedBranchID := formatImportedBranchID(toURI.Ref)
		ensureBranchExists(ctx, client, toURI.Repository, importedBranchID, toURI.Ref)

		// commit metarange to the imported branch
		commitResp, err := client.CommitWithResponse(ctx, toURI.Repository, importedBranchID, &api.CommitParams{
			SourceMetarange: metaRangeResp.JSON201.Id,
		}, api.CommitJSONRequestBody{
			Message: message,
			Metadata: &api.CommitCreation_Metadata{
				AdditionalProperties: metadata,
			},
		})
		DieOnErrorOrUnexpectedStatusCode(commitResp, err, http.StatusCreated)
		if commitResp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		if stagingToken != nil && *stagingToken != "" {
			stageResp, err := client.UpdateBranchTokenWithResponse(ctx, toURI.Repository, importedBranchID, api.UpdateBranchTokenJSONRequestBody{
				StagingToken: *stagingToken,
			})
			DieOnErrorOrUnexpectedStatusCode(stageResp, err, http.StatusCreated)
			// Commit staged data (skipped files)
			commitResp, err = client.CommitWithResponse(ctx, toURI.Repository, importedBranchID, &api.CommitParams{}, api.CommitJSONRequestBody{
				Message: "Import commit for staged objects",
				Metadata: &api.CommitCreation_Metadata{
					AdditionalProperties: metadata,
				},
			})
			DieOnErrorOrUnexpectedStatusCode(commitResp, err, http.StatusCreated)
			if commitResp.JSON201 == nil {
				Die("Bad response from server", 1)
			}
		}

		Write(importSummaryTemplate, struct {
			Objects     int
			MetaRangeID string
			Branch      string
			Commit      *api.Commit
		}{
			Objects:     sum,
			MetaRangeID: api.StringValue(metaRangeResp.JSON201.Id),
			Branch:      importedBranchID,
			Commit:      commitResp.JSON201,
		})

		// merge to target branch if needed
		if merge {
			mergeImportedBranch(ctx, client, toURI.Repository, importedBranchID, toURI.Ref)
		}
	},
}

func newImportProgressBar(visible bool) *progressbar.ProgressBar {
	const (
		barSpinnerType = 14
		barWidth       = 10
		barThrottle    = 65 * time.Millisecond
	)
	bar := progressbar.NewOptions64(
		-1,
		progressbar.OptionSetDescription("Importing"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSetWidth(barWidth),
		progressbar.OptionThrottle(barThrottle),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("object"),
		progressbar.OptionOnCompletion(func() {
			_, _ = fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(barSpinnerType),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetVisibility(visible),
	)
	_ = bar.RenderBlank()
	return bar
}

func verifySourceMatchConfiguredStorage(ctx context.Context, client *api.ClientWithResponses, source string) {
	storageConfResp, err := client.GetStorageConfigWithResponse(ctx)
	DieOnErrorOrUnexpectedStatusCode(storageConfResp, err, http.StatusOK)
	storageConfig := storageConfResp.JSON200
	if storageConfig == nil {
		Die("Bad response from server", 1)
	}
	if storageConfig.BlockstoreNamespaceValidityRegex == "" {
		return
	}
	matched, err := regexp.MatchString(storageConfig.BlockstoreNamespaceValidityRegex, source)
	if err != nil {
		DieErr(err)
	}
	if !matched {
		DieFmt("import source '%s' doesn't match current configured storage '%s'", source, storageConfig.BlockstoreType)
	}
}

func mergeImportedBranch(ctx context.Context, client *api.ClientWithResponses, repository, fromBranch, toBranch string) {
	mergeResp, err := client.MergeIntoBranchWithResponse(ctx, repository, fromBranch, toBranch, api.MergeIntoBranchJSONRequestBody{})
	DieOnErrorOrUnexpectedStatusCode(mergeResp, err, http.StatusOK)
	if mergeResp.JSON200 == nil {
		Die("Bad response from server", 1)
	}
	Write(mergeCreateTemplate, struct {
		Merge  FromTo
		Result *api.MergeResult
	}{
		Merge: FromTo{
			FromRef: fromBranch,
			ToRef:   toBranch,
		},
		Result: mergeResp.JSON200,
	})
}

func branchExists(ctx context.Context, client *api.ClientWithResponses, repository string, branch string) (error, bool) {
	resp, err := client.GetBranchWithResponse(ctx, repository, branch)
	if err != nil {
		return err, false
	}
	if resp.JSON200 != nil {
		return nil, true
	}
	if resp.JSON404 != nil {
		return nil, false
	}
	return RetrieveError(resp, err), false
}

func ensureBranchExists(ctx context.Context, client *api.ClientWithResponses, repository, branch, sourceBranch string) {
	if err, ok := branchExists(ctx, client, repository, branch); err != nil {
		DieErr(err)
	} else if ok {
		return
	}
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repository, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: sourceBranch,
	})
	DieOnErrorOrUnexpectedStatusCode(createBranchResp, err, http.StatusCreated)
}

func formatImportedBranchID(branch string) string {
	return "_" + branch + "_imported"
}

//nolint:gochecknoinits,gomnd
func init() {
	importCmd.Flags().String("from", "", "prefix to read from (e.g. \"s3://bucket/sub/path/\"). must not be in a storage namespace")
	_ = importCmd.MarkFlagRequired("from")
	importCmd.Flags().String("to", "", "lakeFS path to load objects into (e.g. \"lakefs://repo/branch/sub/path/\")")
	_ = importCmd.MarkFlagRequired("to")
	importCmd.Flags().Bool("merge", false, "merge imported branch into target branch")
	importCmd.Flags().Bool("no-progress", false, "switch off the progress output")
	importCmd.Flags().StringP("message", "m", "Import objects", "commit message")
	importCmd.Flags().StringSlice("meta", []string{}, "key value pair in the form of key=value")
	rootCmd.AddCommand(importCmd)
}
