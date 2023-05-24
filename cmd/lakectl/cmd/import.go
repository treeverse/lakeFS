package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"syscall"
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
	Use:   "import --from <object store URI> --to <lakeFS path URI>",
	Short: "Import data from external source to a destination branch",
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()
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
		body := api.ImportStartJSONRequestBody{
			Commit: api.CommitCreation{
				Message: message,
			},
			Paths: []api.ImportLocation{
				{
					Destination: api.StringValue(toURI.Path),
					Path:        from,
					Type:        "common_prefix",
				},
			},
		}
		if metadata != nil {
			body.Commit.Metadata = &api.CommitCreation_Metadata{AdditionalProperties: metadata}
		}

		importResp, err := client.ImportStartWithResponse(ctx, toURI.Repository, toURI.Ref, body)
		DieOnErrorOrUnexpectedStatusCode(importResp, err, http.StatusAccepted)
		if importResp.JSON202 == nil {
			Die("Bad response from server", 1)
		}
		importID := importResp.JSON202.Id
		// Handle interrupts
		sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()

		const (
			statusPollInterval = 5 * time.Second
			maxUpdateFailures  = 5
		)
		var (
			statusResp     *api.ImportStatusResponse
			updateFailures int
			updatedAt      time.Time
		)
		ticker := time.NewTicker(statusPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sigCtx.Done():
				Fmt("\nCanceling import\n")
				resp, err := client.ImportCancelWithResponse(ctx, toURI.Repository, toURI.Ref, &api.ImportCancelParams{Id: importID})
				DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
				Die("Import Canceled", 1)
			case <-ticker.C:
				statusResp, err = client.ImportStatusWithResponse(ctx, toURI.Repository, toURI.Ref, &api.ImportStatusParams{Id: importID})
				DieOnErrorOrUnexpectedStatusCode(statusResp, err, http.StatusOK)
				status := statusResp.JSON200
				if status == nil {
					Die("Bad response from server", 1)
				}
				if status.Error != nil {
					DieFmt("Import failed: %s", status.Error.Message)
				}
				_ = bar.Set64(*status.IngestedObjects)
				if updatedAt == status.UpdateTime {
					updateFailures += 1
				}
				if updateFailures >= maxUpdateFailures {
					DieFmt("Import status did not update for %s - abandon", maxUpdateFailures*statusPollInterval)
				}
				updatedAt = status.UpdateTime
			}

			if statusResp.JSON200.Completed {
				break
			}
		}
		_ = bar.Clear()

		Write(importSummaryTemplate, struct {
			Objects     int64
			MetaRangeID string
			Branch      string
			Commit      *api.Commit
		}{
			Objects:     api.Int64Value(statusResp.JSON200.IngestedObjects),
			MetaRangeID: api.StringValue(statusResp.JSON200.MetarangeId),
			Branch:      toURI.Ref,
			Commit:      statusResp.JSON200.Commit,
		})
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

//nolint:gochecknoinits,gomnd
func init() {
	importCmd.Flags().String("from", "", "prefix to read from (e.g. \"s3://bucket/sub/path/\"). must not be in a storage namespace")
	_ = importCmd.MarkFlagRequired("from")
	importCmd.Flags().String("to", "", "lakeFS path to load objects into (e.g. \"lakefs://repo/branch/sub/path/\")")
	_ = importCmd.MarkFlagRequired("to")
	importCmd.Flags().Bool("merge", false, "merge imported branch into target branch")
	_ = importCmd.Flags().MarkDeprecated("merge", "import is done directly into target branch")
	importCmd.Flags().Bool("no-progress", false, "switch off the progress output")
	importCmd.Flags().StringP("message", "m", "Import objects", "commit message")
	importCmd.Flags().StringSlice("meta", []string{}, "key value pair in the form of key=value")
	rootCmd.AddCommand(importCmd)
}
