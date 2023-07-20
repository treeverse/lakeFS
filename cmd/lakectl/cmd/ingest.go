package cmd

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/ingest/store"
)

const ingestSummaryTemplate = `
Staged {{ .Objects | yellow }} external objects (total of {{ .Bytes | human_bytes | yellow }})
`

type stageRequest struct {
	repository string
	branch     string
	params     *api.StageObjectParams
	body       api.StageObjectJSONRequestBody
}

func stageWorker(ctx context.Context, client api.ClientWithResponsesInterface, wg *sync.WaitGroup, requests <-chan *stageRequest, responses chan<- *api.StageObjectResponse) {
	defer wg.Done()
	for req := range requests {
		resp, err := client.StageObjectWithResponse(ctx, req.repository, req.branch, req.params, req.body)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			utils.Die("Bad response from server", 1)
		}
		responses <- resp
	}
}

var ingestCmd = &cobra.Command{
	Deprecated: "use import command instead",
	Use:        "ingest --from <object store URI> --to <lakeFS path URI> [--dry-run]",
	Short:      "Ingest objects from an external source into a lakeFS branch (without actually copying them)",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		verbose := utils.MustBool(cmd.Flags().GetBool("verbose"))
		dryRun := utils.MustBool(cmd.Flags().GetBool("dry-run"))
		s3EndpointURL := utils.MustString(cmd.Flags().GetString("s3-endpoint-url"))
		from := utils.MustString(cmd.Flags().GetString("from"))
		to := utils.MustString(cmd.Flags().GetString("to"))
		concurrency := utils.MustInt(cmd.Flags().GetInt("concurrency"))
		lakefsURI := utils.MustParsePathURI("to", to)

		// initialize worker pool
		client := getClient()
		var wg sync.WaitGroup
		wg.Add(concurrency)
		requests := make(chan *stageRequest)
		responses := make(chan *api.StageObjectResponse)
		for w := 0; w < concurrency; w++ {
			go stageWorker(ctx, client, &wg, requests, responses)
		}

		summary := struct {
			Objects int64
			Bytes   int64
		}{}

		var path string
		if lakefsURI.Path != nil {
			path = *lakefsURI.Path
		}
		if len(path) > 0 && !strings.HasSuffix(path, utils.PathDelimiter) {
			path += utils.PathDelimiter // append a path delimiter (slash) if not passed by the user, and it's not an empty path in lakeFS
		}
		go func() {
			walker, err := store.NewFactory(nil).GetWalker(ctx, store.WalkerOptions{
				S3EndpointURL:  s3EndpointURL,
				StorageURI:     from,
				SkipOutOfOrder: false,
			})
			if err != nil {
				utils.DieFmt("error creating object-store walker: %v", err)
			}
			err = walker.Walk(ctx, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
				if dryRun {
					utils.Fmt("%s\n", e)
					return nil
				}
				// iterate entries and feed our pool
				key := e.RelativeKey
				mtime := e.Mtime.Unix()
				requests <- &stageRequest{
					repository: lakefsURI.Repository,
					branch:     lakefsURI.Ref,
					params: &api.StageObjectParams{
						Path: path + key,
					},
					body: api.StageObjectJSONRequestBody{
						Checksum:        e.ETag,
						Mtime:           &mtime,
						PhysicalAddress: e.Address,
						SizeBytes:       e.Size,
					},
				}
				return nil
			})
			if err != nil {
				utils.DieFmt("error walking object store: %v", err)
			}
			close(requests)  // we're done feeding work!
			wg.Wait()        // until all responses have been written
			close(responses) // so we're also done with responses
		}()

		elapsed := time.Now()
		for response := range responses {
			summary.Objects += 1
			summary.Bytes += api.Int64Value(response.JSON201.SizeBytes)

			if verbose {
				utils.Write("Staged "+fsStatTemplate+"\n", response.JSON201)
				continue
			}

			// If not verbose, at least update no more than once a second
			if time.Since(elapsed) > time.Second {
				utils.Write("Staged {{ .Objects | green }} objects so far...\r", summary)
				elapsed = time.Now()
			}

		}
		if !verbose {
			utils.Fmt("\n")
		}

		// print summary
		utils.Write(ingestSummaryTemplate, summary)
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	ingestCmd.Flags().String("from", "", "prefix to read from (e.g. \"s3://bucket/sub/path/\"). must not be in a storage namespace")
	_ = ingestCmd.MarkFlagRequired("from")
	ingestCmd.Flags().String("to", "", "lakeFS path to load objects into (e.g. \"lakefs://repo/branch/sub/path/\")")
	_ = ingestCmd.MarkFlagRequired("to")
	ingestCmd.Flags().Bool("dry-run", false, "only print the paths to be ingested")
	ingestCmd.Flags().String("s3-endpoint-url", "", "URL to access S3 storage API (by default, use regular AWS S3 endpoint")
	ingestCmd.Flags().BoolP("verbose", "v", false, "print stats for each individual object staged")
	ingestCmd.Flags().IntP("concurrency", "C", 64, "max concurrent API calls to make to the lakeFS server")
	rootCmd.AddCommand(ingestCmd)
}
