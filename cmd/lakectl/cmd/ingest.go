package cmd

import (
	"context"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/store"
	"github.com/treeverse/lakefs/pkg/api"
)

const ingestSummaryTemplate = `
Staged {{ .Objects | yellow }} external objects (total of {{ .Bytes | human_bytes | yellow }})
`

type stageRequest struct {
	ctx        context.Context
	repository string
	branch     string
	params     *api.StageObjectParams
	body       api.StageObjectJSONRequestBody
}

type stageResponse struct {
	resp  *api.StageObjectResponse
	error error
}

func stageWorker(wg *sync.WaitGroup, requests <-chan *stageRequest, responses chan<- *stageResponse) {
	defer wg.Done()
	client := getClient()
	for req := range requests {
		resp, err := client.StageObjectWithResponse(
			req.ctx, req.repository, req.branch, req.params, req.body)
		responses <- &stageResponse{
			resp:  resp,
			error: err,
		}
	}
}

var ingestCmd = &cobra.Command{
	Use:   "ingest --from <object store URI> --to <lakeFS path URI> [--dry-run]",
	Short: "Ingest objects from an external source into a lakeFS branch (without actually copying them)",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		verbose := MustBool(cmd.Flags().GetBool("verbose"))
		dryRun := MustBool(cmd.Flags().GetBool("dry-run"))
		from := MustString(cmd.Flags().GetString("from"))
		to := MustString(cmd.Flags().GetString("to"))
		concurrency := MustInt(cmd.Flags().GetInt("concurrency"))
		lakefsURI := MustParsePathURI("to", to)

		// initialize worker pool
		var wg sync.WaitGroup
		wg.Add(concurrency)
		requests := make(chan *stageRequest)
		responses := make(chan *stageResponse)
		for w := 0; w < concurrency; w++ {
			go stageWorker(&wg, requests, responses)
		}

		summary := struct {
			Objects int64
			Bytes   int64
		}{}

		// iterate entries and feed our pool
		go func() {
			err := store.Walk(ctx, from, func(e store.ObjectStoreEntry) error {
				if dryRun {
					Fmt("%s\n", e)
					return nil
				}
				key := e.RelativeKey
				if lakefsURI.Path != nil && *lakefsURI.Path != "" {
					path := *lakefsURI.Path
					if strings.HasSuffix(*lakefsURI.Path, "/") {
						key = path + key
					} else {
						key = path + "/" + key
					}
				}
				mtime := e.Mtime.Unix()
				requests <- &stageRequest{
					ctx:        ctx,
					repository: lakefsURI.Repository,
					branch:     lakefsURI.Ref,
					params: &api.StageObjectParams{
						Path: key,
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
				DieFmt("error walking object store: %v", err)
			}
			close(requests)  // we're done feeding work!
			wg.Wait()        // until all responses have been written
			close(responses) // so we're also done with responses
		}()

		for response := range responses {
			DieOnResponseError(response.resp, response.error)
			summary.Objects += 1
			summary.Bytes += api.Int64Value(response.resp.JSON201.SizeBytes)

			// update every 10k records
			if summary.Objects%10000 == 0 {
				Write("Staged {{ .Objects | green }} objects so far...\n", summary)
			}

			if verbose {
				Write("Staged "+fsStatTemplate+"\n", response.resp.JSON201)
			}
		}

		// print summary
		Write(ingestSummaryTemplate, summary)
	},
}

//nolint:gochecknoinits
func init() {
	ingestCmd.Flags().String("from", "", "prefix to read from (e.g. \"s3://bucket/sub/path/\")")
	_ = ingestCmd.MarkFlagRequired("from")
	ingestCmd.Flags().String("to", "", "lakeFS path to load objects into (e.g. \"lakefs://repo/branch/sub/path/\")")
	_ = ingestCmd.MarkFlagRequired("to")
	ingestCmd.Flags().Bool("dry-run", false, "only print the paths to be ingested")
	ingestCmd.Flags().BoolP("verbose", "v", false, "print stats for each individual object staged")
	ingestCmd.Flags().IntP("concurrency", "C", 64, "max concurrent API calls to make to the lakeFS server")
	rootCmd.AddCommand(ingestCmd)
}
