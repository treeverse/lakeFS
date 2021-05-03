package cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/store"
	"github.com/treeverse/lakefs/pkg/api"
)

const ingestSummaryTemplate = `
Staged {{ .Objects | yellow }} external objects (total of {{ .Bytes | human_bytes | yellow }})
`

var ingestCmd = &cobra.Command{
	Use:   "ingest --from <object store URI> --to <lakeFS path URI> [--dry-run]",
	Short: "Ingest objects from an external source into a lakeFS branch (without actually copying them)",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		verbose := MustBool(cmd.Flags().GetBool("verbose"))
		dryRun := MustBool(cmd.Flags().GetBool("dry-run"))
		from := MustString(cmd.Flags().GetString("from"))
		to := MustString(cmd.Flags().GetString("to"))
		lakefsURI := MustParsePathURI("to", to)

		summary := struct {
			Objects int64
			Bytes   int64
		}{}
		client := getClient()
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
			resp, err := client.StageObjectWithResponse(ctx,
				lakefsURI.Repository,
				lakefsURI.Ref,
				&api.StageObjectParams{
					Path: key,
				},
				api.StageObjectJSONRequestBody{
					Checksum:        e.ETag,
					Mtime:           &mtime,
					PhysicalAddress: e.Address,
					SizeBytes:       e.Size,
				},
			)
			DieOnResponseError(resp, err)
			if verbose {
				Write("Staged "+fsStatTemplate+"\n", resp.JSON201)
			}
			summary.Objects += 1
			summary.Bytes += api.Int64Value(resp.JSON201.SizeBytes)
			return nil
		})
		if err != nil {
			DieFmt("error walking object store: %v", err)
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
	rootCmd.AddCommand(ingestCmd)
}
