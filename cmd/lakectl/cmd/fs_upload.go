package cmd

import (
	"context"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

var fsUploadCmd = &cobra.Command{
	Use:               "upload <path uri>",
	Short:             "Upload a local file to the specified URI",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := MustParsePathURI("path", args[0])
		flagSet := cmd.Flags()
		source := Must(flagSet.GetString("source"))
		preSignMode := Must(flagSet.GetBool("pre-sign"))
		parallelism := Must(flagSet.GetInt("parallelism"))
		contentType := Must(flagSet.GetString("content-type"))

		if parallelism < 1 {
			DieFmt("Invalid value for parallelism (%d), minimum is 1.\n", parallelism)
		}

		ctx := cmd.Context()

		info, err := os.Stat(source)
		if err != nil {
			DieErr(err)
			return
		}

		if !info.IsDir() {
			if pathURI.GetPath() == "" {
				Die("target path is not a valid URI", 1)
			}
			stat, err := upload(ctx, client, source, pathURI, contentType, preSignMode)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}

		changes := localDiff(cmd.Context(), client, pathURI, source)
		// sync changes
		c := make(chan *local.Change, filesChanSize)
		go func() {
			defer close(c)
			for _, change := range changes {
				if change.Type == local.ChangeTypeRemoved {
					continue
				}
				c <- change
			}
		}()
		s := local.NewSyncManager(ctx, client, parallelism, preSignMode)
		fullPath, err := filepath.Abs(source)
		if err != nil {
			DieErr(err)
		}
		err = s.Sync(fullPath, pathURI, c)
		if err != nil {
			DieErr(err)
		}
		Write(localSummaryTemplate, struct {
			Operation string
			local.Tasks
		}{
			Operation: "Upload",
			Tasks:     s.Summary(),
		})
	},
}

func upload(ctx context.Context, client apigen.ClientWithResponsesInterface, sourcePathname string, destURI *uri.URI, contentType string, preSign bool) (*apigen.ObjectStats, error) {
	fp := Must(OpenByPath(sourcePathname))
	defer func() {
		_ = fp.Close()
	}()
	objectPath := apiutil.Value(destURI.Path)
	if preSign {
		return helpers.ClientUploadPreSign(ctx, client, destURI.Repository, destURI.Ref, objectPath, nil, contentType, fp)
	}
	return helpers.ClientUpload(ctx, client, destURI.Repository, destURI.Ref, objectPath, nil, contentType, fp)
}

//nolint:gochecknoinits
func init() {
	fsUploadCmd.Flags().StringP("source", "s", "", "local file to upload, or \"-\" for stdin")
	_ = fsUploadCmd.MarkFlagRequired("source")
	fsUploadCmd.Flags().StringP("content-type", "", "", "MIME type of contents")
	fsUploadCmd.Flags().Bool("pre-sign", false, "Use pre-sign link to access the data")
	withParallelismFlag(fsUploadCmd)

	fsCmd.AddCommand(fsUploadCmd)
}
