package cmd

import (
	"context"

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
		recursive := Must(flagSet.GetBool("recursive"))
		preSignMode := Must(flagSet.GetBool("pre-sign"))
		contentType := Must(flagSet.GetString("content-type"))

		ctx := cmd.Context()
		if !recursive {
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

		s := local.NewSyncManager(ctx, client, 1, preSignMode)
		err := s.Upload(ctx, source, pathURI, *pathURI.Path)
		if err != nil {
			DieErr(err)
		}
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
	fsUploadCmd.Flags().BoolP("recursive", "r", false, "recursively copy all files under local source")
	_ = fsUploadCmd.MarkFlagRequired("source")
	fsUploadCmd.Flags().StringP("content-type", "", "", "MIME type of contents")
	fsUploadCmd.Flags().Bool("pre-sign", false, "Use pre-sign link to access the data")

	fsCmd.AddCommand(fsUploadCmd)
}
