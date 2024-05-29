package cmd

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

var fsUploadCmd = &cobra.Command{
	Use:               "upload <path URI>",
	Short:             "Upload a local file to the specified URI",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI, _ := getSyncArgs(args, true, false)
		syncFlags := getSyncFlags(cmd, client)
		source := Must(cmd.Flags().GetString("source"))
		contentType := Must(cmd.Flags().GetString("content-type"))
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		remotePath := pathURI.GetPath()
		ctx := cmd.Context()

		ctx, stop := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
		defer stop()

		if !recursive { // Assume source is a single file
			if strings.HasSuffix(remotePath, uri.PathSeparator) {
				Die("target path is not a valid URI", 1)
			}
			stat, err := upload(ctx, client, source, pathURI, contentType, syncFlags)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}

		// Try recursive upload
		if !strings.HasSuffix(source, string(filepath.Separator)) {
			source += string(filepath.Separator)
		}
		changes := localDiff(ctx, client, pathURI, source)
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
		s := local.NewSyncManager(ctx, client, getHTTPClient(), syncFlags)
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

func upload(ctx context.Context, client apigen.ClientWithResponsesInterface, sourcePathname string, destURI *uri.URI, contentType string, syncFlags local.SyncFlags) (*apigen.ObjectStats, error) {
	fp := Must(OpenByPath(sourcePathname))
	defer func() {
		_ = fp.Close()
	}()
	objectPath := apiutil.Value(destURI.Path)
	if syncFlags.Presign {
		return helpers.ClientUploadPreSign(ctx, client, getHTTPClient(), destURI.Repository, destURI.Ref, objectPath, nil, contentType, fp, syncFlags.PresignMultipart)
	}
	return helpers.ClientUpload(ctx, client, destURI.Repository, destURI.Ref, objectPath, nil, contentType, fp)
}

//nolint:gochecknoinits
func init() {
	fsUploadCmd.Flags().StringP("source", "s", "", "local file to upload, or \"-\" for stdin")
	_ = fsUploadCmd.MarkFlagRequired("source")
	fsUploadCmd.Flags().StringP("content-type", "", "", "MIME type of contents")
	withRecursiveFlag(fsUploadCmd, "recursively copy all files under local source")
	withSyncFlags(fsUploadCmd)

	fsCmd.AddCommand(fsUploadCmd)
}
