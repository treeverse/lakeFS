package cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	fsDownloadCmdMinArgs = 1
	fsDownloadCmdMaxArgs = 2

	fsDownloadParallelDefault = 6
)

var fsDownloadCmd = &cobra.Command{
	Use:   "download <path uri> [<destination path>]",
	Short: "Download object(s) from a given repository path",
	Args:  cobra.RangeArgs(fsDownloadCmdMinArgs, fsDownloadCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		flagSet := cmd.Flags()
		preSignMode := Must(flagSet.GetBool("pre-sign"))
		recursive := Must(flagSet.GetBool("recursive"))
		parallel := Must(flagSet.GetInt("parallel"))

		if parallel < 1 {
			DieFmt("Invalid value for parallel (%d), minimum is 1.\n", parallel)
		}

		// optional destination directory
		var dest string
		if len(args) > 1 {
			dest = args[1]
		}

		// list the files
		client := getClient()
		sourcePath := apiutil.Value(pathURI.Path)

		// recursive assume the source is directory
		if recursive && len(sourcePath) > 0 && !strings.HasSuffix(sourcePath, uri.PathSeparator) {
			sourcePath += uri.PathSeparator
		}

		ctx := cmd.Context()
		s := local.NewSyncManager(ctx, client, parallel, preSignMode)
		err := s.Download(ctx, dest, pathURI, sourcePath)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	fsDownloadCmd.Flags().BoolP("recursive", "r", false, "recursively all objects under path")
	fsDownloadCmd.Flags().IntP("parallel", "p", fsDownloadParallelDefault, "max concurrent downloads")
	fsDownloadCmd.Flags().Bool("pre-sign", false, "Request pre-sign link to access the data")

	fsCmd.AddCommand(fsDownloadCmd)
}
