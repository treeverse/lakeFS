package cmd

import (
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var fsStageCmd = &cobra.Command{
	Use:   "stage <path URI>",
	Short: "Link an external object with a path in a repository",
	Long: `Link an external object with a path in a repository, creating an uncommitted change.
The object location must be outside the repository's storage namespace`,
	Hidden:            true,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := MustParsePathURI("path URI", args[0])
		flags := cmd.Flags()
		size, _ := flags.GetInt64("size")
		mtimeSeconds, _ := flags.GetInt64("mtime")
		location, _ := flags.GetString("location")
		checksum, _ := flags.GetString("checksum")
		contentType, _ := flags.GetString("content-type")
		meta, metaErr := getKV(cmd, "meta")

		var mtime *int64
		if mtimeSeconds != 0 {
			mtime = &mtimeSeconds
		}
		repoResp, err := client.GetRepositoryWithResponse(cmd.Context(), pathURI.Repository)
		DieOnErrorOrUnexpectedStatusCode(repoResp, err, http.StatusOK)
		ns := strings.TrimSuffix(repoResp.JSON200.StorageNamespace, "/") + "/"
		if strings.HasPrefix(location, ns) {
			Die("The staged object must be outside the repository's storage namespace", 1)
		}
		obj := apigen.ObjectStageCreation{
			Checksum:        checksum,
			Mtime:           mtime,
			PhysicalAddress: location,
			SizeBytes:       size,
			ContentType:     &contentType,
		}
		if metaErr == nil {
			metadata := apigen.ObjectUserMetadata(meta)
			obj.Metadata = &metadata
		}

		resp, err := client.StageObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &apigen.StageObjectParams{
			Path: *pathURI.Path,
		}, apigen.StageObjectJSONRequestBody(obj))
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		Write(fsStatTemplate, resp.JSON201)
	},
}

//nolint:gochecknoinits
func init() {
	fsStageCmd.Flags().String("location", "", "fully qualified storage location (i.e. \"s3://bucket/path/to/object\")")
	fsStageCmd.Flags().Int64("size", 0, "Object size in bytes")
	fsStageCmd.Flags().String("checksum", "", "Object MD5 checksum as a hexadecimal string")
	fsStageCmd.Flags().Int64("mtime", 0, "Object modified time (Unix Epoch in seconds). Defaults to current time")
	fsStageCmd.Flags().String("content-type", "", "MIME type of contents")
	fsStageCmd.Flags().StringSlice("meta", []string{}, "key value pairs in the form of key=value")
	_ = fsStageCmd.MarkFlagRequired("location")
	_ = fsStageCmd.MarkFlagRequired("size")
	_ = fsStageCmd.MarkFlagRequired("checksum")

	fsCmd.AddCommand(fsStageCmd)
}
