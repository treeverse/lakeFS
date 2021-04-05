package cmd

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const fsStatTemplate = `Path: {{.Path | yellow }}
Modified Time: {{.Mtime|date}}
Size: {{ .SizeBytes }} bytes
Human Size: {{ .SizeBytes|human_bytes }}
Physical Address: {{ .PhysicalAddress }}
Checksum: {{ .Checksum }}
Metadata: {{ if .UserMetadata }} {{- .UserMetadata.AdditionalProperties | map -}} {{- end}}
`

const fsRecursiveTemplate = `Files: {{.Count}}
Total Size: {{.Bytes}} bytes
Human Total Size: {{.Bytes|human_bytes}}
`

var fsStatCmd = &cobra.Command{
	Use:   "stat <path uri>",
	Short: "view object metadata",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		res, err := client.StatObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.StatObjectParams{
			Path: *pathURI.Path,
		})
		DieOnResponseError(res, err)

		stat := res.JSON200
		Write(fsStatTemplate, stat)
	},
}

const fsLsTemplate = `{{ range $val := . -}}
{{ $val.PathType|ljust 12 }}    {{ if eq $val.PathType "object" }}{{ $val.Mtime|date|ljust 29 }}    {{ $val.SizeBytes|human_bytes|ljust 12 }}{{ else }}                                            {{ end }}    {{ $val.Path|yellow }}
{{ end -}}
`

var fsListCmd = &cobra.Command{
	Use:   "ls <path uri>",
	Short: "list entries under a given tree",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		recursive, _ := cmd.Flags().GetBool("recursive")
		prefix := *pathURI.Path

		// prefix we need to trim in ls output (non recursive)
		const delimiter = "/"
		var trimPrefix string
		if idx := strings.LastIndex(prefix, delimiter); idx != -1 {
			trimPrefix = prefix[:idx]
		}
		// delimiter used for listing
		var paramsDelimiter *string
		if recursive {
			paramsDelimiter = api.StringPtr("")
		} else {
			paramsDelimiter = api.StringPtr(delimiter)
		}
		var from string
		for {
			params := &api.ListObjectsParams{
				Prefix:    &prefix,
				After:     api.PaginationAfterPtr(from),
				Delimiter: paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, params)
			DieOnResponseError(resp, err)

			results := resp.JSON200.Results
			// trim prefix if non recursive
			if !recursive {
				for i := range results {
					trimmed := strings.TrimPrefix(results[i].Path, trimPrefix)
					results[i].Path = trimmed
				}
			}

			Write(fsLsTemplate, results)
			pagination := resp.JSON200.Pagination
			if !pagination.HasMore {
				break
			}
			from = pagination.NextOffset
		}
	},
}

var fsCatCmd = &cobra.Command{
	Use:   "cat <path uri>",
	Short: "dump content of object to stdout",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		direct, _ := cmd.Flags().GetBool("direct")
		var contents []byte
		if direct {
			_, body, err := helpers.ClientDownload(cmd.Context(), client, pathURI.Repository, pathURI.Ref, *pathURI.Path)
			if err != nil {
				DieErr(err)
			}
			defer body.Close()
			contents, err = ioutil.ReadAll(body)
			if err != nil {
				DieErr(err)
			}
		} else {
			resp, err := client.GetObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.GetObjectParams{
				Path: *pathURI.Path,
			})
			DieOnResponseError(resp, err)
			contents = resp.Body
		}
		Fmt("%s\n", string(contents))
	},
}

func upload(ctx context.Context, client api.ClientWithResponsesInterface, sourcePathname string, destURI *uri.URI, userMetadata map[string]string, direct bool) (*api.ObjectStats, error) {
	fp := OpenByPath(sourcePathname)
	defer func() {
		_ = fp.Close()
	}()
	if direct {
		return helpers.ClientUpload(ctx, client, destURI.Repository, destURI.Ref, *destURI.Path, userMetadata, fp)
	}
	if len(userMetadata) > 0 {
		Write(`{{ "WARNING: user metadata dropped on upload through server" | yellow }}`, nil)
	}
	return uploadObject(ctx, client, destURI.Repository, destURI.Ref, *destURI.Path, fp)
}

func uploadObject(ctx context.Context, client api.ClientWithResponsesInterface, repoID, branchID, filePath string, fp io.Reader) (*api.ObjectStats, error) {
	pr, pw := io.Pipe()
	mpw := multipart.NewWriter(pw)
	contentType := mpw.FormDataContentType()
	go func() {
		defer func() {
			_ = pw.Close()
		}()
		cw, err := mpw.CreateFormFile("content", path.Base(filePath))
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(cw, fp); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = mpw.Close()
	}()

	resp, err := client.UploadObjectWithBodyWithResponse(ctx, repoID, branchID, &api.UploadObjectParams{
		Path: filePath,
	}, contentType, pr)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusCreated {
		return nil, helpers.ResponseAsError(resp)
	}
	return resp.JSON201, nil
}

var fsUploadCmd = &cobra.Command{
	Use:   "upload <path uri>",
	Short: "upload a local file to the specified URI",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		source, _ := cmd.Flags().GetString("source")
		recursive, _ := cmd.Flags().GetBool("recursive")
		direct, _ := cmd.Flags().GetBool("direct")
		metadata, _ := cmd.Flags().GetStringToString("metadata")
		if !recursive {
			stat, err := upload(cmd.Context(), client, source, pathURI, metadata, direct)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}
		// copy recursively
		var totals struct {
			Bytes int64
			Count int64
		}
		err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return fmt.Errorf("traverse %s: %w", path, err)
			}
			if info.IsDir() {
				return nil
			}
			relPath := strings.TrimPrefix(path, source)
			uri := *pathURI
			p := filepath.Join(*uri.Path, relPath)
			uri.Path = &p
			stat, err := upload(cmd.Context(), client, path, &uri, nil, direct)
			if err != nil {
				return fmt.Errorf("upload %s: %w", path, err)
			}
			if stat.SizeBytes != nil {
				totals.Bytes += *stat.SizeBytes
			}
			totals.Count++
			return nil
		})
		if err != nil {
			DieErr(err)
		}
		Write(fsRecursiveTemplate, totals)
	},
}

var fsStageCmd = &cobra.Command{
	Use:    "stage <path uri>",
	Short:  "stages a reference to an existing object, to be managed in lakeFS",
	Hidden: true,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		size, _ := cmd.Flags().GetInt64("size")
		location, _ := cmd.Flags().GetString("location")
		checksum, _ := cmd.Flags().GetString("checksum")
		meta, metaErr := getKV(cmd, "meta")

		obj := api.ObjectStageCreation{
			Checksum:        checksum,
			PhysicalAddress: location,
			SizeBytes:       size,
		}
		if metaErr == nil {
			metadata := api.ObjectStageCreation_Metadata{
				AdditionalProperties: meta,
			}
			obj.Metadata = &metadata
		}

		resp, err := client.StageObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.StageObjectParams{
			Path: *pathURI.Path,
		}, api.StageObjectJSONRequestBody(obj))
		DieOnResponseError(resp, err)

		Write(fsStatTemplate, resp.JSON201)
	},
}

var fsRmCmd = &cobra.Command{
	Use:   "rm <path uri>",
	Short: "delete object",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		resp, err := client.DeleteObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.DeleteObjectParams{
			Path: *pathURI.Path,
		})
		DieOnResponseError(resp, err)
	},
}

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:    "fs",
	Short:  "view and manipulate objects",
	Hidden: true,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(fsCmd)
	fsCmd.AddCommand(fsStatCmd)
	fsCmd.AddCommand(fsListCmd)
	fsCmd.AddCommand(fsCatCmd)
	fsCmd.AddCommand(fsUploadCmd)
	fsCmd.AddCommand(fsStageCmd)
	fsCmd.AddCommand(fsRmCmd)

	fsCatCmd.Flags().BoolP("direct", "d", false, "read directly from backing store (faster but requires more credentials)")

	fsUploadCmd.Flags().StringP("source", "s", "", "local file to upload, or \"-\" for stdin")
	fsUploadCmd.Flags().BoolP("recursive", "r", false, "recursively copy all files under local source")
	fsUploadCmd.Flags().StringToStringP("metadata", "m", nil, "attach user metadata to uploaded file")
	fsUploadCmd.Flags().BoolP("direct", "d", false, "write directly to backing store (faster but requires more credentials)")
	_ = fsUploadCmd.MarkFlagRequired("source")

	fsStageCmd.Flags().String("location", "", "fully qualified storage location (i.e. \"s3://bucket/path/to/object\")")
	fsStageCmd.Flags().Int64("size", 0, "Object size in bytes")
	fsStageCmd.Flags().String("checksum", "", "Object MD5 checksum as a hexadecimal string")
	fsStageCmd.Flags().StringSlice("meta", []string{}, "key value pairs in the form of key=value")
	_ = fsStageCmd.MarkFlagRequired("location")
	_ = fsStageCmd.MarkFlagRequired("size")
	_ = fsStageCmd.MarkFlagRequired("checksum")

	fsListCmd.Flags().Bool("recursive", false, "list all objects under the specified prefix")
}
