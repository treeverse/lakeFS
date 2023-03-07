package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/uri"
)

const fsStatTemplate = `Path: {{.Path | yellow }}
Modified Time: {{.Mtime|date}}
Size: {{ .SizeBytes }} bytes
Human Size: {{ .SizeBytes|human_bytes }}
Physical Address: {{ .PhysicalAddress }}
Checksum: {{ .Checksum }}
Content-Type: {{ .ContentType }}
`

const fsRecursiveTemplate = `Files: {{.Count}}
Total Size: {{.Bytes}} bytes
Human Total Size: {{.Bytes|human_bytes}}
`

type transportMethod int

const (
	transportMethodDefault = iota
	transportMethodDirect
	transportMethodPreSign
)

var ErrRequestFailed = errors.New("request failed")

var fsStatCmd = &cobra.Command{
	Use:               "stat <path uri>",
	Short:             "View object metadata",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		preSign := MustBool(cmd.Flags().GetBool("pre-sign"))
		client := getClient()
		resp, err := client.StatObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.StatObjectParams{
			Path:    *pathURI.Path,
			Presign: swag.Bool(preSign),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		Write(fsStatTemplate, resp.JSON200)
	},
}

const fsLsTemplate = `{{ range $val := . -}}
{{ $val.PathType|ljust 12 }}    {{ if eq $val.PathType "object" }}{{ $val.Mtime|date|ljust 29 }}    {{ $val.SizeBytes|human_bytes|ljust 12 }}{{ else }}                                            {{ end }}    {{ $val.Path|yellow }}
{{ end -}}
`

var fsListCmd = &cobra.Command{
	Use:               "ls <path uri>",
	Short:             "List entries under a given tree",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := MustParsePathURI("path", args[0])
		recursive, _ := cmd.Flags().GetBool("recursive")
		prefix := *pathURI.Path

		// prefix we need to trim in ls output (non-recursive)
		var trimPrefix string
		if idx := strings.LastIndex(prefix, PathDelimiter); idx != -1 {
			trimPrefix = prefix[:idx+1]
		}
		// delimiter used for listing
		var paramsDelimiter api.PaginationDelimiter
		if !recursive {
			paramsDelimiter = PathDelimiter
		}
		var from string
		for {
			pfx := api.PaginationPrefix(prefix)
			params := &api.ListObjectsParams{
				Prefix:    &pfx,
				After:     api.PaginationAfterPtr(from),
				Delimiter: &paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			results := resp.JSON200.Results
			// trim prefix if non-recursive
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
	Use:               "cat <path uri>",
	Short:             "Dump content of object to stdout",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		flagSet := cmd.Flags()
		direct := MustBool(flagSet.GetBool("direct"))
		preSignMode := MustBool(flagSet.GetBool("pre-sign"))
		transport := transportMethodFromFlags(direct, preSignMode)

		var err error
		var body io.ReadCloser
		client := getClient()
		if transport == transportMethodDirect {
			_, body, err = helpers.ClientDownload(cmd.Context(), client, pathURI.Repository, pathURI.Ref, *pathURI.Path)
		} else {
			preSign := swag.Bool(transport == transportMethodPreSign)
			var resp *http.Response
			resp, err = client.GetObject(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.GetObjectParams{
				Path:    *pathURI.Path,
				Presign: preSign,
			})
			DieOnHTTPError(resp)
			body = resp.Body
		}
		if err != nil {
			DieErr(err)
		}

		defer func() {
			if err := body.Close(); err != nil {
				DieErr(err)
			}
		}()
		_, err = io.Copy(os.Stdout, body)
		if err != nil {
			DieErr(err)
		}
	},
}

func upload(ctx context.Context, client api.ClientWithResponsesInterface, sourcePathname string, destURI *uri.URI, contentType string, method transportMethod) (*api.ObjectStats, error) {
	fp := OpenByPath(sourcePathname)
	defer func() {
		_ = fp.Close()
	}()
	objectPath := api.StringValue(destURI.Path)
	switch method {
	case transportMethodDefault:
		return uploadObject(ctx, client, destURI.Repository, destURI.Ref, objectPath, contentType, fp)
	case transportMethodDirect:
		return helpers.ClientUploadDirect(ctx, client, destURI.Repository, destURI.Ref, objectPath, nil, contentType, fp)
	case transportMethodPreSign:
		return helpers.ClientUploadPreSign(ctx, client, destURI.Repository, destURI.Ref, objectPath, nil, contentType, fp)
	default:
		panic("unsupported upload method")
	}
}

func uploadObject(ctx context.Context, client api.ClientWithResponsesInterface, repoID, branchID, objectPath, contentType string, fp io.Reader) (*api.ObjectStats, error) {
	pr, pw := io.Pipe()
	mpw := multipart.NewWriter(pw)
	mpContentType := mpw.FormDataContentType()
	go func() {
		defer func() {
			_ = pw.Close()
		}()
		filename := filepath.Base(objectPath)
		const fieldName = "content"
		var err error
		var cw io.Writer
		// when no content-type is specified we let 'CreateFromFile' add the part with the default content type.
		// otherwise, we add a part and set the content-type.
		if contentType != "" {
			h := make(textproto.MIMEHeader)
			contentDisposition := mime.FormatMediaType("form-data", map[string]string{"name": fieldName, "filename": filename})
			h.Set("Content-Disposition", contentDisposition)
			h.Set("Content-Type", contentType)
			cw, err = mpw.CreatePart(h)
		} else {
			cw, err = mpw.CreateFormFile(fieldName, filename)
		}
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
		Path: objectPath,
	}, mpContentType, pr)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusCreated {
		return nil, helpers.ResponseAsError(resp)
	}
	return resp.JSON201, nil
}

var fsUploadCmd = &cobra.Command{
	Use:               "upload <path uri>",
	Short:             "Upload a local file to the specified URI",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := MustParsePathURI("path", args[0])
		flagSet := cmd.Flags()
		source := MustString(flagSet.GetString("source"))
		recursive := MustBool(flagSet.GetBool("recursive"))
		direct := MustBool(flagSet.GetBool("direct"))
		preSignMode := MustBool(flagSet.GetBool("pre-sign"))
		contentType := MustString(flagSet.GetString("content-type"))

		ctx := cmd.Context()
		transport := transportMethodFromFlags(direct, preSignMode)
		if !recursive {
			stat, err := upload(ctx, client, source, pathURI, contentType, transport)
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
			p := filepath.ToSlash(filepath.Join(*uri.Path, relPath))
			uri.Path = &p
			stat, err := upload(ctx, client, path, &uri, contentType, transport)
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

func transportMethodFromFlags(direct bool, preSign bool) transportMethod {
	switch {
	case direct && preSign:
		Die("Can't enable both direct and pre-sign", 1)
	case direct:
		return transportMethodDirect
	case preSign:
		return transportMethodPreSign
	}
	return transportMethodDefault
}

var fsStageCmd = &cobra.Command{
	Use:               "stage <path uri>",
	Short:             "Stage a reference to an existing object, to be managed in lakeFS",
	Hidden:            true,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := MustParsePathURI("path", args[0])
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

		obj := api.ObjectStageCreation{
			Checksum:        checksum,
			Mtime:           mtime,
			PhysicalAddress: location,
			SizeBytes:       size,
			ContentType:     &contentType,
		}
		if metaErr == nil {
			metadata := api.ObjectUserMetadata{
				AdditionalProperties: meta,
			}
			obj.Metadata = &metadata
		}

		resp, err := client.StageObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.StageObjectParams{
			Path: *pathURI.Path,
		}, api.StageObjectJSONRequestBody(obj))
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		Write(fsStatTemplate, resp.JSON201)
	},
}

func deleteObjectWorker(ctx context.Context, client api.ClientWithResponsesInterface, paths <-chan *uri.URI, errors chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	for pathURI := range paths {
		err := deleteObject(ctx, client, pathURI)
		if err != nil {
			rmErr := fmt.Errorf("rm %s - %w", pathURI, err)
			errors <- rmErr
		}
	}
}

func deleteObject(ctx context.Context, client api.ClientWithResponsesInterface, pathURI *uri.URI) error {
	resp, err := client.DeleteObjectWithResponse(ctx, pathURI.Repository, pathURI.Ref, &api.DeleteObjectParams{
		Path: *pathURI.Path,
	})

	return RetrieveError(resp, err)
}

var fsRmCmd = &cobra.Command{
	Use:               "rm <path uri>",
	Short:             "Delete object",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		recursive, _ := cmd.Flags().GetBool("recursive")
		concurrency := MustInt(cmd.Flags().GetInt("concurrency"))
		pathURI := MustParsePathURI("path", args[0])
		client := getClient()
		if !recursive {
			// Delete single object in the main thread
			err := deleteObject(cmd.Context(), client, pathURI)
			if err != nil {
				DieErr(err)
			}
			return
		}
		// Recursive delete of (possibly) many objects.
		success := true
		var errorsWg sync.WaitGroup
		errors := make(chan error)
		errorsWg.Add(1)
		go func() {
			defer errorsWg.Done()
			for err := range errors {
				_, _ = fmt.Fprintln(os.Stderr, err)
				success = false
			}
		}()

		var deleteWg sync.WaitGroup
		paths := make(chan *uri.URI)
		deleteWg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go deleteObjectWorker(cmd.Context(), client, paths, errors, &deleteWg)
		}

		prefix := *pathURI.Path
		var paramsDelimiter api.PaginationDelimiter = ""
		var from string
		pfx := api.PaginationPrefix(prefix)
		for {
			params := &api.ListObjectsParams{
				Prefix:    &pfx,
				After:     api.PaginationAfterPtr(from),
				Delimiter: &paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			results := resp.JSON200.Results
			for i := range results {
				destURI := uri.URI{
					Repository: pathURI.Repository,
					Ref:        pathURI.Ref,
					Path:       &results[i].Path,
				}
				paths <- &destURI
			}

			pagination := resp.JSON200.Pagination
			if !pagination.HasMore {
				break
			}
			from = pagination.NextOffset
		}
		close(paths)
		deleteWg.Wait()
		close(errors)
		errorsWg.Wait()
		if !success {
			os.Exit(1)
		}
	},
}

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
		direct := MustBool(flagSet.GetBool("direct"))
		preSignMode := MustBool(flagSet.GetBool("pre-sign"))
		recursive := MustBool(flagSet.GetBool("recursive"))
		parallel := MustInt(flagSet.GetInt("parallel"))
		transport := transportMethodFromFlags(direct, preSignMode)

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
		downloadCh := make(chan string)
		sourcePath := api.StringValue(pathURI.Path)

		// recursive assume source is directory
		if recursive && len(sourcePath) > 0 && !strings.HasSuffix(sourcePath, uri.PathSeparator) {
			sourcePath += uri.PathSeparator
		}

		// prefix to remove from destination
		prefix := filepath.Dir(sourcePath)
		if prefix != "" {
			prefix += uri.PathSeparator
		}
		ctx := cmd.Context()
		// list objects to download
		go func() {
			defer close(downloadCh)
			if recursive {
				listRecursiveHelper(ctx, client, pathURI.Repository, pathURI.Ref, sourcePath, downloadCh)
			} else {
				downloadCh <- api.StringValue(pathURI.Path)
			}
		}()

		// download in parallel
		var (
			wg         sync.WaitGroup
			errCounter int64
		)
		wg.Add(parallel)
		for i := 0; i < parallel; i++ {
			go func() {
				defer wg.Done()
				for downloadPath := range downloadCh {
					src := uri.URI{
						Repository: pathURI.Repository,
						Ref:        pathURI.Ref,
						Path:       &downloadPath,
					}
					// destination is without the source URI
					dst := filepath.Join(dest, strings.TrimPrefix(downloadPath, prefix))
					err := downloadHelper(ctx, client, transport, src, dst)
					if err == nil {
						fmt.Printf("Successfully downloaded %s to %s\n", src.String(), dst)
					} else {
						_, _ = fmt.Fprintf(os.Stderr, "Download failed: %s to %s - %s\n", src.String(), dst, err)
						atomic.AddInt64(&errCounter, 1)
					}
				}
			}()
		}

		// wait for download to complete
		wg.Wait()
		// exit with the right status code
		if atomic.LoadInt64(&errCounter) > 0 {
			defer os.Exit(1)
		}
	},
}

func listRecursiveHelper(ctx context.Context, client *api.ClientWithResponses, repo, ref, prefix string, ch chan string) {
	pfx := api.PaginationPrefix(prefix)
	var from string
	for {
		params := &api.ListObjectsParams{
			Prefix: &pfx,
			After:  api.PaginationAfterPtr(from),
		}
		resp, err := client.ListObjectsWithResponse(ctx, repo, ref, params)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		for _, p := range resp.JSON200.Results {
			ch <- p.Path
		}
		pagination := resp.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		from = pagination.NextOffset
	}
}

func downloadHelper(ctx context.Context, client *api.ClientWithResponses, method transportMethod, src uri.URI, dst string) error {
	body, err := getObjectHelper(ctx, client, method, src)
	if err != nil {
		return err
	}
	defer func(body io.ReadCloser) {
		_ = body.Close()
	}(body)

	// create destination dir if needed
	dir := filepath.Dir(dst)
	_ = os.MkdirAll(dir, os.ModePerm)
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, body)
	return err
}

func getObjectHelper(ctx context.Context, client *api.ClientWithResponses, method transportMethod, src uri.URI) (io.ReadCloser, error) {
	if method == transportMethodDirect {
		// download directly from storage
		_, body, err := helpers.ClientDownload(ctx, client, src.Repository, src.Ref, *src.Path)
		if err != nil {
			return nil, err
		}
		return body, nil
	}

	// download from lakefs
	preSign := swag.Bool(method == transportMethodPreSign)
	resp, err := client.GetObject(ctx, src.Repository, src.Ref, &api.GetObjectParams{
		Path:    *src.Path,
		Presign: preSign,
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status)
	}
	return resp.Body, nil
}

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "View and manipulate objects",
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(fsCmd)
	fsCmd.AddCommand(fsStatCmd)
	fsCmd.AddCommand(fsListCmd)
	fsCmd.AddCommand(fsCatCmd)
	fsCmd.AddCommand(fsUploadCmd)
	fsCmd.AddCommand(fsStageCmd)
	fsCmd.AddCommand(fsRmCmd)
	fsCmd.AddCommand(fsDownloadCmd)

	fsStatCmd.Flags().Bool("pre-sign", false, "Request pre-sign for physical address")

	fsCatCmd.Flags().BoolP("direct", "d", false, "read directly from backing store (faster but requires more credentials)")
	fsCatCmd.Flags().Bool("pre-sign", false, "Use pre-sign link to access the data")

	fsUploadCmd.Flags().StringP("source", "s", "", "local file to upload, or \"-\" for stdin")
	fsUploadCmd.Flags().BoolP("recursive", "r", false, "recursively copy all files under local source")
	fsUploadCmd.Flags().BoolP("direct", "d", false, "write directly to backing store (faster but requires more credentials)")
	_ = fsUploadCmd.MarkFlagRequired("source")
	fsUploadCmd.Flags().StringP("content-type", "", "", "MIME type of contents")
	fsUploadCmd.Flags().Bool("pre-sign", false, "Use pre-sign link to access the data")

	fsStageCmd.Flags().String("location", "", "fully qualified storage location (i.e. \"s3://bucket/path/to/object\")")
	fsStageCmd.Flags().Int64("size", 0, "Object size in bytes")
	fsStageCmd.Flags().String("checksum", "", "Object MD5 checksum as a hexadecimal string")
	fsStageCmd.Flags().Int64("mtime", 0, "Object modified time (Unix Epoch in seconds). Defaults to current time")
	fsStageCmd.Flags().String("content-type", "", "MIME type of contents")
	fsStageCmd.Flags().StringSlice("meta", []string{}, "key value pairs in the form of key=value")
	_ = fsStageCmd.MarkFlagRequired("location")
	_ = fsStageCmd.MarkFlagRequired("size")
	_ = fsStageCmd.MarkFlagRequired("checksum")

	fsListCmd.Flags().Bool("recursive", false, "list all objects under the specified prefix")

	fsRmCmd.Flags().BoolP("recursive", "r", false, "recursively delete all objects under the specified path")
	fsRmCmd.Flags().IntP("concurrency", "C", 50, "max concurrent single delete operations to send to the lakeFS server")

	fsDownloadCmd.Flags().BoolP("direct", "d", false, "read directly from backing store (requires credentials)")
	fsDownloadCmd.Flags().BoolP("recursive", "r", false, "recursively all objects under path")
	fsDownloadCmd.Flags().IntP("parallel", "p", fsDownloadParallelDefault, "max concurrent downloads")
	fsDownloadCmd.Flags().Bool("pre-sign", false, "Request pre-sign link to access the data")
}
