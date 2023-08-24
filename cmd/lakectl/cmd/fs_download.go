package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
		direct := Must(flagSet.GetBool("direct"))
		preSignMode := Must(flagSet.GetBool("pre-sign"))
		recursive := Must(flagSet.GetBool("recursive"))
		parallel := Must(flagSet.GetInt("parallel"))
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

		// recursive assume the source is directory
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

//nolint:gochecknoinits
func init() {
	fsDownloadCmd.Flags().BoolP("direct", "d", false, "read directly from backing store (requires credentials)")
	err := fsDownloadCmd.Flags().MarkDeprecated("direct", "use --pre-sign instead")
	if err != nil {
		DieErr(err)
	}
	fsDownloadCmd.Flags().BoolP("recursive", "r", false, "recursively all objects under path")
	fsDownloadCmd.Flags().IntP("parallel", "p", fsDownloadParallelDefault, "max concurrent downloads")
	fsDownloadCmd.Flags().Bool("pre-sign", false, "Request pre-sign link to access the data")

	fsCmd.AddCommand(fsDownloadCmd)
}
