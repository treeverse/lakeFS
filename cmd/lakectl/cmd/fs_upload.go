package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/schollz/progressbar/v3"
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
		client := getCommandClient(cmd)
		pathURI, _ := getSyncArgs(args, true, false)
		syncFlags := getSyncFlags(cmd, client, pathURI.Repository)
		source := Must(cmd.Flags().GetString("source"))
		contentType := Must(cmd.Flags().GetString("content-type"))
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		ctx := cmd.Context()

		ctx, stop := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
		defer stop()

		if !recursive || isFileOrStdin(source) {
			noProgress := getNoProgressMode(cmd)
			stat, err := upload(ctx, client, source, pathURI, contentType, syncFlags, noProgress)
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

		s := local.NewSyncManager(ctx, client, getHTTPClient(lakectlRetryPolicy), local.Config{
			SyncFlags:           syncFlags,
			SkipNonRegularFiles: cfg.Local.SkipNonRegularFiles,
			IncludePerm:         false,
			SymlinkSupport:      cfg.Local.SymlinkSupport,
		})
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

func isFileOrStdin(source string) bool {
	if source == StdinFileName {
		return true
	}
	stat, err := os.Stat(source)
	if err != nil {
		Die("failed to stat source", 1)
	}
	return !stat.IsDir()
}

func upload(ctx context.Context, client apigen.ClientWithResponsesInterface, sourcePathname string, destURI *uri.URI, contentType string, syncFlags local.SyncFlags, noProgress bool) (*apigen.ObjectStats, error) {
	fp := Must(OpenByPath(sourcePathname))
	defer func() {
		_ = fp.Close()
	}()

	// Get the size of the file for the progress bar
	size, err := fp.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if _, err := fp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	reader := newProgressReadSeeker(fp, size, !noProgress)
	defer func() {
		_ = reader.Finish()
	}()

	objectPath := apiutil.Value(destURI.Path)
	if syncFlags.Presign {
		return helpers.ClientUploadPreSign(ctx, client, getHTTPClient(lakectlRetryPolicy), destURI.Repository, destURI.Ref, objectPath, nil, contentType, reader, syncFlags.PresignMultipart)
	}
	return helpers.ClientUpload(ctx, client, destURI.Repository, destURI.Ref, objectPath, nil, contentType, reader)
}

// progressReadSeeker wraps an io.ReadSeeker and updates a progress bar on each Read.
// It also implements io.ReaderAt for presign multipart uploads.
type progressReadSeeker struct {
	io.ReadSeeker
	bar *progressbar.ProgressBar
}

func newProgressReadSeeker(rs io.ReadSeeker, size int64, visible bool) *progressReadSeeker {
	bar := progressbar.NewOptions64(size,
		progressbar.OptionSetDescription("Uploading"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(barWidth),
		progressbar.OptionThrottle(barThrottle),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			_, _ = fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionSetVisibility(visible),
	)
	return &progressReadSeeker{ReadSeeker: rs, bar: bar}
}

func (r *progressReadSeeker) Read(p []byte) (int, error) {
	n, err := r.ReadSeeker.Read(p)
	_ = r.bar.Add(n)
	return n, err
}

func (r *progressReadSeeker) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.ReadSeeker.(io.ReaderAt).ReadAt(p, off)
	_ = r.bar.Add(n)
	return n, err
}

func (r *progressReadSeeker) Finish() error {
	return r.bar.Finish()
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
