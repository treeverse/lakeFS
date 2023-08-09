package cmd

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	localDefaultSyncParallelism = 25
	localDefaultSyncPresign     = true
	localDefaultMinArgs         = 0
	localDefaultMaxArgs         = 1
)

var localDefaultArgsRange = cobra.RangeArgs(localDefaultMinArgs, localDefaultMaxArgs)

func withParallelismFlag(cmd *cobra.Command) {
	cmd.Flags().IntP("parallelism", "p", localDefaultSyncParallelism,
		"Max concurrent operations to perform")
}

func withPresignFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("presign", localDefaultSyncPresign,
		"Use pre-signed URLs when downloading/uploading data (recommended)")
}

func withLocalSyncFlags(cmd *cobra.Command) {
	withParallelismFlag(cmd)
	withPresignFlag(cmd)
}

type syncFlags struct {
	parallelism int
	presign     bool
}

func getLocalSyncFlags(cmd *cobra.Command) syncFlags {
	parallelism := Must(cmd.Flags().GetInt("parallelism"))
	presign := Must(cmd.Flags().GetBool("presign"))
	return syncFlags{parallelism: parallelism, presign: presign}
}

func getLocalArgs(args []string, requireRemote bool) (remote *uri.URI, localPath string) {
	idx := 0
	if requireRemote {
		remote = MustParsePathURI("path", args[0])
		idx += 1
	}

	dir := "."
	if len(args) > idx {
		dir = args[idx]
	}
	localPath = Must(filepath.Abs(dir))
	return
}

func localDiff(ctx context.Context, client api.ClientWithResponsesInterface, remote *uri.URI, path string) local.Changes {
	fmt.Printf("diff 'local://%s' <--> '%s'...\n", path, remote)
	currentRemoteState := make(chan api.ObjectStats, maxDiffPageSize)
	var wg errgroup.Group
	wg.Go(func() error {
		return local.ListRemote(ctx, client, remote, currentRemoteState)
	})

	changes, err := local.DiffLocalWithHead(currentRemoteState, path)
	if err != nil {
		DieErr(err)
	}

	if err = wg.Wait(); err != nil {
		DieErr(err)
	}

	return changes
}

var localCmd = &cobra.Command{
	Use: "local",
	// TODO: Remove BETA when feature complete
	Short: "BETA: sync local directories with lakeFS paths",
}

//nolint:gochecknoinits
func init() {
	// TODO: Remove line when feature complete
	localCmd.Hidden = true
	rootCmd.AddCommand(localCmd)
}
