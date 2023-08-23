package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

type LocalOperation string

const (
	localDefaultSyncParallelism = 25
	localDefaultSyncPresign     = true
	localDefaultMinArgs         = 0
	localDefaultMaxArgs         = 1

	localPresignFlagName     = "pre-sign"
	localParallelismFlagName = "parallelism"
	localGitIgnoreFlagName   = "gitignore"
	localForceFlagName       = "force"

	commitOperation   LocalOperation = "commit"
	pullOperation     LocalOperation = "pull"
	checkoutOperation LocalOperation = "checkout"
	cloneOperation    LocalOperation = "clone"
)

const localSummaryTemplate = `
{{.Operation}} Summary:

{{ if and (eq .Downloaded 0) (eq .Removed 0) (eq .Uploaded 0)}}No changes{{else -}}
{{"Downloaded:" | printf|green}} {{.Downloaded|green}}
{{"Uploaded:" | printf|yellow}} {{.Uploaded|yellow}}
{{"Removed:" | printf|red}} {{.Removed|red}}
{{end}}
`

var localDefaultArgsRange = cobra.RangeArgs(localDefaultMinArgs, localDefaultMaxArgs)

func withParallelismFlag(cmd *cobra.Command) {
	cmd.Flags().IntP(localParallelismFlagName, "p", localDefaultSyncParallelism,
		"Max concurrent operations to perform")
}

func withPresignFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(localPresignFlagName, localDefaultSyncPresign,
		"Use pre-signed URLs when downloading/uploading data (recommended)")
}

func withLocalSyncFlags(cmd *cobra.Command) {
	withParallelismFlag(cmd)
	withPresignFlag(cmd)
}

func withGitIgnoreFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(localGitIgnoreFlagName, true,
		"Update .gitignore file when working in a git repository context")
}

func withForceFlag(cmd *cobra.Command, usage string) {
	cmd.Flags().Bool(localForceFlagName, false, usage)
}

type syncFlags struct {
	parallelism int
	presign     bool
}

func getLocalSyncFlags(cmd *cobra.Command, client *api.ClientWithResponses) syncFlags {
	presign := Must(cmd.Flags().GetBool(localPresignFlagName))
	presignFlag := cmd.Flags().Lookup(localPresignFlagName)
	if !presignFlag.Changed {
		resp, err := client.GetStorageConfigWithResponse(cmd.Context())
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		presign = resp.JSON200.PreSignSupport
	}

	parallelism := Must(cmd.Flags().GetInt(localParallelismFlagName))
	return syncFlags{parallelism: parallelism, presign: presign}
}

// getLocalArgs parses arguments to extract a remote URI and deduces the local path.
// If local path isn't provided and considerGitRoot is true, it uses the git repository root.
func getLocalArgs(args []string, requireRemote bool, considerGitRoot bool) (remote *uri.URI, localPath string) {
	idx := 0
	if requireRemote {
		remote = MustParsePathURI("path", args[0])
		idx += 1
	}

	if len(args) > idx {
		expanded := Must(homedir.Expand(args[idx]))
		localPath = Must(filepath.Abs(expanded))
		return
	}
	localPath = Must(filepath.Abs("."))
	if considerGitRoot {
		gitRoot, err := git.GetRepositoryPath(localPath)
		if err == nil {
			localPath = gitRoot
		} else if !(errors.Is(err, git.ErrNotARepository) || errors.Is(err, git.ErrNoGit)) { // allow support in environments with no git
			DieErr(err)
		}
	}

	return
}

func localDiff(ctx context.Context, client api.ClientWithResponsesInterface, remote *uri.URI, path string) local.Changes {
	fmt.Printf("\ndiff 'local://%s' <--> '%s'...\n", path, remote)
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

func localHandleSyncInterrupt(ctx context.Context, updateIndexFileFunc func(string, string) error, path string, operation string) context.Context {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	go func() {
		defer stop()
		<-ctx.Done()
		err := updateIndexFileFunc(path, operation)
		if err != nil {
			WriteTo("{{.Error|red}}\n", struct{ Error string }{Error: "Failed to write failed operation to index file."}, os.Stderr)
		}
		Die(`Operation was canceled, local data may be incomplete.
	Use "lakectl local checkout..." to sync with the remote.`, 1)
	}()
	return ctx
}

func dieOnInterruptedOperation(interruptedOperation LocalOperation, force bool) {
	if !force {
		switch interruptedOperation {
		case "commit":
			Die(`Latest commit operation was interrupted, data may be incomplete.
Use "lakectl local commit..." to commit your latest changes or "lakectl local pull... --force" to sync with the remote.`, 1)
		case "checkout":
			Die(`Latest checkout operation was interrupted, local data may be incomplete.
Use "lakectl local checkout..." to sync with the remote.`, 1)
		case "pull":
			Die(`Latest pull operation was interrupted, local data may be incomplete.
Use "lakectl local pull... --force" to sync with the remote.`, 1)
		case "clone":
			Die(`Latest clone operation was interrupted, local data may be incomplete.
Use "lakectl local checkout..." to sync with the remote or run "lakectl local clone..." with a different directory to sync with the remote.`, 1)
		}
	}
}

var localCmd = &cobra.Command{
	Use:   "local",
	Short: "Sync local directories with lakeFS paths",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(localCmd)
}
