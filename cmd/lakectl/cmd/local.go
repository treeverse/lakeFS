package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

type LocalOperation string

const (
	localDefaultMinArgs = 0
	localDefaultMaxArgs = 1

	localGitIgnoreFlagName = "gitignore"
	localForceFlagName     = "force"

	commitOperation   LocalOperation = "commit"
	pullOperation     LocalOperation = "pull"
	checkoutOperation LocalOperation = "checkout"
	cloneOperation    LocalOperation = "clone"

	CaseInsensitiveWarningMessageFormat = `Directory '%s' is case-insensitive, versioning tools such as lakectl local and git will work incorrectly.`
)

const localSummaryTemplate = `
{{.Operation}} Summary:

{{ if and (eq .Downloaded 0) (eq .Removed 0) (eq .Uploaded 0)}}No changes{{else -}}
{{"Downloaded:" | printf|green}} {{.Downloaded|green}}
{{"Uploaded:" | printf|yellow}} {{.Uploaded|yellow}}
{{"Removed:" | printf|red}} {{.Removed|red}}
{{end}}
`

var (
	localDefaultArgsRange = cobra.RangeArgs(localDefaultMinArgs, localDefaultMaxArgs)
	ErrUnknownOperation   = errors.New("unknown operation")
)

func withGitIgnoreFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(localGitIgnoreFlagName, true,
		"Update .gitignore file when working in a git repository context")
}

func withForceFlag(cmd *cobra.Command, usage string) {
	cmd.Flags().Bool(localForceFlagName, false, usage)
}

func localDiff(ctx context.Context, client apigen.ClientWithResponsesInterface, remote *uri.URI, path string) local.Changes {
	fmt.Printf("\ndiff 'local://%s' <--> '%s'...\n", path, remote)
	currentRemoteState := make(chan apigen.ObjectStats, maxDiffPageSize)
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

func localHandleSyncInterrupt(ctx context.Context, idx *local.Index, operation string) context.Context {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	go func() {
		defer stop()
		<-ctx.Done()
		pathURI, err := idx.GetCurrentURI()
		if err != nil {
			WriteTo("{{.Error|red}}\n", struct{ Error string }{Error: "Failed to get PathURI index file."}, os.Stderr)
		}
		_, err = local.WriteIndex(idx.LocalPath(), pathURI, idx.AtHead, operation)
		if err != nil {
			WriteTo("{{.Error|red}}\n", struct{ Error string }{Error: "Failed to write failed operation to index file."}, os.Stderr)
		}
		Die(`Operation was canceled, local data may be incomplete.
	Use "lakectl local checkout..." to sync with the remote.`, 1)
	}()
	return ctx
}

func dieOnInterruptedOperation(interruptedOperation LocalOperation, force bool) {
	if !force && interruptedOperation != "" {
		switch interruptedOperation {
		case commitOperation:
			Die(`Latest commit operation was interrupted, data may be incomplete.
Use "lakectl local commit..." to commit your latest changes or "lakectl local pull... --force" to sync with the remote.`, 1)
		case checkoutOperation:
			Die(`Latest checkout operation was interrupted, local data may be incomplete.
Use "lakectl local checkout..." to sync with the remote.`, 1)
		case pullOperation:
			Die(`Latest pull operation was interrupted, local data may be incomplete.
Use "lakectl local pull... --force" to sync with the remote.`, 1)
		case cloneOperation:
			Die(`Latest clone operation was interrupted, local data may be incomplete.
Use "lakectl local checkout..." to sync with the remote or run "lakectl local clone..." with a different directory to sync with the remote.`, 1)
		default:
			panic(fmt.Errorf("found an unknown interrupted operation in the index file: %s- %w", interruptedOperation, ErrUnknownOperation))
		}
	}
}

func warnOnCaseInsensitiveDirectory(path string) {
	isCaseInsensitive, err := fileutil.IsCaseInsensitiveLocation(fileutil.OSFS{}, path, Warning)
	if err != nil {
		Warning(fmt.Sprintf("Check whether directory '%s' is case-insensitive: %s", path, err))
		Warning("Continuing without this check")
	}
	if isCaseInsensitive {
		Warning(fmt.Sprintf(CaseInsensitiveWarningMessageFormat, path))
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
