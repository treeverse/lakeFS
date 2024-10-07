package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
	"github.com/treeverse/lakefs/pkg/uri"
)

var abuseMergeCmd = &cobra.Command{
	Use:               "merge <branch URI>",
	Short:             "Merge nonconflicting objects to the source branch in parallel",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch URI", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))

		fmt.Println("Source branch:", u)

		generator := stress.NewGenerator("merge", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(strconv.Itoa(i + 1))
			}
		})

		client := getClient()
		resp, err := client.GetRepositoryWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			DieFmt("Bad response from server: %+v", resp)
		}

		ctx := cmd.Context()

		// execute ALL the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			client := getClient()
			for work := range input {
				start := time.Now()
				err := mergeSomething(ctx, client, u, work)
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
				// Don't block or sleep to maximise parallel load.
			}
		})
	},
}

func mergeSomething(ctx context.Context, client *apigen.ClientWithResponses, base *uri.URI, name string) error {
	createBranchResponse, err := client.CreateBranchWithResponse(ctx, base.Repository,
		apigen.CreateBranchJSONRequestBody{
			Name:   name,
			Source: base.Ref,
		},
	)
	if err != nil || !apiutil.IsStatusCodeOK(createBranchResponse.StatusCode()) {
		if err == nil {
			err = helpers.ResponseAsError(createBranchResponse)
			return fmt.Errorf("create branch %s: %w", name, err)
		}
	}

	u := base.WithRef(name)
	// Use a different name on each branch, to avoid conflicts.
	path := fmt.Sprintf("object-%s", name)
	u.Path = &path

	getResponse, err := client.GetPhysicalAddressWithResponse(ctx, u.Repository, u.Ref, &apigen.GetPhysicalAddressParams{Path: *u.Path})
	if err != nil || getResponse.JSON200 == nil {
		if err == nil {
			err = helpers.ResponseAsError(getResponse)
		}
		return fmt.Errorf("get physical address for %s: %w", name, err)
	}
	// Link the object but do not actually upload anything - it is not
	// important for merging, and would only reduce load.
	stagingLocation := getResponse.JSON200
	linkResponse, err := client.LinkPhysicalAddressWithResponse(ctx, u.Repository, u.Ref,
		&apigen.LinkPhysicalAddressParams{
			Path: *u.Path,
		},
		apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum: "deadbeef0000cafe",
			Staging: apigen.StagingLocation{
				PhysicalAddress: stagingLocation.PhysicalAddress,
			},
			UserMetadata: nil,
		})
	if err != nil || linkResponse.JSON200 == nil {
		if err == nil {
			err = helpers.ResponseAsError(linkResponse)
		}
		return fmt.Errorf("Link physical address for %s: %w", name, err)
	}

	commitResponse, err := client.CommitWithResponse(ctx, u.Repository, u.Ref, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: fmt.Sprintf("commit %s", name)})
	if err != nil || commitResponse.JSON201 == nil {
		if err == nil {
			err = helpers.ResponseAsError(commitResponse)
		}
		return fmt.Errorf("Commit for %s: %w", name, err)
	}

	mergeResponse, err := client.MergeIntoBranchWithResponse(ctx, u.Repository, u.Ref, base.Ref, apigen.MergeIntoBranchJSONRequestBody{})
	if err != nil || mergeResponse.JSON200 == nil {
		if err == nil {
			err = helpers.ResponseAsError(mergeResponse)
		}
		return fmt.Errorf("Merge from %s: %w", name, err)
	}

	return nil
}

//nolint:gochecknoinits
func init() {
	abuseMergeCmd.Flags().Int("amount", 1000, "amount of merges to perform")
	abuseMergeCmd.Flags().Int("parallelism", abuseDefaultParallelism, "number of merges to perform in parallel")

	abuseCmd.AddCommand(abuseMergeCmd)
}
