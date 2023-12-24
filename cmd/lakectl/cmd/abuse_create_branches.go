package cmd

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
)

var abuseCreateBranchesCmd = &cobra.Command{
	Use:               "create-branches <source ref URI>",
	Short:             "Create a lot of branches very quickly.",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref URI", args[0])
		cleanOnly := Must(cmd.Flags().GetBool("clean-only"))
		branchPrefix := Must(cmd.Flags().GetString("branch-prefix"))
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))
		ignore := Must(cmd.Flags().GetBool("ignore"))

		client := getClient()

		fmt.Println("Source ref:", u)
		deleteGen := stress.NewGenerator("delete branch", parallelism)

		const paginationAmount = 1000
		deleteGen.Setup(func(add stress.GeneratorAddFn) {
			currentOffset := apigen.PaginationAfter(branchPrefix)
			amount := apigen.PaginationAmount(paginationAmount)
			for {
				resp, err := client.ListBranchesWithResponse(cmd.Context(), u.Repository, &apigen.ListBranchesParams{
					After:  &currentOffset,
					Amount: &amount,
				})
				DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
				if resp.JSON200 == nil {
					Die("Bad response from server", 1)
				}

				for _, ref := range resp.JSON200.Results {
					if !strings.HasPrefix(ref.Id, branchPrefix) {
						return
					}
					add(ref.Id) // this branch should be deleted!
				}
				pagination := resp.JSON200.Pagination
				if !pagination.HasMore {
					return
				}
				currentOffset = apigen.PaginationAfter(pagination.NextOffset)
			}
		})

		// wait for deletes to end
		deleteGen.Run(func(input chan string, output chan stress.Result) {
			for branch := range input {
				start := time.Now()
				_, err := client.DeleteBranchWithResponse(cmd.Context(), u.Repository, branch, &apigen.DeleteBranchParams{Force: swag.Bool(ignore)})
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})

		if cleanOnly {
			return // done.
		}

		// start creating branches
		generator := stress.NewGenerator("create branch", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate create branch requests
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(fmt.Sprintf("%s-%d", branchPrefix, i))
			}
		})

		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			for branch := range input {
				start := time.Now()
				resp, err := client.CreateBranchWithResponse(
					ctx, u.Repository, apigen.CreateBranchJSONRequestBody{
						Name:   branch,
						Source: u.Ref,
					})
				if err == nil && resp.StatusCode() != http.StatusCreated {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})
	},
}

//nolint:gochecknoinits
func init() {
	abuseCmd.AddCommand(abuseCreateBranchesCmd)
	abuseCreateBranchesCmd.Flags().String("branch-prefix", "abuse-", "prefix to create branches under")
	abuseCreateBranchesCmd.Flags().Bool("clean-only", false, "only clean up past runs")
	abuseCreateBranchesCmd.Flags().Int("amount", abuseDefaultAmount, "amount of things to do")
	abuseCreateBranchesCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of things to do in parallel")
	abuseCreateBranchesCmd.Flags().Bool("ignore", false, "ignore read-only protection on the repository")
}
