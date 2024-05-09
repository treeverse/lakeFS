package cmd

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
)

var abuseCommitCmd = &cobra.Command{
	Use:               "commit <branch URI>",
	Short:             "Commits to the source branch repeatedly",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch URI", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		gapDuration := Must(cmd.Flags().GetDuration("gap"))

		fmt.Println("Source branch:", u)

		generator := stress.NewGenerator("commit", 1, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(strconv.Itoa(i + 1))
			}
		})

		// generate randomly selected keys as input
		client := getClient()
		resp, err := client.GetRepositoryWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			allowEmpty := true
			for work := range input {
				start := time.Now()
				resp, err := client.CommitWithResponse(ctx, u.Repository, u.Ref, &apigen.CommitParams{},
					apigen.CommitJSONRequestBody(apigen.CommitCreation{
						Message:    work,
						AllowEmpty: &allowEmpty,
					}))
				if err == nil && resp.StatusCode() != http.StatusOK {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(gapDuration):
				}
			}
		})
	},
}

//nolint:gochecknoinits
func init() {
	const defaultGap = 2 * time.Second

	abuseCommitCmd.Flags().Int("amount", abuseDefaultParallelism, "amount of commits to do")
	abuseCommitCmd.Flags().Duration("gap", defaultGap, "duration to wait between commits")

	abuseCmd.AddCommand(abuseCommitCmd)
}
