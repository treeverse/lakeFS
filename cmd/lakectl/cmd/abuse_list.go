package cmd

import (
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

var abuseListCmd = &cobra.Command{
	Use:               "list <source ref uri>",
	Short:             "List from the source ref",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("Operation requires a valid reference URI. e.g. lakefs://<repo>/<ref>", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))
		prefix := Must(cmd.Flags().GetString("prefix"))

		generator := stress.NewGenerator("list", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(strconv.Itoa(i + 1))
			}
		})

		listPrefix := apigen.PaginationPrefix(prefix)
		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for range input {
				start := time.Now()
				resp, err := client.ListObjectsWithResponse(ctx, u.Repository, u.Ref, &apigen.ListObjectsParams{
					Prefix: &listPrefix,
				})
				if err == nil && resp.StatusCode() != http.StatusOK {
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
	abuseCmd.AddCommand(abuseListCmd)
	abuseListCmd.Flags().String("prefix", "abuse/", "prefix to list under")
	abuseListCmd.Flags().Int("amount", abuseDefaultAmount, "amount of lists to do")
	abuseListCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of lists to do in parallel")
}
