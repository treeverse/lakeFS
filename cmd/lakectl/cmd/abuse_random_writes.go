package cmd

import (
	"fmt"
	"net/http"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
)

var abuseRandomWritesCmd = &cobra.Command{
	Use:               "random-write <branch URI>",
	Short:             "Generate random writes to the source branch",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch URI", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))
		prefix := Must(cmd.Flags().GetString("prefix"))

		fmt.Println("Source branch:", u)
		generator := stress.NewGenerator("stage object", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(fmt.Sprintf("%sfile-%d", prefix, i))
			}
		})

		client := getClient()
		resp, err := client.GetRepositoryWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		// execute the things!
		runLinkObject(cmd, u, generator)
	},
}

//nolint:gochecknoinits
func init() {
	abuseCmd.AddCommand(abuseRandomWritesCmd)
	abuseRandomWritesCmd.Flags().String("prefix", "abuse/", "prefix to create paths under")
	abuseRandomWritesCmd.Flags().Int("amount", abuseDefaultAmount, "amount of writes to do")
	abuseRandomWritesCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of writes to do in parallel")
}
