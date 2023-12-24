package cmd

import (
	"fmt"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
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
		ignore := Must(cmd.Flags().GetBool("ignore"))

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

		repo := resp.JSON200
		storagePrefix := repo.StorageNamespace
		var size int64
		checksum := "00695c7307b0480c7b6bdc873cf05c15"
		addr := storagePrefix + "/random-write"
		creationInfo := apigen.ObjectStageCreation{
			Checksum:        checksum,
			PhysicalAddress: addr,
			SizeBytes:       size,
			Force:           swag.Bool(ignore),
		}

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for work := range input {
				start := time.Now()
				resp, err := client.StageObjectWithResponse(ctx, u.Repository, u.Ref, &apigen.StageObjectParams{Path: work},
					apigen.StageObjectJSONRequestBody(creationInfo))
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
	abuseCmd.AddCommand(abuseRandomWritesCmd)
	abuseRandomWritesCmd.Flags().String("prefix", "abuse/", "prefix to create paths under")
	abuseRandomWritesCmd.Flags().Int("amount", abuseDefaultAmount, "amount of writes to do")
	abuseRandomWritesCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of writes to do in parallel")
	abuseRandomWritesCmd.Flags().Bool("ignore", false, "ignore read-only protection on the repository")
}
