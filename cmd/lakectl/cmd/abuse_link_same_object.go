package cmd

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
	"github.com/treeverse/lakefs/pkg/uri"
)

var abuseLinkSameObjectCmd = &cobra.Command{
	Use:               "link-same-object <branch URI>",
	Short:             "Link the same object in parallel.",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch URI", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))
		key := Must(cmd.Flags().GetString("key"))

		fmt.Println("Source ref:", u)
		fmt.Println("Object key:", key)

		generator := stress.NewGenerator("get-and-link", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// setup generator to use the key
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(key)
			}
		})

		// execute the things!
		runLinkObject(cmd, u, generator)
	},
}

func runLinkObject(cmd *cobra.Command, u *uri.URI, generator *stress.Generator) {
	generator.Run(func(input chan string, output chan stress.Result) {
		ctx := cmd.Context()
		client := getClient()
		for work := range input {
			start := time.Now()

			getResponse, err := client.GetPhysicalAddressWithResponse(ctx, u.Repository, u.Ref, &apigen.GetPhysicalAddressParams{Path: work})
			if err == nil && getResponse.JSON200 == nil {
				err = helpers.ResponseAsError(getResponse)
			}
			if err != nil {
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
				continue
			}

			// The code links an "existing object" without actually uploading the object.
			// This tests the operations done on the lakeFS server side without the overhead of uploading the
			// object to the object store which should optimally be performed with lakeFS not in the data path (upload using pre-signed urls / set/link).
			stagingLocation := getResponse.JSON200
			linkResponse, err := client.LinkPhysicalAddressWithResponse(ctx, u.Repository, u.Ref,
				&apigen.LinkPhysicalAddressParams{
					Path: work,
				},
				apigen.LinkPhysicalAddressJSONRequestBody{
					Checksum: "00695c7307b0480c7b6bdc873cf05c15",
					Staging: apigen.StagingLocation{
						PhysicalAddress: stagingLocation.PhysicalAddress,
					},
					UserMetadata: nil,
				})
			if err == nil && linkResponse.JSON200 == nil {
				err = helpers.ResponseAsError(linkResponse)
			}
			output <- stress.Result{
				Error: err,
				Took:  time.Since(start),
			}
		}
	})
}

//nolint:gochecknoinits
func init() {
	abuseCmd.AddCommand(abuseLinkSameObjectCmd)
	abuseLinkSameObjectCmd.Flags().Int("amount", abuseDefaultAmount, "amount of link object to do")
	abuseLinkSameObjectCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of link object to do in parallel")
	abuseLinkSameObjectCmd.Flags().String("key", "linked-object", "key used for the test")
}
