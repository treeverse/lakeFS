package cmd

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
)

var abuseRandomReadsCmd = &cobra.Command{
	Use:               "random-read <source ref uri>",
	Short:             "Read keys from a file and generate random reads from the source ref for those keys.",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))
		fromFile := Must(cmd.Flags().GetString("from-file"))

		fmt.Printf("Source ref: %s\n", u.String())
		// read the input file
		keys, err := readLines(fromFile)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("read a total of %d keys from key file\n", len(keys))

		generator := stress.NewGenerator("read", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		rand.Seed(time.Now().Unix())
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				//nolint:gosec
				add(keys[rand.Intn(len(keys))])
			}
		})

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for work := range input {
				start := time.Now()
				resp, err := client.StatObjectWithResponse(ctx, u.Repository, u.Ref, &api.StatObjectParams{
					Path: work,
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
	abuseCmd.AddCommand(abuseRandomReadsCmd)
	abuseRandomReadsCmd.Flags().String("from-file", "", "read keys from this file (\"-\" for stdin)")
	abuseRandomReadsCmd.Flags().Int("amount", abuseDefaultAmount, "amount of reads to do")
	abuseRandomReadsCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of reads to do in parallel")
}
