package cmd

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
)

var abuseRandomReadsCmd = &cobra.Command{
	Use:               "random-read <source ref URI>",
	Short:             "Read keys from a file and generate random reads from the source ref for those keys.",
	Hidden:            false,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref URI", args[0])
		amount := Must(cmd.Flags().GetInt("amount"))
		parallelism := Must(cmd.Flags().GetInt("parallelism"))
		fromFile := Must(cmd.Flags().GetString("from-file"))

		fmt.Println("Source ref:", u)
		// read the input file
		keys, err := readLines(fromFile)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("read a total of %d keys from key file\n", len(keys))

		generator := stress.NewGenerator("read", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				// rand.Intn is good enough for abuse.
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
				resp, err := client.StatObjectWithResponse(ctx, u.Repository, u.Ref, &apigen.StatObjectParams{
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

func readLines(path string) (lines []string, err error) {
	reader := Must(OpenByPath(path))
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("%w, and while closing %s", err, closeErr)
			}
		}
	}()
	scanner := bufio.NewScanner(reader)
	lines = make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

//nolint:gochecknoinits
func init() {
	abuseCmd.AddCommand(abuseRandomReadsCmd)
	abuseRandomReadsCmd.Flags().String("from-file", "", "read keys from this file (\"-\" for stdin)")
	abuseRandomReadsCmd.Flags().Int("amount", abuseDefaultAmount, "amount of reads to do")
	abuseRandomReadsCmd.Flags().Int("parallelism", abuseDefaultParallelism, "amount of reads to do in parallel")
}
