package cmd

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/go-openapi/swag"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

var abuseCmd = &cobra.Command{
	Use:    "abuse <sub command>",
	Short:  "abuse a running lakeFS instance. See sub commands for more info.",
	Hidden: true,
}

var abuseRandomReadsCmd = &cobra.Command{
	Use:    "random-read <source ref uri>",
	Short:  "Read keys from a file and generate random reads from the source ref for those keys.",
	Hidden: false,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))

		// how many branches to create
		amount, err := cmd.Flags().GetInt("amount")
		if err != nil {
			DieErr(err)
		}

		// how many calls in parallel to execute
		parallelism, err := cmd.Flags().GetInt("parallelism")
		if err != nil {
			DieErr(err)
		}

		// prefix to create new branches with
		fromFile, err := cmd.Flags().GetString("from-file")
		if err != nil {
			DieErr(err)
		}
		reader := GetReader(fromFile)
		defer func() {
			err := reader.Close()
			if err != nil {
				DieErr(err)
			}
		}()
		scanner := bufio.NewScanner(reader)
		keys := make([]string, 0)
		for scanner.Scan() {
			keys = append(keys, scanner.Text())
		}
		Fmt("read a total of %d keys from key file\n", len(keys))

		workFn := func(input chan testutil.Request, output chan testutil.Result) {
			ctx := context.Background()
			client := getClient()
			for work := range input {
				start := time.Now()
				_, err := client.StatObject(ctx, u.Repository, u.Ref, work.Payload)
				output <- testutil.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		}
		pool := testutil.NewWorkerPool(parallelism, workFn)

		// generate load
		go func(reqs chan testutil.Request) {
			rand.Seed(time.Now().Unix())
			for i := 0; i < amount; i++ {
				//nolint:gosec
				reqs <- testutil.Request{
					Payload: keys[rand.Intn(len(keys))],
				}
			}
			close(reqs)
		}(pool.Input)

		// execute the things!
		collector := testutil.NewResultCollector(pool.Output)
		go collector.Collect() // start measuring the stuff
		pool.Start()           // start executing the stuff

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Printf("%s\n", collector.Stats())
			case <-pool.Done():
				fmt.Printf("%s\n\n", collector.Stats())
				fmt.Printf("%s\n", collector.Histogram())
				return
			case <-signals:
				fmt.Printf("%s\n\n", collector.Stats())
				fmt.Printf("Historgram (ms): %s\n", collector.Histogram())
				return
			}
		}
	},
}

var abuseCreateBranchesCmd = &cobra.Command{
	Use:    "create-branches <source ref uri>",
	Short:  "Create a lot of branches very quickly.",
	Hidden: false,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))

		// only clean prefixed branches without creating new ones
		cleanOnly, err := cmd.Flags().GetBool("clean-only")
		if err != nil {
			DieErr(err)
		}

		// prefix to create new branches with
		branchPrefix, err := cmd.Flags().GetString("branch-prefix")
		if err != nil {
			DieErr(err)
		}
		client := getClient()

		// how many branches to create
		amount, err := cmd.Flags().GetInt("amount")
		if err != nil {
			DieErr(err)
		}

		// how many calls in parallel to execute
		parallelism, err := cmd.Flags().GetInt("parallelism")
		if err != nil {
			DieErr(err)
		}

		// delete all prefixed branches first for a clean start
		totalDeleted := 0
		semaphore := make(chan bool, parallelism)
		for {
			branches, pagination, err := client.ListBranches(context.Background(), u.Repository, branchPrefix, 1000)
			if err != nil {
				DieErr(err)
			}
			matches := 0
			var wg sync.WaitGroup
			for _, b := range branches {
				branch := swag.StringValue(b.ID)
				if !strings.HasPrefix(branch, branchPrefix) {
					continue
				}
				matches++
				totalDeleted++
				wg.Add(1)
				go func(branch string) {
					semaphore <- true
					err := client.DeleteBranch(context.Background(), u.Repository, branch)
					if err != nil {
						DieErr(err)
					}
					wg.Done()
					<-semaphore
				}(branch)
			}
			if matches == 0 {
				break // no more
			}
			wg.Wait() // wait for this batch to be over
			Fmt("branches deleted so far: %d\n", totalDeleted)
			if !swag.BoolValue(pagination.HasMore) {
				break
			}
		}
		Fmt("total deleted with prefix: %d\n", totalDeleted)

		if cleanOnly {
			return // done.
		}

		worker := func(ctx context.Context, inp chan string, out chan struct{}) {
			for {
				select {
				case x := <-inp:
					_, err := client.CreateBranch(ctx, u.Repository, &models.BranchCreation{
						Name:   &x,
						Source: &u.Ref,
					})
					if err != nil {
						DieErr(err)
					}
					out <- struct{}{}
				case <-ctx.Done():
					break
				}
			}
		}

		Fmt("creating %d branches now...\n", amount)
		reqs := make(chan string, amount)
		responses := make(chan struct{})
		for i := 1; i <= amount; i++ {
			reqs <- fmt.Sprintf("%s%d", branchPrefix, i)
		}

		// now run it with the given parallelism
		ctx := context.Background()
		for i := 0; i < parallelism; i++ {
			go worker(ctx, reqs, responses)
		}

		// collect responses
		i := 0
		t := time.Now()
		const printEvery = 10000
		start := time.Now()
		for {
			<-responses
			i++
			if i%printEvery == 0 {
				thisBatch := time.Since(t)
				Fmt("done %d calls in %s (%.2f/second)\n", i, thisBatch, float64(printEvery)/thisBatch.Seconds())
				t = time.Now()
			}
			if i == amount {
				break
			}
		}

		took := time.Since(start)
		Fmt("Done! created %d branches in %s: (%.2f/second)\n\n", amount, took, float64(amount)/took.Seconds())
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(abuseCmd)

	abuseCmd.AddCommand(abuseCreateBranchesCmd)
	abuseCreateBranchesCmd.Flags().String("branch-prefix", "abuse-", "prefix to create branches under")
	abuseCreateBranchesCmd.Flags().Bool("clean-only", false, "only clean up past runs")
	abuseCreateBranchesCmd.Flags().Int("amount", 1000000, "amount of things to do")
	abuseCreateBranchesCmd.Flags().Int("parallelism", 100, "amount of things to do in parallel")

	abuseCmd.AddCommand(abuseRandomReadsCmd)
	abuseRandomReadsCmd.Flags().String("from-file", "", "read keys from this file (\"-\" for stdin)")
	abuseRandomReadsCmd.Flags().Int("amount", 1000000, "amount of reads to do")
	abuseRandomReadsCmd.Flags().Int("parallelism", 100, "amount of reads to do in parallel")
}
