package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/config"

	"github.com/google/uuid"
	"github.com/jamiealquiza/tachymeter"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
)

// repoCmd represents the repo command
var repoCmd = &cobra.Command{
	Use:   "repo",
	Short: "Load test database with repository calls",
	Run: func(cmd *cobra.Command, args []string) {
		connectionString, _ := cmd.Flags().GetString("db")
		requests, _ := cmd.Flags().GetInt("requests")
		repository, _ := cmd.Flags().GetString("repository")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		sampleRatio, _ := cmd.Flags().GetFloat64("sample")
		runTrace, _ := cmd.Flags().GetBool("trace")

		if concurrency < 1 {
			fmt.Printf("Concurrency must be above 1! (%d)\n", concurrency)
			os.Exit(1)
		}
		if requests < 0 {
			fmt.Printf("Requests must be above 1! (%d)\n", concurrency)
			os.Exit(1)
		}

		rand.Seed(time.Now().UTC().UnixNano()) // make it special

		ctx := context.Background()
		database := connectToDB(connectionString)
		conf := config.NewConfig()
		c := catalog.NewCataloger(database, conf.GetBatchReadParams())
		if repository == "" {
			repository = "repo-" + strings.ToLower(uuid.New().String())
		}
		fmt.Printf("Repository: %s\n", repository)
		fmt.Printf("Concurrency: %d\n", concurrency)
		fmt.Printf("Requests: %d\n", requests)

		if runTrace {
			f, err := os.Create("trace.out")
			if err != nil {
				fmt.Printf("failed to create trace output file: %s\n", err)
				os.Exit(1)
			}
			defer func() {
				_ = f.Close()
			}()

			if err := trace.Start(f); err != nil {
				fmt.Printf("failed to start trace: %s\n", err)
				os.Exit(1)
			}
		}

		bar := progressbar.New(requests * concurrency)
		t := tachymeter.New(&tachymeter.Config{Size: int(float64(requests) * sampleRatio)})
		var wg sync.WaitGroup
		wg.Add(concurrency) // workers and one producer
		var errCount int64
		startingLine := make(chan bool)
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				<-startingLine
				for reqID := 0; reqID < requests; reqID++ {
					startTime := time.Now()
					_, err := c.GetRepository(ctx, repository)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
					}
					t.AddTime(time.Since(startTime))
					_ = bar.Add64(1)
				}
			}()
		}

		wallTimeStart := time.Now()
		close(startingLine)

		// generate repository calls and wait for workers to complete
		wg.Wait()
		if runTrace {
			trace.Stop()
		}
		_ = bar.Finish()
		t.SetWallTime(time.Since(wallTimeStart))
		fmt.Printf("\n%s\n", t.Calc())
		if errCount > 0 {
			fmt.Printf("%d requests failed!\n", errCount)
		}
	},
}

//nolint:gochecknoinits
func init() {
	dbCmd.AddCommand(repoCmd)
	repoCmd.Flags().BoolP("trace", "t", false, "Run trace")
}
