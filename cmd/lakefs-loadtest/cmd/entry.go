package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/cmdutils"

	"github.com/treeverse/lakefs/uri"

	"github.com/google/uuid"

	"github.com/treeverse/lakefs/config"

	"github.com/jamiealquiza/tachymeter"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
)

// entryCmd represents the repo command
var entryCmd = &cobra.Command{
	Use:   "entry <ref uri>",
	Short: "Load test database with create entry calls",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))
		connectionString, _ := cmd.Flags().GetString("db")
		requests, _ := cmd.Flags().GetInt("requests")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		sampleRatio, _ := cmd.Flags().GetFloat64("sample")

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
		c := catalog.NewCataloger(database, catalog.WithParams(conf.GetCatalogerCatalogParams()))

		// validate repository and branch
		_, err := c.GetRepository(ctx, u.Repository)
		if err != nil {
			fmt.Printf("Get repository (%s) failed: %s", u.Repository, err)
			os.Exit(1)
		}
		_, err = c.GetBranchReference(ctx, u.Repository, u.Ref)
		if err != nil {
			fmt.Printf("Get branch (%s) failed: %s", u.Ref, err)
			os.Exit(1)
		}

		fmt.Printf("Concurrency: %d\n", concurrency)
		fmt.Printf("Requests: %d\n", requests)

		bar := progressbar.New(requests * concurrency)
		t := tachymeter.New(&tachymeter.Config{Size: int(float64(requests) * sampleRatio)})
		var wg sync.WaitGroup
		wg.Add(concurrency)

		var errCount int64
		startingLine := make(chan bool)
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				<-startingLine
				createEntryParams := catalog.CreateEntryParams{}
				for reqID := 0; reqID < requests; reqID++ {
					startTime := time.Now()
					gen := strings.ReplaceAll(uuid.New().String(), "-", "")
					err := c.CreateEntry(ctx, u.Repository, u.Ref,
						catalog.Entry{
							Path:            gen,
							PhysicalAddress: gen,
							CreationDate:    time.Now(),
							Size:            42,
							Checksum:        gen,
						},
						createEntryParams)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
					}
					t.AddTime(time.Since(startTime))
					_ = bar.Add(1)
				}
			}()
		}

		// start the work
		wallTimeStart := time.Now()
		close(startingLine)

		// wait for workers to complete
		wg.Wait()
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
	dbCmd.AddCommand(entryCmd)
}
