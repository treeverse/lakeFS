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

	"github.com/google/uuid"
	"github.com/jamiealquiza/tachymeter"
	nanoid "github.com/matoous/go-nanoid"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/uri"
)

const createEntryPathLength = 110

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
		defer database.Close()

		conf := config.NewConfig()
		c, err := catalog.NewCataloger(database, conf)
		if err != nil {
			fmt.Printf("Cannot create cataloger: %s\n", err)
			os.Exit(1)
		}

		// validate repository and branch
		_, err = c.GetRepository(ctx, u.Repository)
		if err != nil {
			fmt.Printf("Get repository (%s) failed: %s", u.Repository, err)
			os.Exit(1)
		}
		_, err = c.GetBranchReference(ctx, u.Repository, u.Ref)
		if err != nil {
			fmt.Printf("Get branch (%s) failed: %s", u.Ref, err)
			os.Exit(1)
		}

		totalRequests := requests * concurrency
		fmt.Printf("Concurrency: %d\n", concurrency)
		fmt.Printf("Requests: %d per worker (%d total)\n", requests, totalRequests)

		bar := progressbar.New(totalRequests)
		t := tachymeter.New(&tachymeter.Config{Size: int(float64(totalRequests) * sampleRatio)})
		var wg sync.WaitGroup
		wg.Add(concurrency)

		var errCount int64
		startingLine := make(chan bool)
		for i := 0; i < concurrency; i++ {
			wid := fmt.Sprintf("-%02d", i)
			go func() {
				defer wg.Done()
				<-startingLine
				for reqID := 0; reqID < requests; reqID++ {
					id, err := nanoid.ID(createEntryPathLength)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
					}
					addr := strings.ReplaceAll(uuid.New().String(), "-", "")
					entryPath := strings.ReplaceAll(id, "-", "") + wid
					startTime := time.Now()
					err = c.CreateEntry(ctx, u.Repository, u.Ref, catalog.DBEntry{
						Path:            entryPath,
						CreationDate:    time.Now(),
						Checksum:        addr,
						PhysicalAddress: addr,
					})
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
		fmt.Printf("\n\n%s\n", t.Calc())
		if errCount > 0 {
			fmt.Printf("\n%d requests FAILED!\n", errCount)
		}
		if err := c.Close(); err != nil {
			fmt.Printf("Catalog close with error: %s", err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	dbCmd.AddCommand(entryCmd)
}
