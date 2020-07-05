package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/docker/testutil"
	"github.com/google/uuid"
	"github.com/jamiealquiza/tachymeter"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

type ReqResult struct {
	Err  error
	Took time.Duration
}

// entryCmd represents the entry command
var entryCmd = &cobra.Command{
	Use:   "entry",
	Short: "Load test database with entries",
	Run: func(cmd *cobra.Command, args []string) {
		connectionString, _ := cmd.Flags().GetString("db")
		requests, _ := cmd.Flags().GetInt("requests")
		repository, _ := cmd.Flags().GetString("repository")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		sampleRatio, _ := cmd.Flags().GetFloat64("sample")
		createRepository := repository == ""

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

		c := catalog.NewCataloger(database)
		if createRepository {
			// create repository
			repository = "repo-" + strings.ToLower(testutil.GenerateRandomAlphaOnlyString(10))
			err := c.CreateRepository(ctx, repository, fmt.Sprintf("s3://test-bucket/%s/", repository), "master")
			if err != nil {
				fmt.Printf("Failed to create repository %s: %s\n", repository, err)
			} else {
				fmt.Printf("Repository created: %s\n", repository)
			}
		}
		fmt.Printf("Concurrency: %d\n", concurrency)
		fmt.Printf("Requests: %d\n", requests)

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
					addr := uuid.New().String()
					prefix := ""
					levels := rand.Intn(10)
					for i := 0; i < levels; i++ {
						prefix += fmt.Sprintf("folder%d/", i)
					}
					entPath := prefix + addr
					entChecksum := sha256.Sum256([]byte(entPath))
					creationDate := time.Now()
					ent := catalog.Entry{
						Path:            entPath,
						PhysicalAddress: "a" + addr,
						CreationDate:    creationDate,
						Size:            int64(rand.Intn(1024*1024)) + 1,
						Checksum:        hex.EncodeToString(entChecksum[:]),
						Metadata:        nil,
					}
					err := c.CreateEntry(ctx, repository, "master", ent)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
						fmt.Printf("Requet failed - %s: %s", entPath, err)
					} else {
						t.AddTime(time.Since(creationDate))
					}
					_ = bar.Add64(1)
				}
			}()
		}

		wallTimeStart := time.Now()
		close(startingLine)

		// generate entries and wait for workers to complete
		wg.Wait()
		_ = bar.Finish()
		t.SetWallTime(time.Since(wallTimeStart))
		fmt.Printf("\n%s\n", t.Calc())
		if errCount > 0 {
			fmt.Printf("%d requests failed!\n", errCount)
		}
	},
}

func connectToDB(connectionString string) db.Database {
	database, err := db.ConnectDB(config.DefaultDatabaseDriver, connectionString)
	if err != nil {
		fmt.Printf("Failed connecting to database: %s\n", err)
		os.Exit(1)
	}
	return database
}

func init() {
	dbCmd.AddCommand(entryCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// entryCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// entryCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
