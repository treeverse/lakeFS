package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/docker/testutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jamiealquiza/tachymeter"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const usePgx = false

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
		runTrace, _ := cmd.Flags().GetBool("trace")
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

		pgxConfig, err := pgxpool.ParseConfig(connectionString)
		if err != nil {
			panic(err)
		}
		pgxConfig.MaxConns = 25
		pgxConfig.MaxConnIdleTime = 5 * time.Minute
		pgxConfig.MinConns = 5
		pool, err := pgxpool.ConnectConfig(ctx, pgxConfig)
		if err != nil {
			panic(err)
		}

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

		if runTrace {
			f, err := os.Create("trace.out")
			if err != nil {
				fmt.Printf("failed to create trace output file: %s\n", err)
				os.Exit(1)
			}
			defer func() {
				if err := f.Close(); err != nil {
					fmt.Printf("Failed to close 'trace.out': %s\n", err)
				}
			}()

			if err := trace.Start(f); err != nil {
				fmt.Printf("failed to start trace: %s\n", err)
				os.Exit(1)
			}
		}

		//bar := progressbar.New(requests * concurrency)
		t := tachymeter.New(&tachymeter.Config{Size: int(float64(requests) * sampleRatio)})
		var wg sync.WaitGroup
		wg.Add(concurrency) // workers and one producer
		var errCount int64
		startingLine := make(chan bool)
		for i := 0; i < concurrency; i++ {
			uid := strings.ReplaceAll(uuid.New().String(), "-", "")
			go func() {
				res, err := database.Transact(func(tx db.Tx) (interface{}, error) {
					const q = `SELECT b.id FROM catalog_branches b join catalog_repositories r ON r.id = b.repository_id WHERE r.name = $1 AND b.name = $2`
					if err != nil {
						return 0, err
					}
					var branchID int64
					err = tx.Get(&branchID, q, repository, "master")
					return branchID, err

				}, db.WithContext(ctx))
				if err != nil {
					panic(err)
				}
				branchID := res.(int64)

				defer wg.Done()
				<-startingLine
				//logger := logging.Default()
				for reqID := 0; reqID < requests; reqID++ {
					reqIDString := strconv.Itoa(reqID)
					addr := uid + "_" + reqIDString
					prefix := ""
					levels := rand.Intn(10)
					for i := 0; i < levels; i++ {
						prefix += fmt.Sprintf("folder%d/", i)
					}
					entPath := prefix + addr
					entChecksum := uid + reqIDString
					creationDate := time.Now()
					ent := catalog.Entry{
						Path:            entPath,
						PhysicalAddress: "a" + addr,
						CreationDate:    creationDate,
						Size:            int64(rand.Intn(1024*1024)) + 1,
						Checksum:        entChecksum,
						Metadata:        nil,
					}
					if usePgx {
						err = insertEntryPgx(ctx, pool, branchID, &ent)
					} else {
						err = insertEntry(ctx, database, branchID, &ent)
					}
					if err != nil {
						atomic.AddInt64(&errCount, 1)
					}
					t.AddTime(time.Since(creationDate))
					//_ = bar.Add64(1)
				}
			}()
		}

		wallTimeStart := time.Now()
		close(startingLine)

		// generate entries and wait for workers to complete
		wg.Wait()
		//_ = bar.Finish()
		t.SetWallTime(time.Since(wallTimeStart))
		if runTrace {
			trace.Stop()
		}
		fmt.Printf("\n%s\n", t.Calc())
		if errCount > 0 {
			fmt.Printf("%d requests failed!\n", errCount)
		}
	},
}

func insertEntryPgx(ctx context.Context, pool *pgxpool.Pool, branchID int64, entry *catalog.Entry) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	var ctid string
	err = conn.QueryRow(ctx, `INSERT INTO catalog_entries (branch_id,path,physical_address,checksum,size,metadata) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=catalog_max_commit_id()
			RETURNING ctid`,
		branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata).
		Scan(&ctid)
	return err
}

func insertEntry(ctx context.Context, database db.Database, branchID int64, entry *catalog.Entry) error {
	_, err := database.Transact(func(tx db.Tx) (interface{}, error) {
		var ctid string
		err := tx.Get(&ctid, `INSERT INTO catalog_entries (branch_id,path,physical_address,checksum,size,metadata) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=catalog_max_commit_id()
			RETURNING ctid`,
			branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata)
		return ctid, err
	}, db.WithContext(ctx))
	return err
}

//nolint:gochecknoinits
func init() {
	dbCmd.AddCommand(entryCmd)
	entryCmd.Flags().BoolP("trace", "t", false, "Run trace")
}
