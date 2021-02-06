package testutil

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/gs"
	"github.com/treeverse/lakefs/block/mem"
	lakefsS3 "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/db/params"
)

const (
	DBContainerTimeoutSeconds = 60 * 30 // 30 minutes

	EnvKeyUseBlockAdapter = "USE_BLOCK_ADAPTER"
	envKeyAwsKeyID        = "AWS_ACCESS_KEY_ID"
	envKeyAwsSecretKey    = "AWS_SECRET_ACCESS_KEY" //nolint:gosec
	envKeyAwsRegion       = "AWS_DEFAULT_REGION"
)

var keepDB = flag.Bool("keep-db", false, "keep test DB instance running")

func GetDBInstance(pool *dockertest.Pool) (string, func()) {
	// connect using docker container name
	containerName := os.Getenv("PG_DB_CONTAINER")
	if containerName != "" {
		resource, ok := pool.ContainerByName(containerName)
		if !ok {
			log.Fatalf("Cloud not find DB container (%s)", containerName)
		}
		uri := formatPostgresResourceURI(resource)
		return uri, func() {}
	}

	// connect using supply address
	dbURI := os.Getenv("PG_TEST_URI")
	if len(dbURI) > 0 {
		// use supplied DB connection for testing
		if err := verifyDBConnectionString(dbURI); err != nil {
			log.Fatalf("could not connect to postgres: %s", err)
		}
		return dbURI, func() {}
	}

	// run new instance and connect
	resource, err := pool.Run("postgres", "11", []string{
		"POSTGRES_USER=lakefs",
		"POSTGRES_PASSWORD=lakefs",
		"POSTGRES_DB=lakefs_db",
	})
	if err != nil {
		log.Fatalf("Could not start postgresql: %s", err)
	}

	// expire the container, just to be on the safe side
	if !*keepDB {
		err = resource.Expire(DBContainerTimeoutSeconds)
		if err != nil {
			log.Fatalf("could not expire postgres container")
		}
	}

	// format db uri
	uri := formatPostgresResourceURI(resource)

	// wait for container to start and connect to db
	if err = pool.Retry(func() error {
		return verifyDBConnectionString(uri)
	}); err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
	}

	// set cleanup
	closer := func() {
		if *keepDB {
			return
		}
		err := pool.Purge(resource)
		if err != nil {
			log.Fatalf("could not kill postgres container")
		}
	}

	// return DB address and closer func
	return uri, closer
}

func formatPostgresResourceURI(resource *dockertest.Resource) string {
	dbParams := map[string]string{
		"POSTGRES_DB":       "lakefs_db",
		"POSTGRES_USER":     "lakefs",
		"POSTGRES_PASSWORD": "lakefs",
		"POSTGRES_PORT":     resource.GetPort("5432/tcp"),
	}
	env := resource.Container.Config.Env
	for _, entry := range env {
		for key := range dbParams {
			if strings.HasPrefix(entry, key+"=") {
				dbParams[key] = entry[len(key)+1:]
				break
			}
		}
	}
	uri := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable",
		dbParams["POSTGRES_USER"],
		dbParams["POSTGRES_PASSWORD"],
		dbParams["POSTGRES_PORT"],
		dbParams["POSTGRES_DB"],
	)
	return uri
}

func verifyDBConnectionString(uri string) error {
	ctx := context.Background()
	pool, err := pgxpool.Connect(ctx, uri)
	if err != nil {
		return err
	}
	defer pool.Close()
	return db.Ping(ctx, pool)
}

type GetDBOptions struct {
	ApplyDDL bool
}

type GetDBOption func(options *GetDBOptions)

func WithGetDBApplyDDL(apply bool) GetDBOption {
	return func(options *GetDBOptions) {
		options.ApplyDDL = apply
	}
}

func GetDB(t testing.TB, uri string, opts ...GetDBOption) (db.Database, string) {
	ctx := context.Background()
	options := &GetDBOptions{
		ApplyDDL: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	// generate uuid as schema name
	generatedSchema := fmt.Sprintf("schema_%s",
		strings.ReplaceAll(uuid.New().String(), "-", ""))

	// create connection
	connURI := fmt.Sprintf("%s&search_path=%s,public", uri, generatedSchema)
	pool, err := pgxpool.Connect(ctx, connURI)
	if err != nil {
		t.Fatalf("could not connect to PostgreSQL: %s", err)
	}
	err = db.Ping(ctx, pool)
	if err != nil {
		pool.Close()
		t.Fatalf("could not ping PostgreSQL: %s", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	database := db.NewPgxDatabase(pool)
	_, err = database.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("CREATE SCHEMA IF NOT EXISTS " + generatedSchema)
	})
	if err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	if options.ApplyDDL {
		// do the actual migration
		err := db.MigrateUp(params.Database{ConnectionString: connURI})
		if err != nil {
			t.Fatal("could not create schema:", err)
		}
	}

	// return DB
	return database, connURI
}

func Must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("error returned for operation: %v", err)
	}
}

func MustDo(t testing.TB, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s, expected no error, got err=%s", what, err)
	}
}

func NewBlockAdapterByType(t testing.TB, translator block.UploadIDTranslator, blockstoreType string) block.Adapter {
	switch blockstoreType {
	case gs.BlockstoreType:
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			t.Fatal("Google Storage new client", err)
		}
		return gs.NewAdapter(client, gs.WithTranslator(translator))

	case lakefsS3.BlockstoreType:
		awsRegion, regionOk := os.LookupEnv(envKeyAwsRegion)
		if !regionOk {
			awsRegion = "us-east-1"
		}
		cfg := &aws.Config{
			Region: aws.String(awsRegion),
		}
		awsSecret, secretOk := os.LookupEnv(envKeyAwsSecretKey)
		awsKey, keyOk := os.LookupEnv(envKeyAwsKeyID)
		if keyOk && secretOk {
			cfg.Credentials = credentials.NewStaticCredentials(awsKey, awsSecret, "")
		} else {
			cfg.Credentials = credentials.NewSharedCredentials("", "default")
		}
		sess := session.Must(session.NewSession(cfg))
		svc := s3.New(sess)
		return lakefsS3.NewAdapter(svc, lakefsS3.WithTranslator(translator))

	default:
		return mem.New(mem.WithTranslator(translator))
	}
}
