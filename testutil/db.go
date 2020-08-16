package testutil

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
	lakefsS3 "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/db/params"
)

const (
	TimeFormat = "Jan 2 15:04:05 2006 -0700"

	DBContainerTimeoutSeconds = 60 * 30 // 30 minutes

	envKeyUseS3BlockAdapter = "USE_S3_BLOCK_ADAPTER"
	envKeyAwsKeyID          = "AWS_ACCESS_KEY_ID"
	envKeyAwsSecretKey      = "AWS_SECRET_ACCESS_KEY" //nolint:gosec
	envKeyAwsRegion         = "AWS_DEFAULT_REGION"
)

var keepDB = flag.Bool("keep-db", false, "keep test DB instance running")
var addrDB = flag.String("db", "", "DB address to use")

func GetDBInstance(pool *dockertest.Pool) (string, func()) {
	if len(*addrDB) > 0 {
		// use supplied DB connection for testing
		if err := verifyDBConnectionString(*addrDB); err != nil {
			log.Fatalf("could not connect to postgres: %s", err)
		}
		return *addrDB, func() {}
	}
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
	uri := fmt.Sprintf("postgres://lakefs:lakefs@localhost:%s/lakefs_db?sslmode=disable",
		resource.GetPort("5432/tcp"))

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

func verifyDBConnectionString(uri string) error {
	conn, err := sqlx.Connect("pgx", uri)
	if err != nil {
		return err
	}
	err = conn.Ping()
	_ = conn.Close()
	return err
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
	connURI := fmt.Sprintf("%s&search_path=%s", uri, generatedSchema)
	conn, err := sqlx.Connect("pgx", connURI)
	if err != nil {
		t.Fatalf("could not connect to PostgreSQL: %s", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	database := db.NewSqlxDatabase(conn)
	_, err = database.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("CREATE SCHEMA IF NOT EXISTS " + generatedSchema)
	})
	if err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	if options.ApplyDDL {
		// do the actual migration
		err := db.MigrateUp(params.Database{DatabaseURI: connURI})
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

func NewBlockAdapterByEnv(translator block.UploadIDTranslator) block.Adapter {
	_, useS3 := os.LookupEnv(envKeyUseS3BlockAdapter)
	isLocal := !useS3
	if isLocal {
		return mem.New(mem.WithTranslator(translator))
	}

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
	adapter := lakefsS3.NewAdapter(svc, lakefsS3.WithTranslator(translator))
	return adapter
}
