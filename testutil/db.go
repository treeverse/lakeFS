package testutil

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/treeverse/lakefs/block/local"

	"github.com/jmoiron/sqlx"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/ory/dockertest/v3"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/block"
	lakefsS3 "github.com/treeverse/lakefs/block/s3"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
)

const (
	TimeFormat                = "Jan 2 15:04:05 2006 -0700"
	FixtureRoot               = "lakeFsFixtures"
	DBContainerTimeoutSeconds = 60 * 30 // 30 minutes
	S3BlockAdapterEnvVar      = "USE_S3_BLOCK_ADAPTER"
	AWS_KEY                   = "AWS_ACCESS_KEY_ID"
	AWS_SECRET                = "AWS_SECRET_ACCESS_KEY"
	AWS_REGION                = "AWS_DEFAULT_REGION"
)

func GetIndexWithRepo(t *testing.T, conn db.Database) (index.Index, *model.Repo) {
	repoCreateDate, _ := time.Parse(TimeFormat, "Apr 7 15:13:13 2005 -0700")
	createIndex := index.NewDBIndex(conn, index.WithTimeGenerator(func() time.Time {
		return repoCreateDate
	}))
	Must(t, createIndex.CreateRepo("example", "s3://example-tzahi", "master"))
	return index.NewDBIndex(conn), &model.Repo{
		Id:               "example",
		StorageNamespace: " example-tzahi",
		CreationDate:     repoCreateDate,
		DefaultBranch:    "master",
	}
}

func GetDBInstance(pool *dockertest.Pool) (string, func()) {
	resource, err := pool.Run("postgres", "11", []string{
		"POSTGRES_USER=lakefs",
		"POSTGRES_PASSWORD=lakefs",
		"POSTGRES_DB=lakefs_db",
	})
	if err != nil {
		log.Fatalf("Could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := pool.Purge(resource)
		if err != nil {
			log.Fatalf("could not kill postgres container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(DBContainerTimeoutSeconds)
	if err != nil {
		log.Fatalf("could not expire postgres container")
	}

	// create connection
	var conn *sqlx.DB
	uri := fmt.Sprintf("postgres://lakefs:lakefs@localhost:%s/lakefs_db?sslmode=disable",
		resource.GetPort("5432/tcp"))
	if err = pool.Retry(func() error {
		var err error
		conn, err = sqlx.Connect("pgx", uri)
		if err != nil {
			return err
		}
		return conn.Ping()
	}); err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
	}
	_ = conn.Close()

	// return DB
	return uri, closer
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

func GetDB(t *testing.T, uri, schemaName string, opts ...GetDBOption) db.Database {
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
	conn, err := sqlx.Connect("pgx", fmt.Sprintf("%s&search_path=%s", uri, generatedSchema))
	if err != nil {
		t.Fatalf("could not connect to PostgreSQL: %s", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	database := db.NewDatabase(conn)

	// apply DDL
	_, err = database.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec("CREATE SCHEMA " + generatedSchema)
		if err != nil {
			return nil, err
		}
		if options.ApplyDDL {
			// do the actual migration
			return nil, db.MigrateSchemaAll(tx, schemaName)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// return DB
	return database
}

func GetBlockAdapter(t *testing.T, translator block.UploadIdTranslator) block.Adapter {
	_, useS3 := os.LookupEnv(S3BlockAdapterEnvVar)
	isLocal := !useS3
	if isLocal {
		dir := filepath.Join(os.TempDir(), FixtureRoot, fmt.Sprintf("blocks-%s", uuid.Must(uuid.NewUUID()).String()))
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := local.NewAdapter(dir, local.WithTranslator(translator))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := os.RemoveAll(dir)
			if err != nil {
				t.Fatal(err)
			}
		})
		return adapter
	} else {
		aws_region, region_ok := os.LookupEnv(AWS_REGION)
		if !region_ok {
			aws_region = "us-east-1"
		}
		cfg := &aws.Config{
			Region: aws.String(aws_region),
		}
		aws_secret, secret_ok := os.LookupEnv(AWS_SECRET)
		aws_key, key_ok := os.LookupEnv(AWS_KEY)
		if key_ok && secret_ok {
			cfg.Credentials = credentials.NewStaticCredentials(aws_key, aws_secret, "")
		} else {
			cfg.Credentials = credentials.NewSharedCredentials("", "default")
		}

		sess := session.Must(session.NewSession(cfg))
		svc := s3.New(sess)
		adapter := lakefsS3.NewAdapter(svc, lakefsS3.WithTranslator(translator))
		return adapter
	}
}

func Must(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("error returned for operation: %v", err)
	}
}
