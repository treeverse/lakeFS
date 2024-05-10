package kvparams

import (
	"fmt"
	"net/http"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/treeverse/lakefs/pkg/config"
)

type Config struct {
	Type     string
	Postgres *Postgres
	DynamoDB *DynamoDB
	Local    *Local
	CosmosDB *CosmosDB
}

type Local struct {
	// Path - Local directory path to store the DB files
	Path string
	// SyncWrites - Sync ensures data written to disk on each writing instead of mem cache
	SyncWrites bool
	// PrefetchSize - Number of elements to prefetch while iterating
	PrefetchSize int
	// EnableLogging - Enable store and badger (trace only) logging
	EnableLogging bool
}

type Postgres struct {
	ConnectionString      string
	MaxOpenConnections    int32
	MaxIdleConnections    int32
	ConnectionMaxLifetime time.Duration
	ScanPageSize          int
	Metrics               bool
}

type DynamoDB struct {
	// The name of the DynamoDB table to be used as KV
	TableName string

	// Maximal number of items per page during scan operation
	ScanLimit int64

	// Specifies the maximum number attempts to make on a request.
	MaxAttempts int

	// The endpoint URL of the DynamoDB endpoint
	// Can be used to redirect to DynamoDB on AWS, local docker etc.
	Endpoint string

	// AWS connection details - region, profile and credentials
	// This will override any such details that are already exist in the system
	// While in general, AWS region and credentials are configured in the system for AWS usage,
	// these can be used to specify fake values, that cna be used to connect to local DynamoDB,
	// in case there are no credentials configured in the system
	// This is a client requirement as described in section 4 in
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
	AwsRegion             string
	AwsProfile            string
	AwsAccessKeyID        string
	AwsSecretAccessKey    string
	HealthCheckInterval   time.Duration
	MaxConnectionsPerHost int
}

type CosmosDB struct {
	Key        string
	Endpoint   string
	Database   string
	Container  string
	Throughput int32
	Autoscale  bool
	// These values should only be set to false for testing purposes using the CosmosDB emulator
	Client            *http.Client
	StrongConsistency bool
}

func NewConfig(cfg *config.Config) (Config, error) {
	p := Config{
		Type: cfg.Database.Type,
	}
	if cfg.Database.Local != nil {
		localPath, err := homedir.Expand(cfg.Database.Local.Path)
		if err != nil {
			return Config{}, fmt.Errorf("parse database local path '%s': %w", cfg.Database.Local.Path, err)
		}
		p.Local = &Local{
			Path:         localPath,
			PrefetchSize: cfg.Database.Local.PrefetchSize,
		}
	}

	if cfg.Database.Postgres != nil {
		p.Postgres = &Postgres{
			ConnectionString:      cfg.Database.Postgres.ConnectionString.SecureValue(),
			MaxIdleConnections:    cfg.Database.Postgres.MaxIdleConnections,
			MaxOpenConnections:    cfg.Database.Postgres.MaxOpenConnections,
			ConnectionMaxLifetime: cfg.Database.Postgres.ConnectionMaxLifetime,
		}
	}

	if cfg.Database.DynamoDB != nil {
		p.DynamoDB = &DynamoDB{
			TableName:             cfg.Database.DynamoDB.TableName,
			ScanLimit:             cfg.Database.DynamoDB.ScanLimit,
			Endpoint:              cfg.Database.DynamoDB.Endpoint,
			AwsRegion:             cfg.Database.DynamoDB.AwsRegion,
			AwsProfile:            cfg.Database.DynamoDB.AwsProfile,
			AwsAccessKeyID:        cfg.Database.DynamoDB.AwsAccessKeyID.SecureValue(),
			AwsSecretAccessKey:    cfg.Database.DynamoDB.AwsSecretAccessKey.SecureValue(),
			HealthCheckInterval:   cfg.Database.DynamoDB.HealthCheckInterval,
			MaxAttempts:           cfg.Database.DynamoDB.MaxAttempts,
			MaxConnectionsPerHost: cfg.Database.DynamoDB.MaxConnections,
		}
	}

	if cfg.Database.CosmosDB != nil {
		if cfg.Database.CosmosDB.Autoscale && cfg.Database.CosmosDB.Throughput == 0 {
			return Config{}, fmt.Errorf("enabling autoscale requires setting the throughput param: %w", config.ErrBadConfiguration)
		}
		p.CosmosDB = &CosmosDB{
			Key:               cfg.Database.CosmosDB.Key.SecureValue(),
			Endpoint:          cfg.Database.CosmosDB.Endpoint,
			Database:          cfg.Database.CosmosDB.Database,
			Container:         cfg.Database.CosmosDB.Container,
			Throughput:        cfg.Database.CosmosDB.Throughput,
			Autoscale:         cfg.Database.CosmosDB.Autoscale,
			StrongConsistency: true,
		}
	}

	return p, nil
}
