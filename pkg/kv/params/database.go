package params

import (
	"time"
)

type KV struct {
	Type     string
	Postgres *Postgres
	DynamoDB *DynamoDB
	Local    *Local
}

type Local struct {
	// Path - Local directory path to store the DB files
	Path string
	// SyncWrites - Sync ensures data written to disk on each write instead of mem cache
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
	ScanPageSize          int32
	Metrics               bool
}

type DynamoDB struct {
	// The name of the DynamoDB table to be used as KV
	TableName string

	// Table provisioned throughput parameters, as described in
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
	ReadCapacityUnits  int64
	WriteCapacityUnits int64

	// Maximal number of items per page during scan operation
	ScanLimit int64

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
	AwsRegion          string
	AwsProfile         string
	AwsAccessKeyID     string
	AwsSecretAccessKey string
}
