package params

import (
	"time"
)

type KV struct {
	Type     string
	Postgres *Postgres
	DynamoDB *DynamoDB
}

type Postgres struct {
	ConnectionString      string
	MaxOpenConnections    int32
	MaxIdleConnections    int32
	ConnectionMaxLifetime time.Duration
	ScanPageSize          int32
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
	// Can be used to redirect to DynmoDB on AWS, local docker etc.
	Endpoint string

	// AWS connection details - region and credentials
	// This will override any such details that are already exist in the system
	// While in general, AWS region and credentials are configured in the system for AWS usage,
	// these can be used to specify fake values, that cna be used to connect to local DynamoDB,
	// in case there are no credentials configured in the system
	// This is a client requirement as described in section 4 in
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
	AwsRegion          string
	AwsAccessKeyID     string
	AwsSecretAccessKey string
}
