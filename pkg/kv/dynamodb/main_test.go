package dynamodb_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	dsn string
)

func TestMain(m *testing.M) {
	databaseURI, cleanpFunc, err := testutil.GetDynamoDBInstance()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	testParams := &dynamodb.Params{
		TableName:          testutil.UniqueKVTableName(),
		ReadCapacityUnits:  100,
		WriteCapacityUnits: 100,
		ScanLimit:          10,
		Endpoint:           databaseURI,
		AwsRegion:          "us-east-1",
		AwsAccessKeyID:     "fakeMyKeyId",
		AwsSecretAccessKey: "fakeSecretAccessKey",
	}

	dsnBytes, err := json.Marshal(testParams)
	if err != nil {
		log.Fatalf("Failed to initalize tests params :%s", err)
	}

	dsn = string(dsnBytes)

	code := m.Run()
	cleanpFunc()
	os.Exit(code)
}
