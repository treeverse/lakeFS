package dynamodb_test

import (
	"log"
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	databaseURI string
)

func TestMain(m *testing.M) {
	dbURI, cleanpFunc, err := testutil.RunLocalDynamoDBInstance("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseURI = dbURI
	code := m.Run()
	cleanpFunc()
	os.Exit(code)
}
