package db_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestMigrations(t *testing.T) {
	databaseURI, closer := testutil.GetDBInstance(pool)
	defer closer()
	err := db.MigrateUp(params.Database{ConnectionString: databaseURI})
	if err != nil {
		t.Fatal("failed running migrate up:", err)
	}
	err = db.MigrateDown(params.Database{ConnectionString: databaseURI})
	if err != nil {
		t.Fatal("failed running migrate down", err)
	}
}
