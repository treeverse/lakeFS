package db_test

import (
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"testing"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestMigrations(t *testing.T) {
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	databaseURI, closer := testutil.GetDBInstance(pool)
	defer closer()
	err := db.MigrateUp(params.Database{ConnectionString: databaseURI}, nil)
	if err != nil {
		t.Fatal("failed running migrate up:", err)
	}
	err = db.MigrateDown(params.Database{ConnectionString: databaseURI})
	if err != nil {
		t.Fatal("failed running migrate down", err)
	}
}
