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
	cfg, err := config.NewConfig()
	if err != nil {
		t.Fatal("creating config:", err)
	}
	databaseURI, closer := testutil.GetDBInstance(pool)
	defer closer()
	err = db.MigrateUp(params.Database{ConnectionString: databaseURI}, cfg)
	if err != nil {
		t.Fatal("failed running migrate up:", err)
	}
	err = db.MigrateDown(params.Database{ConnectionString: databaseURI})
	if err != nil {
		t.Fatal("failed running migrate down", err)
	}
}
