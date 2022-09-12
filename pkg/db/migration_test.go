package db_test

import (
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"testing"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestMigrations(t *testing.T) {
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	viper.Set(config.DatabaseType, kvpg.DriverName)

	databaseURI, closer := testutil.GetDBInstance(pool)
	defer closer()
	viper.Set("database.connection_string", databaseURI)
	viper.Set("database.postgres.connection_string", databaseURI)

	cfg, err := config.NewConfig()
	if err != nil {
		t.Fatal("creating config:", err)
	}
	err = db.MigrateUp(cfg.GetDatabaseParams(), cfg, cfg.GetKVParams())
	if err != nil {
		t.Fatal("failed running migrate up:", err)
	}
	err = db.MigrateDown(params.Database{ConnectionString: databaseURI})
	if err != nil {
		t.Fatal("failed running migrate down", err)
	}
}
