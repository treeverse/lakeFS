package db

import (
	"context"

	// These 2 lines are the ones registering the stores to lakeFS
	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	_ "github.com/treeverse/lakefs/pkg/kv/postgres"
)

type Migrator interface {
	Migrate(ctx context.Context) error
}
