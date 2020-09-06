package catalog

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	"testing"
)

func TestCataloger_CreateMergeJob(t *testing.T) {
	ctx := context.Background()
	conn, _ := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public")
	cdb := db.NewSqlxDatabase(conn)
	//conf := config.NewConfig()
	c := NewCataloger(cdb)
	//c := NewCataloger(cdb, conf.GetBatchReadParams())
	err := c.CreateMergeJob(ctx, int64(3), CommitID(3), CommitID(2))
	_ = err

}
