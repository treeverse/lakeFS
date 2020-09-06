package jobs

import (
	"context"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	"testing"
	"time"
)

func TestJob_JobRunner(t *testing.T) {
	ctx := context.Background()
	conn, err := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public")
	cdb := db.NewSqlxDatabase(conn)
	//c := catalog.NewCataloger(cdb)
	_ = ctx
	err = InitJobActivator(cdb)
	if err != nil {

	}
	time.Sleep(time.Hour * 24)
}
