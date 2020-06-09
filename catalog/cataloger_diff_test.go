package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/treeverse/lakefs/db"
)

//func Must(err error, t *testing.T, message string) {
//	if err != nil {
//		t.Fatal(message)
//	}
//}
func TestCataloger_Diff(t *testing.T) {
	ctx := context.Background()
	//cdb, _ := testutil.GetDB(t, "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public", "lakefs_catalog")
	conn, _ := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public")
	cdb := db.NewSqlxDatabase(conn)
	c := NewCataloger(cdb)

	t.Run("simple write", func(t *testing.T) {
		diff, err := c.Diff(ctx, "example", "m1", "m2")
		fmt.Print(err)
		fmt.Print(diff)

	})

}
