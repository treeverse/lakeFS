package mvcc_model_test

//postgres://tzahij:sa@localhost:32768/mvcc_test?sslmode=disable
import (
	"fmt"
	"strconv"
	"testing"

	_ "github.com/jackc/pgx"
	_ "github.com/jackc/pgx/pgtype"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

const (
	RowsInTransaction = 10_000
	RowNumber         = 146_000_000
)

func checkErr(err error, msg string) {
	if err != nil {
		log.WithError(err).Fatal(msg)
	}
}
func TestDb(t *testing.T) {
	conn, err := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_test?sslmode=disable")
	checkErr(err, "failed connect to DB")
	tx, err := conn.Beginx()
	checkErr(err, "failed start transaction")
	_ = tx.Rollback()

	//generateEntries(conn, 1, RowNumber, 1, 100, 3)
	//generateEntries(conn, 2, RowNumber, 5, 100, 2)
	//generateEntries(conn, 3, RowNumber, 13, 100, 3)
	generateEntries(conn, 5, RowNumber, 39, 100, 3)
}

func generateEntries(conn *sqlx.DB, branch, maxNum, jump, step, commitRange int) {
	tx, err := conn.Beginx()
	checkErr(err, "failed initial start transactin branch  "+strconv.Itoa(branch))
	for i := 0; i < maxNum; i += jump {
		key := fmt.Sprintf("%03d/%06d/%08d", i%step, i/step, i)
		err := insertEntry(tx, key, branch, i%commitRange+1, i)
		checkErr(err, "failed inserting "+key)
		if i%RowsInTransaction == 0 {
			log.WithField("branch", branch).WithField("number", i).Info("got to point")
			err = tx.Commit()
			checkErr(err, "failed commit "+strconv.Itoa(i))
			tx, err = conn.Beginx()
			checkErr(err, "failed start transaction "+strconv.Itoa(i))
		}
	}
	err = tx.Commit()
	checkErr(err, "failed final commit branch "+strconv.Itoa(branch))
}

func insertEntry(tx *sqlx.Tx, key string, branch, startCommit, size int) error {
	rng := fmt.Sprintf("[%d,)", startCommit)

	_, err := tx.Exec(` INSERT INTO entries (branch_id, key, commits, physical_address, size, checksum)
			VALUES ($1, $2, $3, $4, $5, $6)`,
		branch, key, rng, "12af6783409", size, "8730975ab40")
	return err
}
