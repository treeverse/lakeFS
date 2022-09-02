package db_test

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
)

func getDB(t *testing.T) db.Database {
	t.Helper()
	ctx := context.Background()
	ret, err := db.ConnectDB(ctx, params.Database{Type: "postgres", Driver: "pgx", ConnectionString: databaseURI})
	if err != nil {
		t.Fatal("failed to get DB")
	}
	return ret
}

func TestGetPrimitive(t *testing.T) {
	d := getDB(t)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		ret, err := d.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var i int64
			err := tx.GetPrimitive(&i, "SELECT 17")
			return i, err
		})

		if err != nil {
			t.Errorf("failed to SELECT 17: %s", err)
		}
		i := ret.(int64)
		if i != 17 {
			t.Errorf("got %d not 17 from SELECT 17", i)
		}
	})

	t.Run("failure", func(t *testing.T) {
		_, err := d.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var i int64
			err := tx.GetPrimitive(&i, "SELECT 17 WHERE 2=1")
			return i, err
		})

		if !errors.Is(err, db.ErrNotFound) {
			t.Errorf("got %s wanted not found", err)
		}
	})
}

func TestGet(t *testing.T) {
	type R struct {
		A int64
		B string
	}

	d := getDB(t)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		ret, err := d.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var r R
			err := tx.Get(&r, "SELECT 17 A, 'foo' B")
			return &r, err
		})

		if err != nil {
			t.Errorf("failed to SELECT 17 and 'foo': %s", err)
		}
		r := ret.(*R)
		if r.A != 17 {
			t.Errorf("got %+v with A != 17 from SELECT 17 and 'foo'", r)
		}
		if r.B != "foo" {
			t.Errorf("got %+v with B != 'foo' from SELECT 17 and 'foo'", r)
		}
	})

	t.Run("failure", func(t *testing.T) {
		_, err := d.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var r R
			err := tx.Get(&r, "SELECT 17 A, 'foo' B WHERE 2=1")
			return &r, err
		})

		if !errors.Is(err, db.ErrNotFound) {
			t.Errorf("got %s wanted not found", err)
		}
	})

	t.Run("failure unique", func(t *testing.T) {
		_, err := d.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var r R
			_ = tx.Get(&r, `CREATE TABLE test_failure_unique (ID int NOT NULL UNIQUE)`)
			_ = tx.Get(&r, `INSERT INTO test_failure_unique VALUES (1)`)
			err := tx.Get(&r, `INSERT INTO test_failure_unique VALUES (1)`)
			return &r, err
		})

		if !errors.Is(err, db.ErrAlreadyExists) {
			t.Errorf("got: %s expected: %s", err, db.ErrAlreadyExists)
		}
	})
}
