package db_test

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
)

func TestConnectDB(t *testing.T) {
	type args struct {
		driver string
		uri    string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "no schema", args: args{"pgx", databaseURI}, wantErr: true},
		{name: "known schema", args: args{"pgx", databaseURI + "&search_path=lakefs_index"}, wantErr: false},
		{name: "unknown driver", args: args{"bad", databaseURI + "&search_path=lakefs_index"}, wantErr: true},
		{name: "unknown schema", args: args{"pgx", databaseURI + "&search_path=lakefs_stam"}, wantErr: true},
		{name: "invalid address with known schema", args: args{"file", "bad://place/db&search_path=lakefs_stam"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.ConnectDB(tt.args.driver, tt.args.uri)
			if (err != nil) && !tt.wantErr {
				t.Errorf("ConnectDB() error = %v, unexpected error", err)
				return
			}
			if (err == nil) && got == nil {
				t.Errorf("ConnectDB() got no database instance when expected")
			}
			if err != nil && got != nil {
				_ = got.Close()
			}
		})
	}

	t.Run("collate", func(t *testing.T) {
		// create database with non "C" collate
		conn, err := sqlx.Connect("pgx", databaseURI)
		if err != nil {
			t.Fatal("Failed to connect to DB", err)
		}
		defer conn.Close()
		const testDBName = dbName + "_non_c"
		_, err = conn.Exec("CREATE DATABASE " + testDBName + " LC_COLLATE='en_US.utf8' TEMPLATE template0")
		if err != nil {
			t.Fatal("Failed to create non C collate database", err)
		}
		testDBURI := strings.Replace(databaseURI, dbName, testDBName, 1)
		idxDB, err := db.ConnectDB("pgx", testDBURI+"&search_path=lakefs_index")
		if err == nil {
			t.Error("Connect to database with unexpected collate should fail")
		}
		if idxDB != nil {
			_ = idxDB.Close()
		}
	})
}
