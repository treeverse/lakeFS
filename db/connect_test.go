package db_test

import (
	"testing"

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
		})
	}
}
