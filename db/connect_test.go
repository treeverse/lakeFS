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
		{name: "basic", args: args{"pgx", databaseURI}, wantErr: false},
		{name: "unknown driver", args: args{"bad", databaseURI}, wantErr: true},
		{name: "invalid address", args: args{"file", "bad://place/db&search_path=lakefs_stam"}, wantErr: true},
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
}
