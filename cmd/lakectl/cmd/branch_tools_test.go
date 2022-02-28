package cmd

import (
	"context"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/mock"
)

const (
	dbName     = "source"
	repoName   = "repo"
	branchName = "branch"
)

var (
	validPath   = fmt.Sprintf("s3://%s/%s/path/to/schema", repoName, branchName)
	invalidPath = fmt.Sprintf("s3/%s/%s/", repoName, branchName)
)

func TestExtractRepoAndBranchFromDBName(t *testing.T) {
	type args struct {
		ctx    context.Context
		dbName string
	}
	tests := []struct {
		name      string
		args      args
		want      string
		want1     string
		wantErr   bool
		validPath bool
	}{
		{
			name: "Happy flow",
			args: args{
				ctx:    nil,
				dbName: dbName,
			},
			want:      repoName,
			want1:     branchName,
			wantErr:   false,
			validPath: true,
		},
		{
			name: "Client with no database",
			args: args{
				ctx:    nil,
				dbName: "db name that doesn't exist",
			},
			want:      "",
			want1:     "",
			wantErr:   true,
			validPath: true,
		},
		{
			name: "Failed getting repo and branch from uri",
			args: args{
				ctx:    nil,
				dbName: dbName,
			},
			want:      "",
			want1:     "",
			wantErr:   true,
			validPath: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var schemaPath string
			if tt.validPath {
				schemaPath = validPath
			} else {
				schemaPath = invalidPath
			}
			client := initializeMockClient(t, schemaPath)
			got, got1, err := ExtractRepoAndBranchFromDBName(tt.args.ctx, tt.args.dbName, client)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRepoAndBranchFromDBName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractRepoAndBranchFromDBName() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ExtractRepoAndBranchFromDBName() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGenerateLakeFSBranchURIFromRepoAndBranchName(t *testing.T) {
	expected := fmt.Sprintf("lakefs://%s/%s", repoName, branchName)
	if got := GenerateLakeFSBranchURIFromRepoAndBranchName(repoName, branchName); got != expected {
		t.Errorf("GenerateLakeFSBranchURIFromRepoAndBranchName() = %v, want %v", got, expected)
	}
}

func initializeMockClient(t *testing.T, schemaPath string) *mock.MSClient {
	initialDatabases := make(map[string]*metastore.Database)
	initialDatabases[dbName] = &metastore.Database{Name: dbName, LocationURI: schemaPath}
	return mock.NewMSClient(t, initialDatabases, nil, nil)
}
