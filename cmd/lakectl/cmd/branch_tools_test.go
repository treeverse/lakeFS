package cmd

import (
	"context"
	"errors"
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

type extractRepoAndBranchFromDBNameTestCase struct {
	name string
	args struct {
		dbName string
	}
	repositoryName string
	branchName     string
	errType        error
	validPath      bool
}

func TestExtractRepoAndBranchFromDBName(t *testing.T) {
	type args struct {
		dbName string
	}
	tests := []extractRepoAndBranchFromDBNameTestCase{
		{
			name: "Sunny day flow",
			args: args{
				dbName: dbName,
			},
			repositoryName: repoName,
			branchName:     branchName,
			errType:        nil,
			validPath:      true,
		},
		{
			name: "Client with no database",
			args: args{
				dbName: "db name that doesn't exist",
			},
			repositoryName: "",
			branchName:     "",
			errType:        MissingDBError{},
			validPath:      true,
		},
		{
			name: "Failed getting repo and branch from uri",
			args: args{
				dbName: dbName,
			},
			repositoryName: "",
			branchName:     "",
			errType:        ExtractSourceBranchError{},
			validPath:      false,
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
			got, got1, err := ExtractRepoAndBranchFromDBName(context.Background(), tt.args.dbName, client)
			if (err != nil) && errors.Is(err, tt.errType) {
				t.Errorf("ExtractRepoAndBranchFromDBName() error = %v, errType %v", err, tt.errType)
				return
			}
			if got != tt.repositoryName {
				t.Errorf("ExtractRepoAndBranchFromDBName() got = %v, repositoryName %v", got, tt.repositoryName)
			}
			if got1 != tt.branchName {
				t.Errorf("ExtractRepoAndBranchFromDBName() got1 = %v, repositoryName %v", got1, tt.branchName)
			}
		})
	}
}

func initializeMockClient(t *testing.T, schemaPath string) *mock.MSClient {
	initialDatabases := make(map[string]*metastore.Database)
	initialDatabases[dbName] = &metastore.Database{Name: dbName, LocationURI: schemaPath}
	return mock.NewMSClient(t, initialDatabases, nil, nil)
}

type generateLakeFSBranchURIFromRepoAndBranchNameTestCase struct {
	name string
	args struct {
		repoName   string
		branchName string
	}
	lakeFSURI string
	wantErr   bool
}

func TestGenerateLakeFSBranchURIFromRepoAndBranchName(t *testing.T) {
	type args struct {
		repoName   string
		branchName string
	}
	tests := []generateLakeFSBranchURIFromRepoAndBranchNameTestCase{
		{
			name: "Sunny day flow",
			args: args{
				repoName:   repoName,
				branchName: branchName,
			},
			lakeFSURI: fmt.Sprintf("lakefs://%s/%s", repoName, branchName),
			wantErr:   false,
		},
		{
			name: "Empty repo",
			args: args{
				repoName:   "",
				branchName: branchName,
			},
			lakeFSURI: "",
			wantErr:   true,
		},
		{
			name: "Empty brnach",
			args: args{
				repoName:   repoName,
				branchName: "",
			},
			lakeFSURI: "",
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateLakeFSBranchURIFromRepoAndBranchName(tt.args.repoName, tt.args.branchName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateLakeFSBranchURIFromRepoAndBranchName() error = %v, errType %v", err, tt.wantErr)
				return
			}
			if got != tt.lakeFSURI {
				t.Errorf("GenerateLakeFSBranchURIFromRepoAndBranchName() got = %v, repositoryName %v", got, tt.lakeFSURI)
			}
		})
	}
}
