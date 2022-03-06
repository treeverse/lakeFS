package cmd_test

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd"

	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/mock"
)

func TestExtractRepoAndBranchFromDBName(t *testing.T) {
	type args struct {
		DBName string
	}
	type extractRepoAndBranchFromDBNameTestCase struct {
		Name           string
		Args           args
		RepositoryName string
		BranchName     string
		Err            error
		Path           string
	}
	const (
		dbName      = "source"
		repoName    = "repo"
		branchName  = "branch"
		validPath   = "s3://" + repoName + "/" + branchName + "/path/to/schema"
		invalidPath = "s3/" + repoName + "/" + branchName + "/"
	)
	tests := []extractRepoAndBranchFromDBNameTestCase{
		{
			Name: "Sunny day flow",
			Args: args{
				DBName: dbName,
			},
			RepositoryName: repoName,
			BranchName:     branchName,
			Err:            nil,
			Path:           validPath,
		},
		{
			Name: "Client with no database",
			Args: args{
				DBName: "db Name that doesn't exist",
			},
			RepositoryName: "",
			BranchName:     "",
			Err:            mock.ErrNotFound,
			Path:           validPath,
		},
		{
			Name: "Failed getting repo and branch from uri",
			Args: args{
				DBName: dbName,
			},
			RepositoryName: "",
			BranchName:     "",
			Err:            metastore.ErrInvalidLocation,
			Path:           invalidPath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			client := NewMockClient(t, tt.Path, dbName)
			repo, branch, err := cmd.ExtractRepoAndBranchFromDBName(context.Background(), tt.Args.DBName, client)
			if err != nil && !errors.Is(err, tt.Err) {
				t.Errorf("ExtractRepoAndBranchFromDBName() error = %v, Err %v", err, tt.Err)
				return
			}
			if err == nil && tt.Err != nil {
				t.Errorf("ExtractRepoAndBranchFromDBName() repo = %v, expected error %v but got none", repo, tt.Err)
				return
			}
			if repo != tt.RepositoryName {
				t.Errorf("ExtractRepoAndBranchFromDBName() repo = %v, RepositoryName %v", repo, tt.RepositoryName)
			}
			if branch != tt.BranchName {
				t.Errorf("ExtractRepoAndBranchFromDBName() branch = %v, BranchName %v", branch, tt.BranchName)
			}
		})
	}
}

func NewMockClient(t *testing.T, schemaPath, dbName string) *mock.MSClient {
	initialDatabases := make(map[string]*metastore.Database)
	initialDatabases[dbName] = &metastore.Database{Name: dbName, LocationURI: schemaPath}
	return mock.NewMSClient(t, initialDatabases, nil, nil)
}
