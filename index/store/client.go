package store

import (
	"treeverse-lake/db"
	"treeverse-lake/index/model"

	"github.com/golang/protobuf/proto"
)

type ClientReadOnlyOperations interface {
	ListRepos() ([]*model.Repo, error)
	ReadRepo(repoId string) (*model.Repo, error)
}

type ClientOperations interface {
	ClientReadOnlyOperations
	DeleteRepo(repoId string)
}

type KVClientReadOnlyOperations struct {
	query db.ReadQuery
	store db.Store
}

type KVClientOperations struct {
	*KVClientReadOnlyOperations
	query db.Query
}

func (c *KVClientReadOnlyOperations) ReadRepo(repoId string) (*model.Repo, error) {
	repo := &model.Repo{}
	return repo, c.query.GetAsProto(repo, c.store.Space(SubspaceRepos), repoId)
}

func (c *KVClientReadOnlyOperations) ListRepos() ([]*model.Repo, error) {
	repos := make([]*model.Repo, 0)
	iter := c.query.RangePrefix(c.store.Space(SubspaceRepos))
	for iter.Advance() {
		kv := iter.MustGet()
		repo := &model.Repo{}
		err := proto.Unmarshal(kv.Value, repo)
		if err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, nil
}

func (c *KVClientOperations) DeleteRepo(repoId string) {
	// clear all workspaces, branches, entries, etc.
	c.query.ClearPrefix(c.store.Space(SubspaceWorkspace), repoId)
	c.query.ClearPrefix(c.store.Space(SubspaceBranches), repoId)
	c.query.ClearPrefix(c.store.Space(SubspaceCommits), repoId)
	c.query.ClearPrefix(c.store.Space(SubspaceEntries), repoId)
	c.query.ClearPrefix(c.store.Space(SubspaceObjects), repoId)
	c.query.ClearPrefix(c.store.Space(SubspaceRefCounts), repoId)
	c.query.ClearPrefix(c.store.Space(SubspaceRepos), repoId)
}
