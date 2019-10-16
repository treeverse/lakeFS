package store

import (
	"versio-index/db"
	"versio-index/index/model"

	"github.com/golang/protobuf/proto"
)

type ClientReadOnlyOperations interface {
	ListRepos() ([]*model.Repo, error)
	ReadRepo(repoId string) (*model.Repo, error)
}

type ClientOperations interface {
	ClientReadOnlyOperations
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
