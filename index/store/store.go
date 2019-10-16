package store

import (
	"versio-index/db"
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	SubspaceRepos     = "repos"
	SubspaceWorkspace = "workspace"
	SubspaceEntries   = "entries"
	SubspaceObjects   = "objects"
	SubspaceCommits   = "commits"
	SubspaceBranches  = "branches"
	SubspaceRefCounts = "refCounts"
)

type Store interface {
	ClientReadTransact(clientId string, fn func(ops ClientReadOnlyOperations) (interface{}, error)) (interface{}, error)
	ClientTransact(clientId string, fn func(ops ClientOperations) (interface{}, error)) (interface{}, error)
	RepoReadTransact(*model.Repo, func(ops RepoReadOnlyOperations) (interface{}, error)) (interface{}, error)
	RepoTransact(*model.Repo, func(ops RepoOperations) (interface{}, error)) (interface{}, error)
}

type KVStore struct {
	kv db.Store
}

func (s *KVStore) ClientReadTransact(clientId string, fn func(ops ClientReadOnlyOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.ReadTransact(tuple.Tuple{clientId}, func(q db.ReadQuery) (interface{}, error) {
		return fn(&KVClientReadOnlyOperations{
			query: q,
			store: s.kv,
		})
	})
}

func (s *KVStore) ClientTransact(clientId string, fn func(ops ClientOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.Transact(tuple.Tuple{clientId}, func(q db.Query) (interface{}, error) {
		return fn(&KVClientOperations{
			KVClientReadOnlyOperations: &KVClientReadOnlyOperations{
				query: q,
				store: s.kv,
			},
			query: q,
		})
	})
}

func (s *KVStore) RepoReadTransact(repo *model.Repo, fn func(ops RepoReadOnlyOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.ReadTransact(tuple.Tuple{repo.GetClientId(), repo.GetRepoId()}, func(q db.ReadQuery) (interface{}, error) {
		return fn(&KVRepoReadOnlyOperations{
			query: q,
			store: s.kv,
		})
	})
}

func (s *KVStore) RepoTransact(repo *model.Repo, fn func(ops RepoOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.Transact(tuple.Tuple{repo.GetClientId(), repo.GetRepoId()}, func(q db.Query) (interface{}, error) {
		return fn(&KVRepoOperations{
			KVRepoReadOnlyOperations: &KVRepoReadOnlyOperations{
				query: q,
				store: s.kv,
			},
			query: q,
		})
	})
}
