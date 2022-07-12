package graveler

import (
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	gravelerPartition = "graveler"
	reposPrefix       = "repos"
	tagsPrefix        = "tags"
)

func RepoPath(repoID RepositoryID) string {
	return kv.FormatPath(reposPrefix, repoID.String())
}

func RepoPartition() string {
	return gravelerPartition
}

func TagPath(tagID TagID) string {
	return kv.FormatPath(tagsPrefix, tagID.String())
}

func TagPartition(repoID RepositoryID) string {
	return repoID.String()
}

func RepoFromProto(pb *RepositoryData) *RepositoryRecord {
	return &RepositoryRecord{
		RepositoryID: RepositoryID(pb.Id),
		Repository: &Repository{
			StorageNamespace: StorageNamespace(pb.StorageNamespace),
			DefaultBranchID:  BranchID(pb.DefaultBranchId),
			CreationDate:     pb.CreationDate.AsTime(),
		},
	}
}

func ProtoFromRepo(repo *RepositoryRecord) *RepositoryData {
	return &RepositoryData{
		Id:               repo.RepositoryID.String(),
		StorageNamespace: repo.Repository.StorageNamespace.String(),
		DefaultBranchId:  repo.Repository.DefaultBranchID.String(),
		CreationDate:     timestamppb.New(repo.Repository.CreationDate),
	}
}
