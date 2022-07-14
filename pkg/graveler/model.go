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

// RepositoriesPartition - The common partition under which all repositories exist
func RepositoriesPartition() string {
	return gravelerPartition
}

// RepoPartition - The partition under which all the repository's entities (branched, commits, tags)
// The Repository object itself is found under the common RepositoriesPartition, as it is needed to
// generate this partition
func RepoPartition(repoID RepositoryID, _ Repository) string {
	return repoID.String()
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
