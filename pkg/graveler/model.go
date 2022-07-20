package graveler

import (
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	gravelerPartition = "graveler"
	reposPrefix       = "repos"
	tagsPrefix        = "tags"
	commitsPrefix     = "commits"
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

func CommitPath(commitID CommitID) string {
	return kv.FormatPath(commitsPrefix, commitID.String())
}

func CommitFromProto(pb *CommitData) *Commit {
	var parents []CommitID
	for _, parent := range pb.Parents {
		parents = append(parents, CommitID(parent))
	}

	return &Commit{
		Version:      CommitVersion(pb.Version),
		Committer:    pb.Committer,
		Message:      pb.Message,
		MetaRangeID:  MetaRangeID(pb.MetaRangeId),
		CreationDate: pb.CreationDate.AsTime(),
		Parents:      parents,
		Metadata:     pb.Metadata,
		Generation:   int(pb.Generation),
	}
}

func ProtoFromCommit(commitID CommitID, c *Commit) *CommitData {
	// convert parents to slice of strings
	var parents []string
	for _, parent := range c.Parents {
		parents = append(parents, string(parent))
	}

	return &CommitData{
		Id:           string(commitID),
		Committer:    c.Committer,
		Message:      c.Message,
		CreationDate: timestamppb.New(c.CreationDate),
		MetaRangeId:  string(c.MetaRangeID),
		Metadata:     c.Metadata,
		Parents:      parents,
		Version:      int32(c.Version),
		Generation:   int32(c.Generation),
	}
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
