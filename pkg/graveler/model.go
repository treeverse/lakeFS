package graveler

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	gravelerPartition      = "graveler"
	cleanupTokensPartition = "cleanup-tokens"
	reposPrefix            = "repos"
	tagsPrefix             = "tags"
	branchesPrefix         = "branches"
	commitsPrefix          = "commits"
	settingsPrefix         = "settings"
	addressesPrefix        = "link-addresses"
)

//nolint:gochecknoinits
func init() {
	kv.MustRegisterType("graveler", "repos", (&RepositoryData{}).ProtoReflect().Type())
	kv.MustRegisterType("*", "branches", (&BranchData{}).ProtoReflect().Type())
	kv.MustRegisterType("*", "commits", (&CommitData{}).ProtoReflect().Type())
	kv.MustRegisterType("*", "tags", (&TagData{}).ProtoReflect().Type())
	kv.MustRegisterType("*", "*", (&StagedEntryData{}).ProtoReflect().Type())
}

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
func RepoPartition(repo *RepositoryRecord) string {
	return fmt.Sprintf("%s-%s", repo.RepositoryID.String(), repo.InstanceUID)
}

func StagingTokenPartition(token StagingToken) string {
	return token.String()
}

func CleanupTokensPartition() string {
	return cleanupTokensPartition
}

func TagPath(tagID TagID) string {
	return kv.FormatPath(tagsPrefix, tagID.String())
}

func BranchPath(branchID BranchID) string {
	return kv.FormatPath(branchesPrefix, branchID.String())
}

func CommitPath(commitID CommitID) string {
	return kv.FormatPath(commitsPrefix, commitID.String())
}

func SettingsPath(key string) string {
	return kv.FormatPath(settingsPrefix, key)
}

func LinkedAddressPath(key string) string {
	return kv.FormatPath(addressesPrefix, key)
}

func CommitFromProto(pb *CommitData) *Commit {
	parents := make([]CommitID, 0)
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
	parents := make([]string, 0)
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
			InstanceUID:      pb.InstanceUid,
			State:            pb.State,
		},
	}
}

func ProtoFromRepo(repo *RepositoryRecord) *RepositoryData {
	return &RepositoryData{
		Id:               repo.RepositoryID.String(),
		StorageNamespace: repo.Repository.StorageNamespace.String(),
		DefaultBranchId:  repo.Repository.DefaultBranchID.String(),
		CreationDate:     timestamppb.New(repo.Repository.CreationDate),
		State:            repo.State,
		InstanceUid:      repo.InstanceUID,
	}
}

func StagedEntryFromProto(pb *StagedEntryData) *Value {
	return &Value{
		Identity: pb.Identity,
		Data:     pb.Data,
	}
}

func ProtoFromStagedEntry(key []byte, v *Value) *StagedEntryData {
	return &StagedEntryData{
		Key:      key,
		Identity: v.Identity,
		Data:     v.Data,
	}
}

func TagFromProto(pb *TagData) *TagRecord {
	return &TagRecord{
		TagID:    TagID(pb.Id),
		CommitID: CommitID(pb.CommitId),
	}
}
