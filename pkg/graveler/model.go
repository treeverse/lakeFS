package graveler

import (
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	tagsPrefix    = "tags"
	commitsPrefix = "commits"
)

func TagPath(tagID TagID) string {
	return kv.FormatPath(tagsPrefix, tagID.String())
}

func TagPartition(repoID RepositoryID) string {
	return repoID.String()
}

func CommitPath(commitID CommitID) string {
	return kv.FormatPath(commitsPrefix, commitID.String())
}

func CommitPartition(repoID RepositoryID) string {
	return repoID.String()
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
