package retention

import (
	"context"
	"encoding/csv"
	"io"
	"strings"

	"github.com/treeverse/lakefs/pkg/block"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type CommitSetWriter struct {
	ctx   context.Context
	block block.Adapter
}

func NewCommitSetWriter(block block.Adapter) *CommitSetWriter {
	return &CommitSetWriter{block: block}
}

func write(commitIDs map[graveler.CommitID]bool, writer *io.PipeWriter, isExpired bool) error {
	csvExpiredWriter := csv.NewWriter(writer)
	for commitID := range commitIDs {
		err := csvExpiredWriter.Write([]string{string(commitID), isExpired})
		if err != nil {
			return err
		}
	}
	csvExpiredWriter.Flush()
	return writer.Close()
}

func (c *CommitSetWriter) Write(commits *Commits) error {
	b := &strings.Builder{}
	csv.NewWriter(b)

	c.block.UploadPart().Put(c.ctx, &block.ObjectPointer{
		StorageNamespace: "",
		Identifier:       "",
		IdentifierType:   block.IdentifierTypeFull,
	})
	err := write(commits.Expired, c.expiredWriter)
	if err != nil {
		return err
	}
	return write(commits.Active, c.activeWriter)
}
