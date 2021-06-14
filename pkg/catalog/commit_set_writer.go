package catalog

//
//import (
//	"context"
//	"encoding/csv"
//	"strconv"
//	"strings"
//
//	"github.com/treeverse/lakefs/pkg/block"
//)
//
//type CommitSetWriter struct {
//	ctx   context.Context
//	block block.Adapter
//}
//
//func NewCommitSetWriter(block block.Adapter) *CommitSetWriter {
//	return &CommitSetWriter{block: block}
//}
//
//func (c *CommitSetWriter) Write(commits *retention.Commits, pointer *block.ObjectPointer) error {
//	b := &strings.Builder{}
//	csvWriter := csv.NewWriter(b)
//	for commitID := range commits.Expired {
//		err := csvWriter.Write([]string{string(commitID), strconv.FormatBool(true)})
//		if err != nil {
//			return err
//		}
//	}
//	for commitID := range commits.Active {
//		err := csvWriter.Write([]string{string(commitID), strconv.FormatBool(false)})
//		if err != nil {
//			return err
//		}
//	}
//	commitsStr := b.String()
//	return c.block.Put(c.ctx, *pointer, int64(len(commitsStr)), strings.NewReader(commitsStr), block.PutOpts{})
//}
