package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/sstable"
	"google.golang.org/protobuf/proto"
)

const sstCatTemplate = `{{ .Table | table }}`

var catSstCmd = &cobra.Command{
	Use:    "cat-sst <sst-file>",
	Short:  "Explore lakeFS .sst files",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		filePath := Must(cmd.Flags().GetString("file"))
		iter, props, err := getIterFromFile(filePath)
		if err != nil {
			DieErr(err)
		}
		defer iter.Close()

		// get props
		typ, ok := props[committed.MetadataTypeKey]
		if !ok {
			DieFmt("could not determine sstable file type")
		}

		var table *Table
		switch typ {
		case committed.MetadataMetarangesType:
			table, err = formatMetaRangeSSTable(iter, amount)
		case committed.MetadataRangesType:
			table, err = formatRangeSSTable(iter, amount, props[graveler.EntityTypeKey])
		default:
			DieFmt("unknown sstable file type: %s", typ)
		}
		if err != nil {
			DieErr(err)
		}

		// write to stdout
		Write(sstCatTemplate, struct {
			Table *Table
		}{table})
	},
}

func getIterFromFile(filePath string) (committed.ValueIterator, map[string]string, error) {
	// read from stdin (file has to be seekable/stat-able, so we have this weird wrapper
	file, err := OpenByPath(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()

	// read all content
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}

	// read file descriptor
	reader, err := pebblesst.NewMemReader(content, pebblesst.ReaderOptions{})
	if err != nil {
		return nil, nil, err
	}

	// create an iterator over the whole thing
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, nil, err
	}

	// wrap it in a Graveler iterator
	dummyDeref := func() error { return nil }
	return sstable.NewIterator(iter, dummyDeref), reader.Properties.UserProperties, nil
}

func formatEntryRangeSSTable(iter committed.ValueIterator, amount int) (*Table, error) {
	rows := make([][]interface{}, 0)
	for iter.Next() {
		if amount != -1 && len(rows)+1 > amount {
			break
		}
		v := iter.Value()
		gv, err := committed.UnmarshalValue(v.Value)
		if err != nil {
			panic(err)
		}
		ent, err := catalog.ValueToEntry(gv)
		if err != nil {
			return nil, err
		}
		rows = append(rows, []interface{}{
			string(v.Key),
			fmt.Sprintf("%x", gv.Identity),
			fmt.Sprint(ent.GetSize()),
			fmt.Sprint(ent.GetETag()),
			ent.GetLastModified().AsTime().String(),
			ent.GetAddress(),
			fmt.Sprint(ent.GetMetadata()),
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return &Table{
		Headers: []interface{}{"path", "identity", "size", "etag", "last_modified", "address", "metadata"},
		Rows:    rows,
	}, nil
}

func formatBranchRangeSSTable(iter committed.ValueIterator, amount int) (*Table, error) {
	rows := make([][]interface{}, 0)
	for iter.Next() {
		if amount != -1 && len(rows)+1 > amount {
			break
		}
		v := iter.Value()
		gv, err := committed.UnmarshalValue(v.Value)
		if err != nil {
			return nil, err
		}
		b := &graveler.BranchData{}
		err = proto.Unmarshal(gv.Data, b)
		if err != nil {
			return nil, err
		}
		rows = append(rows, []interface{}{b.Id, b.CommitId})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return &Table{
		Headers: []interface{}{"branch ID", "commit ID"},
		Rows:    rows,
	}, nil
}

func formatTagsRangeSSTable(iter committed.ValueIterator, amount int) (*Table, error) {
	rows := make([][]interface{}, 0)
	for iter.Next() {
		if amount != -1 && len(rows)+1 > amount {
			break
		}
		v := iter.Value()
		gv, err := committed.UnmarshalValue(v.Value)
		if err != nil {
			return nil, err
		}
		t := &graveler.TagData{}
		err = proto.Unmarshal(gv.Data, t)
		if err != nil {
			return nil, err
		}
		rows = append(rows, []interface{}{t.Id, t.CommitId})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return &Table{
		Headers: []interface{}{"tag ID", "commit ID"},
		Rows:    rows,
	}, nil
}

func formatCommitRangeSSTable(iter committed.ValueIterator, amount int) (*Table, error) {
	rows := make([][]interface{}, 0)
	for iter.Next() {
		if amount != -1 && len(rows)+1 > amount {
			break
		}
		v := iter.Value()
		gv, err := committed.UnmarshalValue(v.Value)
		if err != nil {
			return nil, err
		}
		c := &graveler.CommitData{}
		err = proto.Unmarshal(gv.Data, c)
		if err != nil {
			return nil, err
		}

		metadata := c.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, err
		}
		parents := c.Parents
		if parents == nil {
			parents = []string{}
		}
		parentsJSON, err := json.Marshal(parents)
		if err != nil {
			return nil, err
		}
		rows = append(rows, []interface{}{
			string(v.Key),
			c.Committer,
			c.Message,
			c.CreationDate.AsTime().Format(time.RFC3339),
			string(metadataJSON),
			string(parentsJSON),
			c.MetaRangeId,
			c.Version,
			c.Generation,
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return &Table{
		Headers: []interface{}{"commit ID", "committer", "message", "creation date", "metadata", "parents", "metarange ID", "version", "generation"},
		Rows:    rows,
	}, nil
}

func formatRangeSSTable(iter committed.ValueIterator, amount int, entityType string) (*Table, error) {
	switch entityType {
	case graveler.EntityTypeBranch:
		return formatBranchRangeSSTable(iter, amount)
	case graveler.EntityTypeCommit:
		return formatCommitRangeSSTable(iter, amount)
	case graveler.EntityTypeTag:
		return formatTagsRangeSSTable(iter, amount)
	default:
		return formatEntryRangeSSTable(iter, amount)
	}
}

func formatMetaRangeSSTable(iter committed.ValueIterator, amount int) (*Table, error) {
	rows := make([][]interface{}, 0)
	for iter.Next() {
		if amount != -1 && len(rows)+1 > amount {
			break
		}
		v := iter.Value()
		gv, err := committed.UnmarshalValue(v.Value)
		if err != nil {
			panic(err)
		}
		r, err := committed.UnmarshalRange(gv.Data)
		if err != nil {
			panic(err)
		}
		rows = append(rows, []interface{}{
			committed.ID(gv.Identity),
			string(r.MinKey),
			string(r.MaxKey),
			fmt.Sprint(r.Count),
			fmt.Sprint(r.EstimatedSize),
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return &Table{
		Headers: []interface{}{"range_id", "min_key", "max_key", "count", "estimated_size"},
		Rows:    rows,
	}, nil
}

//nolint:gochecknoinits
func init() {
	catSstCmd.Flags().Int("amount", -1, "how many records to return, or -1 for all records")
	catSstCmd.Flags().StringP("file", "f", "", "path to an sstable file, or \"-\" for stdin")
	_ = catSstCmd.MarkFlagRequired("file")

	rootCmd.AddCommand(catSstCmd)
}
