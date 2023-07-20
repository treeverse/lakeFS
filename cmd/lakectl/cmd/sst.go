package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/sstable"
	"google.golang.org/protobuf/proto"
)

func readStdin() (pebblesst.ReadableFile, error) {
	// test if stdin is seekable
	if utils.IsSeekable(os.Stdin) {
		return os.Stdin, nil
	}
	// not seekable - read it into a temp file
	fh, err := os.CreateTemp("", "cat-sst-*")
	if err != nil {
		return nil, fmt.Errorf("error creating tempfile: %w", err)
	}
	_, err = io.Copy(fh, os.Stdin)
	if err != nil {
		return nil, fmt.Errorf("error copying from stdin to tempfile: %w", err)
	}
	err = os.Remove(fh.Name())
	if err != nil {
		return nil, fmt.Errorf("could not unlink temp file %s: %w", fh.Name(), err)
	}
	return fh, nil
}

func getIterFromFile(filePath string) (committed.ValueIterator, map[string]string, error) {
	var file pebblesst.ReadableFile
	var err error
	// read from stdin (file has to be seekable/stat-able so we have this weird wrapper
	if filePath == "-" {
		file, err = readStdin()
	} else {
		file, err = os.Open(filePath)
	}
	if err != nil {
		return nil, nil, err
	}
	// read file descriptor
	reader, err := pebblesst.NewReader(file, pebblesst.ReaderOptions{})
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

func formatEntryRangeSSTable(iter committed.ValueIterator, amount int) (*utils.Table, error) {
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
	return &utils.Table{
		Headers: []interface{}{"path", "identity", "size", "etag", "last_modified", "address", "metadata"},
		Rows:    rows,
	}, nil
}

func formatBranchRangeSSTable(iter committed.ValueIterator, amount int) (*utils.Table, error) {
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
	return &utils.Table{
		Headers: []interface{}{"branch ID", "commit ID"},
		Rows:    rows,
	}, nil
}

func formatTagsRangeSSTable(iter committed.ValueIterator, amount int) (*utils.Table, error) {
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
	return &utils.Table{
		Headers: []interface{}{"tag ID", "commit ID"},
		Rows:    rows,
	}, nil
}

func formatCommitRangeSSTable(iter committed.ValueIterator, amount int) (*utils.Table, error) {
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
	return &utils.Table{
		Headers: []interface{}{"commit ID", "committer", "message", "creation date", "metadata", "parents", "metarange ID", "version", "generation"},
		Rows:    rows,
	}, nil
}

func formatRangeSSTable(iter committed.ValueIterator, amount int, entityType string) (*utils.Table, error) {
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

func formatMetaRangeSSTable(iter committed.ValueIterator, amount int) (*utils.Table, error) {
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
	return &utils.Table{
		Headers: []interface{}{"range_id", "min_key", "max_key", "count", "estimated_size"},
		Rows:    rows,
	}, nil
}

var sstCatTemplate = `{{ .Table | table }}`

var sstCmd = &cobra.Command{
	Use:    "cat-sst <sst-file>",
	Short:  "Explore lakeFS .sst files",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		filePath, _ := cmd.Flags().GetString("file")
		iter, props, err := getIterFromFile(filePath)
		if err != nil {
			utils.DieErr(err)
		}
		defer iter.Close()

		// get props
		typ, ok := props[committed.MetadataTypeKey]
		if !ok {
			utils.DieFmt("could not determine sstable file type")
		}

		var table *utils.Table
		switch typ {
		case committed.MetadataMetarangesType:
			table, err = formatMetaRangeSSTable(iter, amount)
		case committed.MetadataRangesType:
			table, err = formatRangeSSTable(iter, amount, props[graveler.EntityTypeKey])
		default:
			utils.DieFmt("unknown sstable file type: %s", typ)
		}
		if err != nil {
			utils.DieErr(err)
		}

		// write to stdout
		utils.Write(sstCatTemplate, struct {
			Table *utils.Table
		}{table})
	},
}

//nolint:gochecknoinits
func init() {
	sstCmd.Flags().Int("amount", -1, "how many records to return, or -1 for all records")
	sstCmd.Flags().StringP("file", "f", "", "path to an sstable file, or \"-\" for stdin")
	_ = sstCmd.MarkFlagRequired("file")

	rootCmd.AddCommand(sstCmd)
}
