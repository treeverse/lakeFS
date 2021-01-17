package cmd

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/sstable"
)

func getIterFromFile(filePath string) (committed.ValueIterator, map[string]string, error) {
	var file pebblesst.ReadableFile
	var err error
	// read from stdin (file has to be seekable/stat-able so we have this weird wrapper
	if filePath == "-" {
		fh, err := ioutil.TempFile("", "cat-sst-*")
		if err != nil {
			return nil, nil, fmt.Errorf("error creating tempfile: %w", err)
		}
		_, err = io.Copy(fh, os.Stdin)
		if err != nil {
			return nil, nil, fmt.Errorf("error copying from stdin to tempfile: %w", err)
		}
		err = os.Remove(fh.Name())
		if err != nil {
			return nil, nil, fmt.Errorf("could not unlink temp file %s: %w", fh.Name(), err)
		}
		file = fh
	} else {
		file, err = os.Open(filePath)
		if err != nil {
			return nil, nil, err
		}
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

func formatRangeSSTable(iter committed.ValueIterator, amount int) (*Table, error) {
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
		ent, err := rocks.ValueToEntry(gv)
		if err != nil {
			panic(err)
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
			table, err = formatRangeSSTable(iter, amount)
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

//nolint:gochecknoinits
func init() {
	sstCmd.Flags().Int("amount", -1, "how many records to return, or -1 for all records")
	sstCmd.Flags().String("file", "", "path to an sstable file, or \"-\" for stdin")
	_ = sstCmd.MarkFlagRequired("file")

	rootCmd.AddCommand(sstCmd)
}
