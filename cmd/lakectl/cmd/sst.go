package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	sst "github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/sstable"
)

type InMemFileInfo struct {
	name string
	size int64
}

func (r *InMemFileInfo) Name() string       { return r.name }
func (r *InMemFileInfo) Size() int64        { return r.size }
func (r *InMemFileInfo) Mode() os.FileMode  { return os.ModePerm }
func (r *InMemFileInfo) ModTime() time.Time { return time.Now() }
func (r *InMemFileInfo) IsDir() bool        { return false }
func (r *InMemFileInfo) Sys() interface{}   { return nil }

type InMemReadableFile struct {
	data *bytes.Reader
}

func (r *InMemReadableFile) ReadAt(p []byte, off int64) (n int, err error) {
	return r.data.ReadAt(p, off)
}

func (r *InMemReadableFile) Close() error {
	return nil
}

func (r *InMemReadableFile) Stat() (os.FileInfo, error) {
	return &InMemFileInfo{
		name: "stdin",
		size: r.data.Size(),
	}, nil
}

func getIterFromFile(filePath string) (committed.ValueIterator, map[string]string, error) {
	var file sst.ReadableFile
	var err error
	// read from stdin (file has to be seekable/stat-able so we have this weird wrapper
	if filePath == "-" {
		data, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return nil, nil, err
		}
		file = &InMemReadableFile{
			bytes.NewReader(data),
		}
	} else {
		file, err = os.Open(filePath)
		if err != nil {
			return nil, nil, err
		}
	}
	// read file descriptor
	reader, err := sst.NewReader(file, sst.ReaderOptions{})
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

func writeRangeSSTable(iter committed.ValueIterator, amount int) *Table {
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
	return &Table{
		Headers: []interface{}{"path", "identity", "size", "etag", "last_modified", "address", "metadata"},
		Rows:    rows,
	}
}

func writeMetaRangeSSTable(iter committed.ValueIterator, amount int) *Table {
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
	return &Table{
		Headers: []interface{}{"range_id", "min_key", "max_key", "count", "estimated_size"},
		Rows:    rows,
	}
}

var sstCatTemplate = `{{ .Table | table }}`

var sstCmd = &cobra.Command{
	Use:    "cat-sst <sst-file>",
	Short:  "Explore lakeFS .sst files",
	Args:   cobra.ExactArgs(1),
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		iter, props, err := getIterFromFile(args[0])
		if err != nil {
			DieErr(err)
		}
		defer iter.Close()

		// get props
		typ, ok := props[committed.MetadataTypeKey]
		if !ok || (typ != committed.MetadataRangesType && typ != committed.MetadataMetarangesType) {
			panic("could not determine sst file type")
		}

		var table *Table
		if typ == committed.MetadataMetarangesType {
			table = writeMetaRangeSSTable(iter, amount)
		} else {
			// otherwise, a normal range
			table = writeRangeSSTable(iter, amount)
		}
		// write to stdout
		Write(sstCatTemplate, struct {
			Table *Table
		}{table})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(sstCmd)
	sstCmd.Flags().Int("amount", -1, "how many records to return, or -1 for all records")
}
