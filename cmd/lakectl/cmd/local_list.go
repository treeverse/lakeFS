package cmd

import (
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/local"
)

const (
	indicesListTemplate = `{{.IndicesListTable | table -}}`
)

var localListCmd = &cobra.Command{
	Use:   "list [directory]",
	Short: "find and list directories that are synced with lakeFS.",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		_, localPath := getLocalArgs(args, false, true)

		dirs, err := local.FindIndices(localPath)
		if err != nil {
			DieErr(err)
		}

		var rows [][]interface{}
		for _, d := range dirs {
			idx, err := local.ReadIndex(filepath.Join(localPath, d))
			if err != nil {
				DieErr(err)
			}
			remote, err := idx.GetCurrentURI()
			if err != nil {
				DieErr(err)
			}
			rows = append(rows, table.Row{d, remote, idx.AtHead})
		}
		data := struct {
			IndicesListTable *Table
		}{
			IndicesListTable: &Table{
				Headers: []interface{}{
					"Directory",
					"Remote URI",
					"Synced commit",
				},
				Rows: rows,
			},
		}
		Write(indicesListTemplate, data)
	},
}

//nolint:gochecknoinits
func init() {
	localCmd.AddCommand(localListCmd)
}
