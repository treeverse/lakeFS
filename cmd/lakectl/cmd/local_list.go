package cmd

import (
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
)

const (
	localListMinArgs = 0
	localListMaxArgs = 1

	indicesListTemplate = `{{.IndicesListTable | table -}}`
)

var localListCmd = &cobra.Command{
	Use:   "list [directory]",
	Short: "find and list directories that are synced with lakeFS",
	Args:  cobra.RangeArgs(localListMinArgs, localListMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		dir := "."
		if len(args) > 0 {
			dir = args[0]
		}
		abs, err := filepath.Abs(dir)
		if err != nil {
			DieErr(err)
		}
		gitRoot, err := git.GetRepositoryPath(abs)
		if err == nil {
			abs = gitRoot
		} else if !(errors.Is(err, git.ErrNotARepository) || errors.Is(err, git.ErrNoGit)) { // allow support in environments with no git
			DieErr(err)
		}

		dirs, err := local.FindIndices(abs)
		if err != nil {
			DieErr(err)
		}

		var rows [][]interface{}
		for _, d := range dirs {
			idx, err := local.ReadIndex(d)
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
					"Last synced HEAD",
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
