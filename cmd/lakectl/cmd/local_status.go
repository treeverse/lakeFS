package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/local"
	"golang.org/x/sync/errgroup"
)

var localStatusCmd = &cobra.Command{
	Use:   "status [directory]",
	Short: "show modifications (both remote and local) to the directory and the remote location it tracks",
	Args:  localDefaultArgsRange,
	Run: func(cmd *cobra.Command, args []string) {
		_, localPath := getSyncArgs(args, false, false)
		abs, err := filepath.Abs(localPath)
		if err != nil {
			DieErr(err)
		}

		localOnly := Must(cmd.Flags().GetBool("local"))

		idx, err := local.ReadIndex(abs)
		if err != nil {
			DieErr(err)
		}

		remote, err := idx.GetCurrentURI()
		if err != nil {
			DieErr(err)
		}

		dieOnInterruptedOperation(cmd.Context(), LocalOperation(idx.ActiveOperation), false)

		remoteBase := remote.WithRef(idx.AtHead)
		client := getClient()
		c := localDiff(cmd.Context(), client, remoteBase, idx.LocalPath())

		// compare both
		if !localOnly {
			fmt.Printf("diff '%s' <--> '%s'...\n", remoteBase, remote)
			d := make(chan apigen.Diff, maxDiffPageSize)
			var wg errgroup.Group
			wg.Go(func() error {
				return diff.StreamRepositoryDiffs(cmd.Context(), client, remoteBase, remote, swag.StringValue(remoteBase.Path), d, false)
			})

			var changes local.Changes
			wg.Go(func() error {
				for dif := range d {
					changes = append(changes, &local.Change{
						Source: local.ChangeSourceRemote,
						Path:   strings.TrimPrefix(dif.Path, remoteBase.GetPath()),
						Type:   local.ChangeTypeFromString(dif.Type),
					})
				}
				return nil
			})
			if err := wg.Wait(); err != nil {
				DieErr(err)
			}

			c = c.MergeWith(changes, local.MergeStrategyNone)
		}

		if len(c) == 0 {
			fmt.Printf("\nNo diff found.\n")
			return
		}

		t := table.NewWriter()
		t.SetStyle(table.StyleDouble)
		t.AppendHeader(table.Row{"source", "change", "path"})
		for _, a := range c {
			change := local.ChangeTypeString(a.Type)
			_, color := diff.Fmt(change)
			r := table.Row{
				color.Sprint(local.ChangeSourceString(a.Source)),
				color.Sprint(change),
				color.Sprint(a.Path),
			}
			t.AppendRow(r)
		}
		fmt.Printf("\n%s\n\n", t.Render())
	},
}

//nolint:gochecknoinits
func init() {
	localStatusCmd.Flags().BoolP("local", "l", false, "Don't compare against remote changes")
	localCmd.AddCommand(localStatusCmd)
}
