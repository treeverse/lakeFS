package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	localStatusMinArgs = 0
	localStatusMaxArgs = 1
)

var localStatusCmd = &cobra.Command{
	Use:   "status [directory]",
	Short: "show modifications (both remote and local) to the directory and the remote location it tracks",
	Args:  cobra.RangeArgs(localStatusMinArgs, localStatusMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		dir := "."
		if len(args) > 0 {
			dir = args[0]
		}
		abs, err := filepath.Abs(dir)
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
		remoteBase := uri.WithRef(remote, idx.AtHead)
		fmt.Printf("diff 'local://%s' <--> '%s'...\n", idx.LocalPath(), remoteBase)

		client := getClient()
		c := localDiff(cmd.Context(), client, remoteBase, idx.LocalPath())

		// compare both
		if !localOnly {
			fmt.Printf("diff '%s' <--> '%s'...\n", remoteBase, remote)
			d := make(chan api.Diff, maxDiffPageSize)
			var wg errgroup.Group
			wg.Go(func() error {
				return diff.GetDiffRefs(cmd.Context(), client, remoteBase.Repository, remoteBase.Ref, remote.Ref, swag.StringValue(remoteBase.Path), d, false)
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
			err = wg.Wait()
			if err != nil {
				DieErr(err)
			}

			c = c.MergeWith(changes)
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
