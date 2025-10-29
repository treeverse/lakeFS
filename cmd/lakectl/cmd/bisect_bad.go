package cmd

import "github.com/spf13/cobra"

// bisectBadCmd represents the bad command
var bisectBadCmd = &cobra.Command{
	Use:     "bad",
	Aliases: []string{"new"},
	Short:   "Set 'bad' commit that is known to contain the bug",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runBisectSelect(BisectSelectBad)
	},
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectBadCmd)
}
