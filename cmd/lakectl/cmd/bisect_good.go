package cmd

import "github.com/spf13/cobra"

// bisectGoodCmd represents the good command
var bisectGoodCmd = &cobra.Command{
	Use:     "good",
	Aliases: []string{"old"},
	Short:   "Set current commit as 'good' commit that is known to be before the bug was introduced",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runBisectSelect(BisectSelectGood)
	},
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectGoodCmd)
}
