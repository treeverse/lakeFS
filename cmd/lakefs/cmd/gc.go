package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	//"github.com/treeverse/lakefs/pkg/graveler"
)

var gcCmd = &cobra.Command{
	Use:    "gc",
	Hidden: true,
	Short:  "Manually run garbage collection",
}

var simulateCmd = &cobra.Command{
	Use:   "simulate rules.json partition-dump.json",
	Short: "Simulate a GC run on locally-available files",
	Long: `Run garbage collection on locally-available files.

First arg "rules.json" is a file containing the JSON retention rules.  Second arg
"partition-dump" is a file containing the entire JSON scan of the branch partition on KV.
Create it with "lakefs kv scan <reponame>-ID".  To discover the ID try "lakefs kv scan graveler"
and look for the instance_uid field.
`,
	Args: cobra.ExactArgs(2), Run: func(cmd *cobra.Command, args []string) {
		rulesFile, err := os.Open(args[0])
		if err != nil {
			printMsgAndExit(fmt.Errorf("open rules %s: %w", args[0], err))
		}
		// var rules []graveler.GarbageCollectionRules
		// if err := ReadJSON(rulesFile, &rules); err != nil {
		// 	printMsgAndExit(fmt.Errorf("read rules %s: %w", args[0], err))
		// }
		rulesFile.Close()
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(gcCmd)
	gcCmd.AddCommand(simulateCmd)
}
