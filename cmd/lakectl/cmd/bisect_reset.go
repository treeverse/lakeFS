package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// bisectResetCmd represents the reset command
var bisectResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Clean up the bisection state",
	Run: func(cmd *cobra.Command, args []string) {
		err := BisectRemove()
		if os.IsNotExist(err) {
			// Should not be an error exit code when no active session
			Warning("No active bisect session")
			return
		}
		if err != nil {
			DieErr(err)
		}
		fmt.Println("Cleared bisect session")
	},
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectResetCmd)
}
