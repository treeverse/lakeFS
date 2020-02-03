package cmd

import (
	"fmt"

	"github.com/treeverse/lakefs/uri"

	"github.com/spf13/cobra"
)

func ValidURIAtArgs(argIndex ...int) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		maxIndex := 0
		for _, a := range argIndex {
			if a > maxIndex {
				maxIndex = a
			}
		}
		if len(args)-1 > maxIndex {
			return fmt.Errorf("not enough arguments passed")
		}

		for _, a := range argIndex {
			currentArg := args[a]
			if !uri.IsValid(currentArg) {
				return fmt.Errorf("invalid uri: %s", args[a])
			}
		}

		return nil
	}
}
