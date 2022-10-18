package cmdutils

import (
	"github.com/spf13/cobra"
)

func ValidationChain(funcs ...cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		for _, f := range funcs {
			if err := f(cmd, args); err != nil {
				return err
			}
		}
		return nil
	}
}
