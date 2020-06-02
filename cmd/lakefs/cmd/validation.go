package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func HasNArgs(n int) ValidationFunc {
	return func(args []string) error {
		if len(args) != n {
			return fmt.Errorf("expected %d arguments", n)
		}
		return nil
	}
}

type ValidationFunc func(args []string) error

func ValidationChain(funcs ...ValidationFunc) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		for _, f := range funcs {
			if err := f(args); err != nil {
				return err
			}
		}
		return nil
	}
}
