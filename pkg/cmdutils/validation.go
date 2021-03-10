package cmdutils

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	ErrInvalid      = errors.New("validation")
	ErrNoValidation = errors.New("no validation function passed OR condition")
)

func FuncValidator(pos int, validator func(string) error) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if pos > len(args)-1 {
			return fmt.Errorf("%w - no argument supplied at index %d", ErrInvalid, pos)
		}
		err := validator(args[pos])
		if err != nil {
			return fmt.Errorf("argument at position %d: %w", pos, err)
		}
		return nil
	}
}

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

func Or(funcs ...cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		for _, f := range funcs {
			if err := f(cmd, args); err == nil {
				return nil
			}
		}
		return ErrNoValidation
	}
}
