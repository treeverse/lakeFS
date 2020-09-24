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

type ArgumentValidator func(string) error

func PositionValidator(pos int, validator ArgumentValidator) Validator {
	return func(args []string) error {
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

func HasNArgs(n int) Validator {
	return func(args []string) error {
		if len(args) != n {
			return fmt.Errorf("%w - expected %d arguments", ErrInvalid, n)
		}
		return nil
	}
}

func HasRangeArgs(n1, n2 int) Validator {
	return func(args []string) error {
		if len(args) < n1 || len(args) > n2 {
			return fmt.Errorf("%w - expected %d-%d arguments", ErrInvalid, n1, n2)
		}
		return nil
	}
}

type Validator func(args []string) error

func ValidationChain(funcs ...Validator) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		for _, f := range funcs {
			if err := f(args); err != nil {
				return err
			}
		}
		return nil
	}
}

func Or(funcs ...Validator) Validator {
	return func(args []string) error {
		for _, f := range funcs {
			if err := f(args); err == nil {
				return nil
			}
		}
		return ErrNoValidation
	}
}
