package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

var (
	ErrInvalid      = errors.New("validation")
	ErrNoValidation = errors.New("no validation function passed OR condition")
)

func uriAtPos(args []string, pos int) (*uri.URI, error) {
	if pos > len(args)-1 {
		return nil, fmt.Errorf("%w - no uri supplied at argument %d", ErrInvalid, pos)
	}
	u, err := uri.Parse(args[pos])
	if err != nil {
		return nil, fmt.Errorf("%w - argument at position: %d - %s", ErrInvalid, pos, err.Error())
	}
	return u, nil
}

func IsRefURI(pos int) ValidationFunc {
	return func(args []string) error {
		u, err := uriAtPos(args, pos)
		if err != nil {
			return err
		}
		if !u.IsRef() {
			return fmt.Errorf("%w - argument at position: %d - not a ref URI", ErrInvalid, pos)
		}
		return nil
	}
}

func IsRepoURI(pos int) ValidationFunc {
	return func(args []string) error {
		u, err := uriAtPos(args, pos)
		if err != nil {
			return err
		}
		if !u.IsRepository() {
			return fmt.Errorf("%w - argument at position: %d - not a repository URI", ErrInvalid, pos)
		}
		return nil
	}
}

func IsPathURI(pos int) ValidationFunc {
	return func(args []string) error {
		u, err := uriAtPos(args, pos)
		if err != nil {
			return err
		}
		if !u.IsFullyQualified() {
			return fmt.Errorf("%w - argument at position: %d - not a path URI", ErrInvalid, pos)
		}
		return nil
	}
}

func HasNArgs(n int) ValidationFunc {
	return func(args []string) error {
		if len(args) != n {
			return fmt.Errorf("%w - expected %d arguments", ErrInvalid, n)
		}
		return nil
	}
}

func HasRangeArgs(n1, n2 int) ValidationFunc {
	return func(args []string) error {
		if len(args) < n1 || len(args) > n2 {
			return fmt.Errorf("%w - expected %d-%d arguments", ErrInvalid, n1, n2)
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

func Or(funcs ...ValidationFunc) ValidationFunc {
	return func(args []string) error {
		for _, f := range funcs {
			if err := f(args); err == nil {
				return nil
			}
		}
		return ErrNoValidation
	}
}
