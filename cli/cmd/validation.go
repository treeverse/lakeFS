package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/uri"
)

func uriAtPos(args []string, pos int) (*uri.URI, error) {
	if pos > len(args)-1 {
		return nil, fmt.Errorf("no uri supplied at argument %d", pos)
	}
	u, err := uri.Parse(args[pos])
	if err != nil {
		return nil, fmt.Errorf("argument at position: %d - %s", pos, err.Error())
	}
	return u, nil
}

func IsBranchURI(pos int) validationFunc {
	return func(args []string) error {
		u, err := uriAtPos(args, pos)
		if err != nil {
			return err
		}
		if !u.IsRefspec() {
			return fmt.Errorf("argument at position: %d - not a branch URI", pos)
		}
		return nil
	}
}

func IsRepoURI(pos int) validationFunc {
	return func(args []string) error {
		u, err := uriAtPos(args, pos)
		if err != nil {
			return err
		}
		if !u.IsRepository() {
			return fmt.Errorf("argument at position: %d - not a repository URI", pos)
		}
		return nil
	}
}

func IsPathURI(pos int) validationFunc {
	return func(args []string) error {
		u, err := uriAtPos(args, pos)
		if err != nil {
			return err
		}
		if !u.IsFullyQualified() {
			return fmt.Errorf("argument at position: %d - not a path URI", pos)
		}
		return nil
	}
}

func HasNArgs(n int) validationFunc {
	return func(args []string) error {
		if len(args) != n {
			return fmt.Errorf("expected %d arguments", n)
		}
		return nil
	}
}

func HasRangeArgs(n1, n2 int) validationFunc {
	return func(args []string) error {
		if len(args) < n1 || len(args) > n2 {
			return fmt.Errorf("expected %d-%d arguments", n1, n2)
		}
		return nil
	}
}

type validationFunc func(args []string) error

func ValidationChain(funcs ...validationFunc) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		for _, f := range funcs {
			if err := f(args); err != nil {
				return err
			}
		}
		return nil
	}
}

func Or(funcs ...validationFunc) validationFunc {
	return func(args []string) error {
		for _, f := range funcs {
			if err := f(args); err == nil {
				return nil
			}
		}
		return fmt.Errorf("no validation function passed OR condition")
	}
}
