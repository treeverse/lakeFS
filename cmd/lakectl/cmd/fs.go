package cmd

import (
	"errors"

	"github.com/spf13/cobra"
)

var ErrRequestFailed = errors.New("request failed")

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "View and manipulate objects",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(fsCmd)
}
