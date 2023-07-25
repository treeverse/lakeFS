package cmd

import (
	"errors"

	"github.com/spf13/cobra"
)

type transportMethod int

const (
	transportMethodDefault = iota
	transportMethodDirect
	transportMethodPreSign
)

var ErrRequestFailed = errors.New("request failed")

func transportMethodFromFlags(direct bool, preSign bool) transportMethod {
	switch {
	case direct && preSign:
		Die("Can't enable both direct and pre-sign", 1)
	case direct:
		return transportMethodDirect
	case preSign:
		return transportMethodPreSign
	}
	return transportMethodDefault
}

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "View and manipulate objects",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(fsCmd)
}
