package cmd

import (
	"github.com/spf13/cobra"
)

const (
	abuseDefaultAmount      = 1000000
	abuseDefaultParallelism = 100
)

var abuseCmd = &cobra.Command{
	Use:    "abuse <sub command>",
	Short:  "Abuse a running lakeFS instance. See sub commands for more info.",
	Hidden: true,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(abuseCmd)
}
