package cmd

import (
	"github.com/spf13/cobra"
)

const (
	localDefaultSyncParallelism = 25
	localDefaultSyncPresign     = true
)

func withParallelismFlag(cmd *cobra.Command) {
	cmd.Flags().IntP("parallelism", "p", localDefaultSyncParallelism,
		"Max concurrent operations to perform")
}

func withPresignFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("presign", localDefaultSyncPresign,
		"Use pre-signed URLs when downloading/uploading data (recommended)")
}

func withLocalSyncFlags(cmd *cobra.Command) {
	withParallelismFlag(cmd)
	withPresignFlag(cmd)
}

type syncFlags struct {
	parallelism int
	presign     bool
}

func getLocalSyncFlags(cmd *cobra.Command) syncFlags {
	parallelism := Must(cmd.Flags().GetInt("parallelism"))
	presign := Must(cmd.Flags().GetBool("presign"))
	return syncFlags{parallelism: parallelism, presign: presign}
}

var localCmd = &cobra.Command{
	Use: "local",
	// TODO: Remove BETA when feature complete
	Short: "BETA: sync local directories with lakeFS paths",
}

//nolint:gochecknoinits
func init() {
	// TODO: Remove line when feature complete
	localCmd.Hidden = true
	rootCmd.AddCommand(localCmd)
}
