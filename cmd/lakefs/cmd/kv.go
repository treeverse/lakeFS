package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	ScanCmdMinArgs = 1
	ScanCmdMaxArgs = 2
	GetCmdNumArgs  = 2
)

var kvCmd = &cobra.Command{
	Use:    "kv",
	Short:  "Inspect lakeFS' Key-Value Store",
	Hidden: true,
}

var kvGetCmd = &cobra.Command{
	Use:    "get <partition key> <key>",
	Short:  "Return the value for the given key under the given partition",
	Hidden: true,
	Args:   cobra.ExactArgs(GetCmdNumArgs),
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.Default()
		cfg := loadConfig()

		ctx := cmd.Context()
		logger.WithField("version", version.Version).Info("lakeFS kv")

		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			logger.WithError(err).Fatal("Failed to open KV store")
		}
		defer kvStore.Close()

		partitionKey := args[0]
		key := args[1]
		val, err := kvStore.Get(ctx, []byte(partitionKey), []byte(key))
		if err != nil {
			logger.WithError(err).Fatal("Failed to get value")
		}
		prettyVal, err := kv.ToPrettyString(key, val.Value)
		if err != nil {
			logger.WithError(err).Fatal("Failed to build object from KV value")
		}

		fmt.Printf("%s:\n%s\n", key, prettyVal)
	},
}

var kvScanCmd = &cobra.Command{
	Use:    "scan <partition key> [<key>]",
	Short:  "Scan through keys and values under the given partition. An optional key cna be specified as a starting point (inclusive)",
	Hidden: true,
	Args:   cobra.RangeArgs(ScanCmdMinArgs, ScanCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.Default()
		cfg := loadConfig()

		ctx := cmd.Context()
		logger.WithField("version", version.Version).Info("lakeFS scan")

		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			logger.WithError(err).Fatal("Failed to open KV store")
		}
		defer kvStore.Close()

		partitionKey := args[0]
		var start []byte = nil
		if len(args) > ScanCmdMinArgs {
			start = []byte(args[1])
		}
		iter, err := kvStore.Scan(ctx, []byte(partitionKey), start)
		if err != nil {
			logger.WithError(err).Fatal("Scan failed")
		}
		defer iter.Close()

		for iter.Next() {
			if iter.Err() != nil {
				logger.WithError(err).Fatal("Iteration failed")
			}
			entry := iter.Entry()
			prettyVal, err := kv.ToPrettyString(string(entry.Key), entry.Value)
			if err != nil {
				logger.WithError(err).Fatal("Failed to build object from KV value")
			}
			fmt.Printf("%s:\n%s\n", string(entry.Key), prettyVal)
		}
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(kvCmd)
	kvCmd.AddCommand(kvGetCmd)
	kvCmd.AddCommand(kvScanCmd)
}
