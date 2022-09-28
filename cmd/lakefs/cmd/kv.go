package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/kv"
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
		cfg := loadConfig()

		ctx := cmd.Context()
		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to open KV store - %v\n", err)
			return
		}
		defer kvStore.Close()

		partitionKey := args[0]
		key := args[1]
		val, err := kvStore.Get(ctx, []byte(partitionKey), []byte(key))
		if err != nil {
			fmt.Printf("Failed to get value for (%s,%s), %v\n", partitionKey, key, err)
			return
		}
		prettyVal, err := kv.ToPrettyString(key, val.Value)
		if err != nil {
			fmt.Print("Failed to parse object from KV value - %s\n", err)
			return
		}

		fmt.Printf("%s:\n%s\n", key, prettyVal)
	},
}

var kvScanCmd = &cobra.Command{
	Use:    "scan <partition key> [<key>]",
	Short:  "Scan through keys and values under the given partition. An optional key can be specified as a starting point (inclusive)",
	Hidden: true,
	Args:   cobra.RangeArgs(ScanCmdMinArgs, ScanCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()

		ctx := cmd.Context()
		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to open KV store - %v\n", err)
			return
		}
		defer kvStore.Close()

		partitionKey := args[0]
		var start []byte = nil
		if len(args) > ScanCmdMinArgs {
			start = []byte(args[1])
		}
		iter, err := kvStore.Scan(ctx, []byte(partitionKey), start)
		if err != nil {
			logMsg := "Failed to scan partition '" + partitionKey + "'"
			if start != nil {
				logMsg += " with start key '" + string(start) + "'"
			}
			logMsg += " - " + err.Error() + "\n"
			fmt.Printf("Failed to scan partition %s - %v\n", partitionKey, err)
			return
		}
		defer iter.Close()

		for iter.Next() {
			if iter.Err() != nil {
				fmt.Printf("Failed to get next value - %v\n", iter.Err())
				return
			}
			entry := iter.Entry()
			prettyVal, err := kv.ToPrettyString(string(entry.Key), entry.Value)
			if err != nil {
				fmt.Printf("Failed to parse object from KV value - %s\n", err)
				return
			}
			fmt.Printf("%s:\n%s\n", string(entry.Key), prettyVal)
		}
		if iter.Err() != nil {
			fmt.Printf("Scan operation ended with error - %v", iter.Err())
		}
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(kvCmd)
	kvCmd.AddCommand(kvGetCmd)
	kvCmd.AddCommand(kvScanCmd)
}
