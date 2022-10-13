package cmd

import (
	"fmt"
	"os"

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
			fmt.Fprintf(os.Stderr, "Failed to open KV store - %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		partitionKey := args[0]
		key := args[1]
		val, err := kvStore.Get(ctx, []byte(partitionKey), []byte(key))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get value for (%s,%s), %s\n", partitionKey, key, err)
			os.Exit(1)
		}
		prettyVal, err := kv.ToPrettyString(key, val.Value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse object from KV value - %s\n", err)
			os.Exit(1)
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

		amount, err := cmd.Flags().GetInt("amount")
		if err != nil {
			os.Exit(1)
		}

		ctx := cmd.Context()
		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open KV store - %s\n", err)
			os.Exit(1)
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
			fmt.Fprint(os.Stderr, logMsg)
			os.Exit(1)
		}
		defer iter.Close()

		num := 0
		for iter.Next() {
			if iter.Err() != nil {
				fmt.Fprintf(os.Stderr, "Failed to get next value - %s\n", iter.Err())
				os.Exit(1)
			}
			entry := iter.Entry()
			prettyVal, err := kv.ToPrettyString(string(entry.Key), entry.Value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to parse object from KV value - %s\n", err)
				os.Exit(1)
			}
			fmt.Printf("%s:\n%s\n", string(entry.Key), prettyVal)
			num++
			if num == amount {
				break
			}
		}
		if iter.Err() != nil {
			fmt.Fprintf(os.Stderr, "Scan operation ended with error - %s", iter.Err())
			os.Exit(1)
		}
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(kvCmd)
	kvCmd.AddCommand(kvGetCmd)
	kvCmd.AddCommand(kvScanCmd)
	kvScanCmd.Flags().Int("amount", 0, "number of results to return. By default, all results are returned")
}
