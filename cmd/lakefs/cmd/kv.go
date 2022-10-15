package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/treeverse/lakefs/pkg/actions"
	_ "github.com/treeverse/lakefs/pkg/auth"
	_ "github.com/treeverse/lakefs/pkg/auth/model"
	_ "github.com/treeverse/lakefs/pkg/graveler"

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

		pretty, err := cmd.Flags().GetBool("pretty")
		if err != nil {
			os.Exit(1)
		}

		ctx := cmd.Context()
		kvParams, err := cfg.GetKVParams()
		if err != nil {
			fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}

		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		partitionKey := args[0]
		key := args[1]
		val, err := kvStore.Get(ctx, []byte(partitionKey), []byte(key))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get value for (%s,%s): %s\n", partitionKey, key, err)
			os.Exit(1)
		}
		kvObj, err := kv.ToKvObject(partitionKey, key, val.Value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse object from KV value: %s\n", err)
			os.Exit(1)
		}

		var kvObjJSON []byte
		if pretty {
			kvObjJSON, err = json.MarshalIndent(kvObj, "", "  ")
		} else {
			kvObjJSON, err = json.Marshal(kvObj)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "json.Marshal failed: %s\n", err)
			os.Exit(1)
		}

		fmt.Println(string(kvObjJSON))
	},
}

var kvScanCmd = &cobra.Command{
	Use:    "scan <partition key> [<key>]",
	Short:  "Scan through keys and values under the given partition. An optional key can be specified as a starting point (inclusive)",
	Hidden: true,
	Args:   cobra.RangeArgs(ScanCmdMinArgs, ScanCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()

		limit, err := cmd.Flags().GetInt("limit")
		if err != nil {
			os.Exit(1)
		}
		until, err := cmd.Flags().GetString("until")
		if err != nil {
			os.Exit(1)
		}
		pretty, err := cmd.Flags().GetBool("pretty")
		if err != nil {
			os.Exit(1)
		}

		partitionKey := args[0]
		var start []byte = nil
		if len(args) > ScanCmdMinArgs {
			start = []byte(args[1])
		}

		if len(until) > 0 && until < string(start) {
			fmt.Fprintf(os.Stderr, "Invalid args: `until` cannot preced `key`")
		}

		ctx := cmd.Context()
		kvParams, err := cfg.GetKVParams()
		if err != nil {
			fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}

		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		iter, err := kvStore.Scan(ctx, []byte(partitionKey), start)
		if err != nil {
			logMsg := "Failed to scan partition '" + partitionKey + "'"
			if start != nil {
				logMsg += " with start key '" + string(start) + "'"
			}
			logMsg += ": " + err.Error() + "\n"
			fmt.Fprint(os.Stderr, logMsg)
			os.Exit(1)
		}
		defer iter.Close()

		num := 0
		kvObjs := []kv.KvObject{}
		for iter.Next() {
			if iter.Err() != nil {
				fmt.Fprintf(os.Stderr, "Failed to get next value: %s\n", iter.Err())
				os.Exit(1)
			}
			entry := iter.Entry()
			if len(until) > 0 && string(entry.Key) > until {
				break
			}
			kvObj, err := kv.ToKvObject(partitionKey, string(entry.Key), entry.Value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to parse object from KV value: %s\n", err)
				os.Exit(1)
			}
			kvObjs = append(kvObjs, *kvObj)
			num++
			if num == limit {
				break
			}
		}
		if iter.Err() != nil {
			fmt.Fprintf(os.Stderr, "Scan operation ended with error: %s", iter.Err())
			os.Exit(1)
		}

		var kvObjsJSON []byte
		if pretty {
			kvObjsJSON, err = json.MarshalIndent(kvObjs, "", "  ")
		} else {
			kvObjsJSON, err = json.Marshal(kvObjs)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "json.Marshal failed: %s\n", err)
			os.Exit(1)
		}

		fmt.Println(string(kvObjsJSON))
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(kvCmd)
	kvCmd.AddCommand(kvGetCmd)
	kvGetCmd.Flags().Bool("pretty", false, "print indented output")
	kvCmd.AddCommand(kvScanCmd)
	kvScanCmd.Flags().Int("limit", 0, "maximal number of results to return. By default, all results are returned")
	kvScanCmd.Flags().String("until", "", "last prefix to scan. If this prefix is reached or exceeded, scan will stop")
	kvScanCmd.Flags().Bool("pretty", false, "print indented output")
}
