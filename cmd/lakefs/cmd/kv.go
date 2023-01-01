package cmd

import (
	"encoding/json"
	"errors"
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

var errInvalidParamValue = errors.New("invalid parameter value")

var kvCmd = &cobra.Command{
	Use:    "kv",
	Short:  "Inspect lakeFS' Key-Value Store",
	Hidden: true,
}

var kvGetCmd = &cobra.Command{
	Use:   "get <partition> <path>",
	Short: "Return the value for the given path under the given partition",
	Args:  cobra.ExactArgs(GetCmdNumArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := loadConfig()

		pretty, err := cmd.Flags().GetBool("pretty")
		if err != nil {
			return err
		}

		ctx := cmd.Context()
		kvParams, err := cfg.DatabaseParams()
		if err != nil {
			return fmt.Errorf("KV params: %w", err)
		}

		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			return fmt.Errorf("failed to open KV store: %w", err)
		}
		defer kvStore.Close()

		partitionKey := args[0]
		path := args[1]
		val, err := kvStore.Get(ctx, []byte(partitionKey), []byte(path))
		if err != nil {
			return fmt.Errorf("get failed: %w", err)
		}
		kvObj, err := kv.NewRecord(partitionKey, path, val.Value)
		if err != nil {
			return fmt.Errorf("KV record from value: %w", err)
		}

		encoder := json.NewEncoder(os.Stdout)
		if pretty {
			encoder.SetIndent("", "  ")
		}
		err = encoder.Encode(kvObj)
		if err != nil {
			return fmt.Errorf("json.Marshal failed: %w", err)
		}

		return nil
	},
}

var kvScanCmd = &cobra.Command{
	Use:   "scan <partition> [<path>]",
	Short: "Scan through keys and values under the given partition. An optional path can be specified as a starting point (inclusive)",
	Args:  cobra.RangeArgs(ScanCmdMinArgs, ScanCmdMaxArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := loadConfig()

		limit, err := cmd.Flags().GetInt("limit")
		if err != nil {
			return err
		}
		until, err := cmd.Flags().GetString("until")
		if err != nil {
			return err
		}
		pretty, err := cmd.Flags().GetBool("pretty")
		if err != nil {
			return err
		}

		partitionKey := args[0]
		var start []byte
		if len(args) > ScanCmdMinArgs {
			start = []byte(args[1])
		}

		if len(until) > 0 && until < string(start) {
			return fmt.Errorf("`until` cannot precede `path`: %w", errInvalidParamValue)
		}

		ctx := cmd.Context()
		kvParams, err := cfg.DatabaseParams()
		if err != nil {
			return fmt.Errorf("KV params: %w", err)
		}

		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			return fmt.Errorf("failed to open KV store: %w", err)
		}
		defer kvStore.Close()

		iter, err := kvStore.Scan(ctx, []byte(partitionKey), kv.ScanOptions{KeyStart: start})
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		defer iter.Close()

		num := 0
		encoder := json.NewEncoder(os.Stdout)
		for iter.Next() {
			entry := iter.Entry()
			if len(until) > 0 && string(entry.Key) > until {
				break
			}
			kvObj, err := kv.NewRecord(partitionKey, string(entry.Key), entry.Value)
			if err != nil {
				return fmt.Errorf("KV record from value: %w", err)
			}
			if pretty {
				encoder.SetIndent("", "  ")
			}
			err = encoder.Encode(kvObj)
			if err != nil {
				return fmt.Errorf("json.Marshal failed: %w", err)
			}
			num++
			if num == limit {
				break
			}
		}
		if iter.Err() != nil {
			return fmt.Errorf("scan operation ended with error: %w", iter.Err())
		}
		return nil
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
