package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	_ "github.com/treeverse/lakefs/pkg/actions"
	_ "github.com/treeverse/lakefs/pkg/auth"
	_ "github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

const (
	ScanCmdMinArgs = 1
	ScanCmdMaxArgs = 2
	GetCmdNumArgs  = 2
)

var (
	errInvalidParamValue      = errors.New("invalid parameter value")
	errNoInputFile            = errors.New("no input file provided")
	errInvalidStrategy        = errors.New("invalid strategy")
	errMutuallyExclusiveFlags = errors.New("--all, --repo, and --sections flags are mutually exclusive")
	errRepoNotFound           = errors.New("repository not found")
)

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
		cfg := LoadConfig().GetBaseConfig()

		pretty, err := cmd.Flags().GetBool("pretty")
		if err != nil {
			return err
		}

		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
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
		cfg := LoadConfig().GetBaseConfig()

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
		kvParams, err := kvparams.NewConfig(&cfg.Database)
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

var kvDumpCmd = &cobra.Command{
	Use:   "dump [--output FILE] [--sections SECTIONS] [--all] [--repo NAME] [--pretty]",
	Short: "Dump KV store data to JSON format",
	Long: `Dump KV store data to JSON format.

Usage:
  lakefs kv dump                          # dump all sections to stdout
  lakefs kv dump --output dump.json       # dump to file
  lakefs kv dump --sections auth,pulls    # dump specific sections
  lakefs kv dump --all                    # dump all partitions (not just known sections)
  lakefs kv dump --repo my-repo           # dump partition for specific repository

Sections:
  auth      - authentication data
  pulls     - pull request data
  kv        - kv internal metadata

Default: supported sections (auth, pulls, kv)

Note: --all, --repo, and --sections flags are mutually exclusive`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := LoadConfig().GetBaseConfig()

		outputFile, err := cmd.Flags().GetString("output")
		if err != nil {
			return err
		}

		sectionsFlag, err := cmd.Flags().GetString("sections")
		if err != nil {
			return err
		}

		allFlag, err := cmd.Flags().GetBool("all")
		if err != nil {
			return err
		}

		repoFlag, err := cmd.Flags().GetString("repo")
		if err != nil {
			return err
		}

		pretty, err := cmd.Flags().GetBool("pretty")
		if err != nil {
			return err
		}

		// Validate mutually exclusive flags
		flagsSet := 0
		if sectionsFlag != "" {
			flagsSet++
		}
		if allFlag {
			flagsSet++
		}
		if repoFlag != "" {
			flagsSet++
		}
		if flagsSet > 1 {
			return errMutuallyExclusiveFlags
		}

		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			return fmt.Errorf("KV params: %w", err)
		}

		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			return fmt.Errorf("failed to open KV store: %w", err)
		}
		defer kvStore.Close()

		// Open output file
		var output *os.File
		if outputFile == "" || outputFile == "-" {
			output = os.Stdout
		} else {
			output, err = os.Create(outputFile)
			if err != nil {
				return fmt.Errorf("failed to create output file: %w", err)
			}
			defer func() {
				if err := output.Close(); err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close output file: %v\n", err)
				}
			}()
		}

		var dump *kv.DumpFormat

		// Handle different dump modes
		switch {
		case allFlag:
			// Dump all known partitions (empty sections = all)
			dump, err = kv.CreateDump(ctx, kvStore, []string{})
			if err != nil {
				return fmt.Errorf("failed to create dump: %w", err)
			}
		case repoFlag != "":
			// Dump repository-specific partition
			dump, err = createRepoPartitionDump(ctx, kvStore, repoFlag)
			if err != nil {
				return fmt.Errorf("failed to create repository dump: %w", err)
			}
		default:
			// Parse sections (comma-separated)
			var sections []string
			if sectionsFlag != "" {
				sections = strings.Split(sectionsFlag, ",")
				for i, s := range sections {
					sections[i] = strings.TrimSpace(s)
				}
			}
			// Empty default means supported sections

			// Create dump using sections
			dump, err = kv.CreateDump(ctx, kvStore, sections)
			if err != nil {
				return fmt.Errorf("failed to create dump: %w", err)
			}
		}

		// Encode to JSON
		encoder := json.NewEncoder(output)
		if pretty {
			encoder.SetIndent("", "  ")
		}

		if err := encoder.Encode(dump); err != nil {
			return fmt.Errorf("failed to encode dump: %w", err)
		}

		if outputFile != "" && outputFile != "-" {
			_, _ = fmt.Fprintf(os.Stderr, "Dump written to %s\n", outputFile)
		}

		return nil
	},
}

var kvLoadCmd = &cobra.Command{
	Use:   "load --input FILE [--sections SECTIONS] [--strategy STRATEGY]",
	Short: "Load KV store data from JSON dump",
	Long: `Load KV store data from JSON dump file.

Usage:
  lakefs kv load --input dump.json                    # load all sections from dump
  lakefs kv load --input dump.json --sections auth    # load specific sections
  lakefs kv load --input - --strategy overwrite       # load from stdin, overwrite existing

Sections:
  auth      - authentication data
  pulls     - pull request data
  kv        - kv internal metadata

Default: all sections found in the dump file

Strategies:
  skip      - skip existing keys (default)
  overwrite - overwrite existing keys`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := LoadConfig().GetBaseConfig()

		inputFile, err := cmd.Flags().GetString("input")
		if err != nil {
			return err
		}

		sectionsFlag, err := cmd.Flags().GetString("sections")
		if err != nil {
			return err
		}

		strategyFlag, err := cmd.Flags().GetString("strategy")
		if err != nil {
			return err
		}

		if inputFile == "" {
			return errNoInputFile
		}

		// Parse sections (comma-separated)
		var sections []string
		if sectionsFlag != "" {
			sections = strings.Split(sectionsFlag, ",")
			for i, s := range sections {
				sections[i] = strings.TrimSpace(s)
			}
		}
		// Empty default means all sections in the dump

		// Parse strategy
		strategy := kv.LoadStrategy(strategyFlag)
		switch strategy {
		case kv.LoadStrategyOverwrite, kv.LoadStrategySkip:
		default:
			return fmt.Errorf("%w: %s (must be one of: overwrite or skip)", errInvalidStrategy, strategyFlag)
		}

		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			return fmt.Errorf("KV params: %w", err)
		}

		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			return fmt.Errorf("failed to open KV store: %w", err)
		}
		defer kvStore.Close()

		// Read dump file
		var input *os.File
		if inputFile == "-" {
			input = os.Stdin
		} else {
			input, err = os.Open(inputFile)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer func() {
				if err := input.Close(); err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close input file: %v\n", err)
				}
			}()
		}

		var dump kv.DumpFormat
		decoder := json.NewDecoder(input)
		if err := decoder.Decode(&dump); err != nil {
			return fmt.Errorf("failed to decode dump: %w", err)
		}

		// Load dump
		if err := kv.LoadDump(ctx, kvStore, &dump, sections, strategy); err != nil {
			return fmt.Errorf("failed to load dump: %w", err)
		}

		_, _ = fmt.Fprintf(os.Stderr, "Successfully loaded dump\n")
		return nil
	},
}

// createRepoPartitionDump creates a dump of a specific repository's partition
func createRepoPartitionDump(ctx context.Context, kvStore kv.Store, repoName string) (*kv.DumpFormat, error) {
	// Get repository record from graveler partition
	repoID := graveler.RepositoryID(repoName)
	repoPath := graveler.RepoPath(repoID)

	var repoData graveler.RepositoryData
	_, err := kv.GetMsg(ctx, kvStore, graveler.RepositoriesPartition(), []byte(repoPath), &repoData)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: %s", errRepoNotFound, repoName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get repository: %w", err)
	}

	// Convert to RepositoryRecord to compute partition
	repo := graveler.RepoFromProto(&repoData)
	partition := graveler.RepoPartition(repo)

	// Dump the repository partition
	return kv.CreateDumpWithPartitions(ctx, kvStore, []string{partition})
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(kvCmd)

	kvCmd.AddCommand(kvGetCmd)
	kvGetCmd.Flags().Bool("pretty", false, "print indented output")

	kvCmd.AddCommand(kvScanCmd)
	kvScanCmd.Flags().Int("limit", 0, "maximal number of results to return. By default, all results are returned")
	kvScanCmd.Flags().String("until", "", "last prefix to scan. If this prefix is reached or exceeded, scan will stop")
	kvScanCmd.Flags().Bool("pretty", false, "print indented output")

	kvCmd.AddCommand(kvDumpCmd)
	kvDumpCmd.Flags().String("output", "", "output file (default: stdout)")
	kvDumpCmd.Flags().String("sections", "", "comma-separated list of sections to dump (empty dumps all)")
	kvDumpCmd.Flags().Bool("all", false, "dump all known partitions (not just predefined sections)")
	kvDumpCmd.Flags().String("repo", "", "dump partition for a specific repository")
	kvDumpCmd.Flags().Bool("pretty", false, "print indented output")

	kvCmd.AddCommand(kvLoadCmd)
	kvLoadCmd.Flags().String("input", "", "dump input file (produced by 'kv dump')")
	kvLoadCmd.Flags().String("sections", "", "comma-separated list of sections to load (empty dumps all)")
	kvLoadCmd.Flags().String("strategy", "skip", "skip or overwrite existing keys")
	_ = kvLoadCmd.MarkFlagRequired("input")
}
