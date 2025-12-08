package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

var gcCmd = &cobra.Command{
	Use:    "gc",
	Hidden: true,
	Short:  "Manually run garbage collection",
}

const (
	MinBranchesCapacity               = 100
	MinCommitsCapacity                = 10000
	MinGarbageCollectionRulesCapacity = 10
)

// DumpHeader[T] is the JSON structure to expect for an object holding a T.
type DumpHeader[T any] struct {
	Partition string `json:"partition"`
	Key       string `json:"key"`
	Value     T      `json:"value"`
}

var (
	ErrMultiplePartitions = errors.New("multiple partitions in input")
)

// kvFromJSON fills a KV from JSON records
type kvFromJSON struct {
	ctx context.Context
	// Store is where to store data.
	Store kv.Store
	// Partition is the name of the repository partition loaded.  It is an error to read
	// multiple repository partitions.
	Partition string
}

func (k *kvFromJSON) partition(p string) error {
	if k.Partition == "" {
		k.Partition = p
		return nil
	}
	if k.Partition != p {
		return fmt.Errorf("%w: %s, %s", ErrMultiplePartitions, k.Partition, p)
	}
	return nil
}

func (k *kvFromJSON) parseCommit(data *DumpHeader[graveler.CommitData]) error {
	k.partition(data.Partition)
	return kv.SetMsg(k.ctx, k.Store, data.Partition, []byte(data.Key), &data.Value)
}

func (k *kvFromJSON) parseBranch(data *DumpHeader[graveler.BranchData]) error {
	k.partition(data.Partition)
	return kv.SetMsg(k.ctx, k.Store, data.Partition, []byte(data.Key), &data.Value)
}

// "lakefs kv" incorrectly codes some fields (#9761) as StagedEntryData.  Just skip those.
func (k *kvFromJSON) ignoreOthers(data *DumpHeader[graveler.StagedEntryData]) error {
	k.partition(data.Partition)
	return nil
}

// ReadBranchesAndCommits reads branches and commits from r into store.
func ReadBranchesAndCommits(ctx context.Context, r io.Reader, store kv.Store) (string, error) {
	convertor := kvFromJSON{
		ctx:   ctx,
		Store: store,
	}

	var (
		lineNum int
		err     error
		reader  = bufio.NewReader(r)
	)
	for {
		err = ReadJSON(reader, convertor.parseCommit, convertor.parseBranch, convertor.ignoreOthers)
		if err != nil {
			return "", fmt.Errorf("%d: %w", lineNum, err)
		}
		lineNum++
	}
	return convertor.Partition, nil
}

var simulateCmd = &cobra.Command{
	Use:   "simulate rules.json partition-dump.json",
	Short: "Simulate a GC run on locally-available files",
	Long: `Run garbage collection on locally-available files.

First arg "rules.json" is a file containing the JSON retention rules.  Second arg
"partition-dump" is a file containing the entire JSON scan of the branch partition on KV.
Create it with "lakefs kv scan <reponame>-ID".  To discover the ID try "lakefs kv scan graveler"
and look for the instance_uid field.
`,
	Args: cobra.ExactArgs(2), Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		rulesFile, err := os.Open(args[0])
		if err != nil {
			printMsgAndExit(fmt.Errorf("open rules %s: %w", args[0], err))
		}
		var rules graveler.GarbageCollectionRules
		dec := json.NewDecoder(rulesFile)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rules); err != nil {
			printMsgAndExit(fmt.Errorf("read rules %s: %w", args[0], err))
		}
		rulesFile.Close()

		store, err := kv.Open(ctx, kvparams.Config{
			Type: mem.DriverName,
		})
		if err != nil {
			printMsgAndExit(fmt.Errorf("create mem KV: %w", err))
		}

		partitionDumpFile, err := os.Open(args[1])
		if err != nil {
			printMsgAndExit(fmt.Errorf("open partition dump %s: %w", args[1], err))
		}
		repoID, err := ReadBranchesAndCommits(ctx, partitionDumpFile, store)
		if err != nil && !errors.Is(err, io.EOF) {
			printMsgAndExit(fmt.Errorf("read partition dump %s: %w", args[1], err))
		}

		repo := &graveler.RepositoryRecord{RepositoryID: graveler.RepositoryID(repoID)}

		commitIterator, err := refManager.GCCommi
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(gcCmd)
	gcCmd.AddCommand(simulateCmd)
}
