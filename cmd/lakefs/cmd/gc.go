package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/retention"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

var gcCmd = &cobra.Command{
	Use:    "gc",
	Hidden: true,
	Short:  "Manually run prepare GC commits",
}

const (
	MinBranchesCapacity               = 100
	MinCommitsCapacity                = 10000
	MinGarbageCollectionRulesCapacity = 10

	pprofReadHeaderTimeout = 120 * time.Second
)

// DumpHeader[T] is the JSON structure to expect for an object holding a T.
type DumpHeader[T any] struct {
	Partition string `json:"partition"`
	Key       string `json:"key"`
	Value     T      `json:"value"`
}

var (
	ErrMultiplePartitions = errors.New("multiple partitions in input")
	ErrBadPartitionFormat = errors.New("bad partition format")
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
	err := k.partition(data.Partition)
	if err != nil {
		return err
	}
	return kv.SetMsg(k.ctx, k.Store, data.Partition, []byte(data.Key), &data.Value)
}

func (k *kvFromJSON) parseBranch(data *DumpHeader[graveler.BranchData]) error {
	err := k.partition(data.Partition)
	if err != nil {
		return err
	}
	return kv.SetMsg(k.ctx, k.Store, data.Partition, []byte(data.Key), &data.Value)
}

// "lakefs kv" incorrectly codes some fields (#9761) as StagedEntryData.  Just skip those.
func (k *kvFromJSON) ignoreOthers(data *DumpHeader[graveler.StagedEntryData]) error {
	return k.partition(data.Partition)
}

// ReadBranchesAndCommits reads branches and commits from r into store.  It returns the
// repository ID and its UID.
func ReadBranchesAndCommits(ctx context.Context, r io.Reader, store kv.Store) (string, string, error) {
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
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", "", fmt.Errorf("%d: %w", lineNum, err)
		}
		lineNum++
	}

	partition := convertor.Partition
	index := strings.LastIndex(partition, "-")
	if index < 0 {
		return "", "", fmt.Errorf("%w %s", ErrBadPartitionFormat, partition)
	}
	return partition[0:index], partition[index+1:], nil
}

type RefManager struct {
	store        kv.Store
	repositoryID string
}

func (r *RefManager) GCBranchIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return ref.NewBranchByCommitIterator(ctx, r.store, repository, graveler.ListOptions{ShowHidden: true})
}

func (r *RefManager) GCCommitIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return ref.NewOrderedCommitIterator(ctx, r.store, repository, true)
}

func (r *RefManager) ListCommits(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return ref.NewOrderedCommitIterator(ctx, r.store, repository, false)
}

func (r *RefManager) GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	var data graveler.CommitData

	if _, err := kv.GetMsg(ctx, r.store, r.repositoryID, []byte(graveler.CommitPath(commitID)), &data); err != nil {
		return nil, fmt.Errorf("%s: %w", commitID, err)
	}
	commit := graveler.CommitFromProto(&data)
	return commit, nil
}

var simulateCmd = &cobra.Command{
	Use:   "simulate rules.json partition-dump.json",
	Short: "Simulate prepare GC commits on locally-available files",
	Long: `Run garbage collection on locally-available files.

First arg "rules.json" is a file containing the JSON retention rules.  Second arg
"partition-dump" is a file containing the entire JSON scan of the branch partition on KV.
Create it with "lakefs kv scan <reponame>-ID".  To discover the ID try "lakefs kv scan graveler"
and look for the instance_uid field.
`,
	//nolint:mnd
	Args: cobra.ExactArgs(2), Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()

		output := os.Stdout
		outputFilename, err := cmd.Flags().GetString("output")
		if err != nil {
			printMsgAndExit(fmt.Errorf("output: %w", err))
		}
		if outputFilename != "" {
			output, err = os.Create(outputFilename)
			if err != nil {
				printMsgAndExit(fmt.Errorf("open %s: %w", outputFilename, err))
			}
			defer output.Close()
		}

		cpuProfileFilename, err := cmd.Flags().GetString("cpuprofile")
		if err != nil {
			printMsgAndExit(fmt.Errorf("cpuprofile: %w", err))
		}
		var cpuProfileFile *os.File
		if cpuProfileFilename != "" {
			cpuProfileFile, err = os.Create(cpuProfileFilename)
			if err != nil {
				printMsgAndExit(fmt.Errorf("open %s: %w", cpuProfileFilename, err))
			}
			defer cpuProfileFile.Close()
			// Will start profiling later.
		}
		memProfileFilename, err := cmd.Flags().GetString("memprofile")
		if err != nil {
			printMsgAndExit(fmt.Errorf("memprofile: %w", err))
		}
		var memProfileFile *os.File
		if memProfileFilename != "" {
			memProfileFile, err = os.Create(memProfileFilename)
			if err != nil {
				printMsgAndExit(fmt.Errorf("open %s: %w", memProfileFilename, err))
			}
			defer memProfileFile.Close()
		}

		// Allow profiling over a web-server, too.
		cfg := LoadConfig()
		baseCfg := cfg.GetBaseConfig()
		pprofHandler := httputil.ServePPROF("/_pprof/")
		server := http.Server{
			Addr:              baseCfg.ListenAddress,
			Handler:           pprofHandler,
			ReadHeaderTimeout: pprofReadHeaderTimeout,
		}
		go func() {
			_ = server.ListenAndServe() // this server never exits.
		}()

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
		repositoryID, uid, err := ReadBranchesAndCommits(ctx, partitionDumpFile, store)
		if err != nil && !errors.Is(err, io.EOF) {
			printMsgAndExit(fmt.Errorf("read partition dump %s: %w", args[1], err))
		}

		fmt.Fprintf(os.Stderr, "repositoryID: %s\tUID: %s\n", repositoryID, uid)

		if cpuProfileFile != nil {
			if err = pprof.StartCPUProfile(cpuProfileFile); err != nil {
				printMsgAndExit(fmt.Errorf("start CPU profile: %w", err))
			}
			defer func() {
				pprof.StopCPUProfile()
				fmt.Fprintln(os.Stderr, "wrote cpuprofile")
			}()
		}

		repository := &graveler.RepositoryRecord{
			RepositoryID: graveler.RepositoryID(repositoryID),
			Repository:   &graveler.Repository{InstanceUID: uid},
		}

		refManager := &RefManager{store, repositoryID}

		commitGetter := &retention.RepositoryCommitGetterAdapter{
			RefManager: refManager,
			Repository: repository,
		}
		branchIterator, err := refManager.GCBranchIterator(ctx, repository)
		if err != nil {
			printMsgAndExit(err)
		}
		defer branchIterator.Close()
		// get all commits that are not the first parent of any commit:
		commitIterator, err := refManager.GCCommitIterator(ctx, repository)
		if err != nil {
			printMsgAndExit(fmt.Errorf("create kv ordered commit iterator commits: %w", err))
		}
		defer commitIterator.Close()
		startingPointIterator := retention.NewGCStartingPointIterator(commitIterator, branchIterator)
		defer startingPointIterator.Close()
		gcCommits, err := retention.GetGarbageCollectionCommits(ctx, startingPointIterator, commitGetter, &rules)
		if err != nil {
			printMsgAndExit(fmt.Errorf("find expired commits: %w", err))
		}

		for commitID, metaRangeID := range gcCommits {
			fmt.Fprintf(output, "%s [%s]\n", commitID, metaRangeID)
		}

		if memProfileFile != nil {
			// Alternative: "allocs".
			if err = pprof.Lookup("heap").WriteTo(memProfileFile, 0); err != nil {
				printMsgAndExit(fmt.Errorf("write mem profile: %w", err))
			}
			fmt.Fprintln(os.Stderr, "wrote memprofile")
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(gcCmd)
	f := simulateCmd.Flags()
	f.StringP("output", "o", "", "Write output to this file")
	f.String("cpuprofile", "", "If set, write CPU profile of GC (without time to read inputs) to this file.")
	f.String("memprofile", "", "If set, write memory profile of GC (without time to read inputs) to this file.")
	gcCmd.AddCommand(simulateCmd)
}
