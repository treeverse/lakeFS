package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	osssstable "github.com/treeverse/lakefs/pkg/graveler/sstable"
	"github.com/treeverse/lakefs/pkg/logging"
	"golang.org/x/sync/errgroup"
)

const workersNum = 10

var logger = logging.ContextUnavailable()

var metadataCountCmd = &cobra.Command{
	Use:               "count <repository URI>",
	Short:             "Count active metadata size",
	Long:              "Calculate the total size of active metadata (metaranges and ranges) in a repository",
	Example:           `lakectl metadata count lakefs://my-repo`,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository URI", args[0])
		ctx := cmd.Context()
		client := getClient()

		// Get repository info
		repoResp, err := client.GetRepositoryWithResponse(ctx, u.Repository)
		DieOnErrorOrUnexpectedStatusCode(repoResp, err, http.StatusOK)
		if repoResp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		repo := repoResp.JSON200

		logger.WithFields(logging.Fields{
			"repository":        repo.Id,
			"storage_namespace": repo.StorageNamespace,
			"default_branch":    repo.DefaultBranch,
		}).Info("Repository info")

		// Initialize S3 client using default credentials from environment
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			DieFmt("Failed to load AWS config: %s", err)
		}
		s3Client := s3.NewFromConfig(cfg)

		// Collect active metaranges from all branches
		logger.Info("Collecting active metaranges from all branches...")
		var activeMetaRanges sync.Map
		err = getActiveMetaRangesFromAPI(ctx, client, u.Repository, repo.DefaultBranch, &activeMetaRanges)
		if err != nil {
			DieFmt("Failed to collect active metaranges: %s", err)
		}

		// Remove empty metarange (initial commit)
		activeMetaRanges.Delete("")
		totalActiveMetaRanges := syncMapLen(&activeMetaRanges)
		logger.WithField("count", totalActiveMetaRanges).Info("Found unique active metaranges")

		// Get storage namespace details for S3 bucket/prefix
		storageURL, err := url.Parse(repo.StorageNamespace)
		if err != nil {
			DieFmt("Failed to parse storage namespace: %s", err)
		}

		bucket := storageURL.Host

		// Collect active ranges
		logger.Info("Collecting active ranges...")
		activeRanges, totalSize, err := collectActiveRanges(ctx, s3Client, bucket, storageURL.Path, &activeMetaRanges)
		if err != nil {
			DieFmt("Failed to collect active ranges: %s", err)
		}
		logger.Debug(PrintSyncMap(&activeMetaRanges))

		// Output results
		logger.WithFields(logging.Fields{
			"active_metaranges": totalActiveMetaRanges,
			"active_ranges":     activeRanges,
			"total_size_bytes":  totalSize,
			"total_size_human":  prettyByteSize(totalSize),
		}).Info("Active metadata count completed")
	},
}

func PrintSyncMap(m *sync.Map) string {
	// print map,
	out := ""
	i := 0
	m.Range(func(key, value interface{}) bool {
		out += fmt.Sprintf("\t[%d] key: %v, value: %v\n", i, key, value)
		i++
		return true
	})
	return out
}

func syncMapLen(m *sync.Map) int {
	var count int
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func getActiveMetaRangesFromAPI(ctx context.Context, client *apigen.ClientWithResponses, repoID, defaultBranch string, activeMetaRanges *sync.Map) error {
	logger := logging.FromContext(ctx)
	var seenCommits sync.Map

	// Collect commits from default branch first
	logger.WithField("branch", defaultBranch).Debug("Collecting commits from default branch")

	err := collectCommitsFromBranch(ctx, client, repoID, defaultBranch, activeMetaRanges, &seenCommits)
	if err != nil {
		return fmt.Errorf("failed to collect commits from default branch: %w", err)
	}

	// List all branches
	var branches []string
	var after apigen.PaginationAfter
	for {
		resp, err := client.ListBranchesWithResponse(ctx, repoID, &apigen.ListBranchesParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(1000)),
			After:  &after,
		})
		if err != nil || resp.JSON200 == nil {
			return fmt.Errorf("failed to list branches: %w", err)
		}

		for _, branch := range resp.JSON200.Results {
			if branch.Id != defaultBranch {
				branches = append(branches, branch.Id)
			}
		}

		if !resp.JSON200.Pagination.HasMore {
			break
		}
		after = apigen.PaginationAfter(resp.JSON200.Pagination.NextOffset)
	}

	// Create channel for branch processing
	branchChan := make(chan string, workersNum)
	grp, ctx := errgroup.WithContext(ctx)

	// Producer: send branches to channel
	grp.Go(func() error {
		defer close(branchChan)
		for _, branchID := range branches {
			select {
			case branchChan <- branchID:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	// Consumers: process branches in parallel
	for i := 0; i < workersNum; i++ {
		grp.Go(func() error {
			for branchID := range branchChan {
				logger.WithField("branch", branchID).Debug("Collecting commits from branch")
				err := collectCommitsFromBranch(ctx, client, repoID, branchID, activeMetaRanges, &seenCommits)
				if err != nil {
					return fmt.Errorf("failed to collect commits from branch %s: %w", branchID, err)
				}
			}
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

func collectCommitsFromBranch(ctx context.Context, client *apigen.ClientWithResponses, repoID, branchID string, activeMetaRanges *sync.Map, seenCommits *sync.Map) error {
	var after apigen.PaginationAfter

	for {
		resp, err := client.LogCommitsWithResponse(ctx, repoID, branchID, &apigen.LogCommitsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(1000)),
			After:  &after,
		})
		if err != nil || resp.JSON200 == nil {
			return fmt.Errorf("failed to log commits: %w", err)
		}

		for _, commit := range resp.JSON200.Results {
			// If we've seen this commit before, we can stop (shared history)
			//commitID := commit.Id
			//if _, seen := seenCommits.LoadOrStore(commitID, struct{}{}); seen {
			//	return nil
			//}

			activeMetaRanges.Store(commit.MetaRangeId, int64(0))
		}

		if !resp.JSON200.Pagination.HasMore {
			return nil
		}
		after = apigen.PaginationAfter(resp.JSON200.Pagination.NextOffset)
	}
}

func collectActiveRanges(ctx context.Context, s3Client *s3.Client, bucket, prefix string, activeMetaRanges *sync.Map) (int, int64, error) {
	var (
		activeRanges sync.Map
		mu           sync.Mutex
		totalSize    int64
	)

	// Create channel for metarange processing
	metaRangeChan := make(chan string, workersNum)
	grp, ctx := errgroup.WithContext(ctx)
	// Producer: send metaranges to channel
	grp.Go(func() error {
		defer close(metaRangeChan)
		activeMetaRanges.Range(func(key, value any) bool {
			select {
			case metaRangeChan <- key.(string):
			case <-ctx.Done():
				return false
			}
			return true
		})
		return ctx.Err()
	})

	// Consumers: process metaranges
	for i := 0; i < workersNum; i++ {
		grp.Go(func() error {
			for metaRangeID := range metaRangeChan {
				if metaRangeID == "" {
					continue
				}

				metaRangeSize, err := processMetaRange(ctx, s3Client, bucket, prefix, metaRangeID, &activeRanges)
				if err != nil {
					return fmt.Errorf("failed to process metarange %s: %w", metaRangeID, err)
				}

				mu.Lock()
				totalSize += metaRangeSize
				mu.Unlock()
			}
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return 0, 0, err
	}

	// Count and sum range sizes
	rangeCount := 0
	activeRanges.Range(func(key, value any) bool {
		rangeCount++
		totalSize += value.(int64)
		return true
	})

	logger.Debug(PrintSyncMap(&activeRanges))

	return rangeCount, totalSize, nil
}

func processMetaRange(ctx context.Context, s3Client *s3.Client, bucket, prefix, metaRangeID string, activeRanges *sync.Map) (int64, error) {
	// Check context before starting
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Build S3 key for metarange
	key := fmt.Sprintf("%s/_lakefs/%s", prefix, metaRangeID)
	if key[0] == '/' {
		key = key[1:] // Remove leading slash
	}

	// Get metarange object size
	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to head metarange object: %w", err)
	}

	metaRangeSize := aws.ToInt64(headResp.ContentLength)

	// Download and parse metarange to get range IDs
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get metarange object: %w", err)
	}
	defer getResp.Body.Close()

	// Create temporary file for sstable reader
	tmpFile, err := os.CreateTemp("", "metarange-*.sst")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy S3 object to temp file
	_, err = io.Copy(tmpFile, getResp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to copy metarange data: %w", err)
	}

	// Seek back to start for reading
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Open sstable reader - os.File implements the required interface
	sstReader, err := sstable.NewReader(tmpFile, sstable.ReaderOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to create sstable reader: %w", err)
	}
	defer sstReader.Close()

	// Iterate through metarange to get range IDs
	iter, err := sstReader.NewIter(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}

	it := osssstable.NewIterator(iter, func() error { return nil })

	// Ensure cleanup happens only once
	var closeOnce sync.Once
	var closeErr error
	doClose := func() {
		closeOnce.Do(func() {
			it.Close()
		})
	}
	defer doClose()

	// Process ranges
	for it.Next() {
		// Check context periodically
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		r, err := committed.UnmarshalValue(it.Value().Value)
		if err != nil {
			return 0, fmt.Errorf("failed to unmarshal range: %w", err)
		}

		rangeID := string(r.Identity)

		// Get range size
		rangeKey := fmt.Sprintf("%s/_lakefs/%s", prefix, rangeID)
		if rangeKey[0] == '/' {
			rangeKey = rangeKey[1:]
		}

		rangeHead, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(rangeKey),
		})
		if err != nil {
			// Range might not exist or context canceled, skip
			logging.FromContext(ctx).WithFields(logging.Fields{
				"range_id": rangeID,
				"error":    err,
			}).Warn("Failed to get range")
			continue
		}

		activeRanges.Store(rangeID, aws.ToInt64(rangeHead.ContentLength))
	}

	// Close explicitly before checking errors (will only close once due to sync.Once)
	doClose()

	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}

	if closeErr != nil {
		return 0, fmt.Errorf("iterator close error: %w", closeErr)
	}

	return metaRangeSize, nil
}

func prettyByteSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

//nolint:gochecknoinits
func init() {
	metadataCmd.AddCommand(metadataCountCmd)
}
