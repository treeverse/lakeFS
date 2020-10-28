package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/api/gen/client/commits"

	retry "github.com/avast/retry-go"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/thanhpk/randstr"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/testutil"
)

var (
	logger logging.Logger
	client *genclient.Lakefs
)

const (
	defaultGlobalTimeout    = 30 * time.Minute
	defaultFiles            = 50000
	defaultParallelismLevel = 500
)

func main() {
	viper.SetDefault("parallelism_level", defaultParallelismLevel)
	viper.SetDefault("files_amount", defaultFiles)
	viper.SetDefault("global_timeout", defaultGlobalTimeout)

	logger, client, _ = testutil.SetupTestingEnv("benchmark", "lakefs-benchmarking")
	logger.Info("Setup succeeded, running the tests")

	if err := testBenchmarkLakeFS(); err != nil {
		logger.WithError(err).Error("Tests run failed")
		os.Exit(-1)
	}
}

const (
	contentSuffixLength = 32
	contentLength       = 1 * 1024
	branchName          = "branch-1"
	repoName            = "benchmarks"
)

func testBenchmarkLakeFS() error {
	ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration("global_timeout"))
	defer cancel()

	ns := viper.GetString("storage_namespace")
	logger.WithFields(logging.Fields{
		"repository":        repoName,
		"storage_namespace": ns,
		"name":              repoName,
	}).Debug("Create repository for test")
	repo, err := client.Repositories.CreateRepository(repositories.NewCreateRepositoryParamsWithContext(ctx).
		WithRepository(&models.RepositoryCreation{
			DefaultBranch:    "master",
			Name:             swag.String(repoName),
			StorageNamespace: swag.String(ns),
		}), nil)
	if err != nil {
		return fmt.Errorf("failed to create repository, storage '%s': %w", ns, err)
	}

	_, err = client.Branches.CreateBranch(&branches.CreateBranchParams{
		Branch: &models.BranchCreation{
			Source: swag.String("master"),
			Name:   swag.String(branchName),
		},
		Repository: repo.Payload.ID,
		Context:    ctx,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to create a branch from master: %w", err)
	}

	parallelism := viper.GetInt("parallelism_level")
	filesAmount := viper.GetInt("files_amount")

	contentPrefix := randstr.Hex(contentLength - contentSuffixLength)
	failed := doInParallel(ctx, repoName, parallelism, filesAmount, contentPrefix, uploader)
	logger.WithField("failedCount", failed).Info("Finished uploading files")

	failed = doInParallel(ctx, repoName, parallelism, filesAmount, "", reader)
	logger.WithField("failedCount", failed).Info("Finished reading files")

	_, err = client.Commits.Commit(&commits.CommitParams{
		Branch: branchName,
		Commit: &models.CommitCreation{
			Message: swag.String("commit before merge"),
		},
		Repository: repoName,
		Context:    ctx,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	merge(ctx)

	return nil
}

func doInParallel(ctx context.Context, repoName string, level, filesAmount int, contentPrefix string, do func(context.Context, chan string, string, string) int) int {
	filesCh := make(chan string, level)
	wg := sync.WaitGroup{}
	var failed int64

	for i := 0; i < level; i++ {
		wg.Add(1)
		go func() {
			fail := do(ctx, filesCh, repoName, contentPrefix)
			atomic.AddInt64(&failed, int64(fail))
			wg.Done()
		}()
	}

	for i := 1; i <= filesAmount; i++ {
		filesCh <- strconv.Itoa(i)
	}

	close(filesCh)
	wg.Wait()

	return int(failed)
}

const (
	retryAttempts = 3
	retryDelay    = 200 * time.Millisecond
)

func uploader(ctx context.Context, ch chan string, repoName, contentPrefix string) int {
	failed := 0
	for {
		select {
		case <-ctx.Done():
			return failed
		case file, ok := <-ch:
			if !ok {
				// channel closed
				return failed
			}

			// Making sure content isn't duplicated to avoid dedup mechanisms in lakeFS
			content := contentPrefix + randstr.Hex(contentSuffixLength)
			contentReader := runtime.NamedReader("content", strings.NewReader(content))

			if err := retry.Do(func() error {
				_, err := client.Objects.UploadObject(
					objects.NewUploadObjectParamsWithContext(ctx).
						WithRepository(repoName).
						WithBranch(branchName).
						WithPath(file).
						WithContent(contentReader), nil)
				return err
			}, retry.Attempts(retryAttempts),
				retry.Delay(retryDelay),
				retry.LastErrorOnly(true),
				retry.DelayType(retry.FixedDelay)); err != nil {
				failed++
				logger.WithField("fileNum", file).Error("Failed uploading file")
			}
		}
	}
}

func merge(ctx context.Context) {
	err := retry.Do(func() error {
		_, err := client.Refs.MergeIntoBranch(&refs.MergeIntoBranchParams{
			DestinationRef: "master",
			Merge: &models.Merge{
				Message: "merging all objects to master",
			},
			Repository: repoName,
			SourceRef:  branchName,
			Context:    ctx,
		}, nil)
		return err
	}, retry.Attempts(retryAttempts),
		retry.Delay(retryDelay),
		retry.LastErrorOnly(true),
		retry.DelayType(retry.FixedDelay))
	if err != nil {
		logger.WithError(err).Error("Failed merging branch to master")
	}
}

func reader(ctx context.Context, ch chan string, repoName, _ string) int {
	failed := 0
	for {
		select {
		case <-ctx.Done():
			return failed
		case file, ok := <-ch:
			if !ok {
				// channel closed
				return failed
			}

			if err := retry.Do(func() error {
				var b bytes.Buffer
				_, err := client.Objects.GetObject(
					objects.NewGetObjectParamsWithContext(ctx).
						WithRepository(repoName).
						WithRef(branchName).
						WithPath(file), nil, &b)
				return err
			}, retry.Attempts(retryAttempts),
				retry.Delay(retryDelay),
				retry.LastErrorOnly(true),
				retry.DelayType(retry.FixedDelay)); err != nil {
				failed++
				logger.WithField("fileNum", file).Error("Failed reading file")
			}
		}
	}
}
