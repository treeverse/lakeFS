package main

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go"
	"github.com/spf13/viper"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	logger logging.Logger
	client api.ClientWithResponsesInterface
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

	logger, client, _ = testutil.SetupTestingEnv(&testutil.SetupTestingEnvParams{
		Name:      "benchmark",
		StorageNS: "lakefs-benchmarking",
	})
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
	createRepoResp, err := client.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
		DefaultBranch:    api.StringPtr("main"),
		Name:             repoName,
		StorageNamespace: ns,
	})
	if err != nil {
		return fmt.Errorf("failed to create repository, storage '%s': %w", ns, err)
	}
	if err := helpers.ResponseAsError(createRepoResp); err != nil {
		return fmt.Errorf("create repository: %w", err)
	}

	repo := createRepoResp.JSON201
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo.Id, api.CreateBranchJSONRequestBody{
		Name:   branchName,
		Source: "main",
	})
	if err != nil {
		return fmt.Errorf("failed to create a branch from main: %w", err)
	}
	if err := helpers.ResponseAsError(createBranchResp); err != nil {
		return fmt.Errorf("create branch: %w", err)
	}

	parallelism := viper.GetInt("parallelism_level")
	filesAmount := viper.GetInt("files_amount")

	contentPrefix := randstr.Hex(contentLength - contentSuffixLength)
	failed := doInParallel(ctx, repoName, parallelism, filesAmount, contentPrefix, uploader)
	logger.WithField("failedCount", failed).Info("Finished uploading files")

	failed = doInParallel(ctx, repoName, parallelism, filesAmount, "", reader)
	logger.WithField("failedCount", failed).Info("Finished reading files")

	commitResp, err := client.CommitWithResponse(ctx, repo.Id, branchName, api.CommitJSONRequestBody{
		Message: "commit before merge",
	})
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	if err := helpers.ResponseAsError(commitResp); err != nil {
		return fmt.Errorf("commit: %w", err)
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
			var b bytes.Buffer
			w := multipart.NewWriter(&b)
			contentWriter, err := w.CreateFormFile("content", filepath.Base(file))
			if err != nil {
				logger.WithError(err).Error("CreateFormFile failed")
				return failed
			}
			_, err = contentWriter.Write([]byte(content))
			if err != nil {
				logger.WithError(err).Error("CreateFormFile write content failed")
				return failed
			}
			w.Close()
			contentType := w.FormDataContentType()

			if err := retry.Do(func() error {
				resp, err := client.UploadObjectWithBodyWithResponse(ctx, repoName, branchName,
					&api.UploadObjectParams{Path: file},
					contentType,
					&b)
				if err != nil {
					return err
				}
				if err = helpers.ResponseAsError(resp); err != nil {
					return fmt.Errorf("upload object: %w", err)
				}
				return nil
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
		resp, err := client.MergeIntoBranchWithResponse(ctx, repoName, branchName, "main", api.MergeIntoBranchJSONRequestBody{
			Message: api.StringPtr("merging all objects to main"),
		})
		if err != nil {
			return err
		}
		if err = helpers.ResponseAsError(resp); err != nil {
			return fmt.Errorf("merge: %w", err)
		}
		return nil
	}, retry.Attempts(retryAttempts),
		retry.Delay(retryDelay),
		retry.LastErrorOnly(true),
		retry.DelayType(retry.FixedDelay))
	if err != nil {
		logger.WithError(err).Error("Failed merging branch to main")
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
				resp, err := client.GetObjectWithResponse(ctx, repoName, branchName, &api.GetObjectParams{Path: file})
				if err != nil {
					return err
				}
				if err = helpers.ResponseAsError(resp); err != nil {
					return fmt.Errorf("get object: %w", err)
				}
				return nil
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

type APIError struct {
	Action  string
	Message string
}

func (a *APIError) Error() string {
	return fmt.Sprintf("%s failed: %s", a.Action, a.Message)
}
