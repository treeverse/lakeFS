package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prom2json"
	"github.com/spf13/viper"
	"github.com/thanhpk/randstr"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/objects"
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

	scrapePrometheus()
}

const (
	contentSuffixLength = 32
	contentLength       = 1 * 1024
)

func testBenchmarkLakeFS() error {
	ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration("global_timeout"))
	defer cancel()

	ns := viper.GetString("storage_namespace")
	repoName := strings.ToLower("benchmarks")
	logger.WithFields(logging.Fields{
		"repository":        repoName,
		"storage_namespace": ns,
		"name":              repoName,
	}).Debug("Create repository for test")
	_, err := client.Repositories.CreateRepository(repositories.NewCreateRepositoryParamsWithContext(ctx).
		WithRepository(&models.RepositoryCreation{
			DefaultBranch:    "master",
			ID:               swag.String(repoName),
			StorageNamespace: swag.String(ns),
		}), nil)
	if err != nil {
		return fmt.Errorf("failed to create repository, storage '%s': %w", ns, err)
	}

	parallelism := viper.GetInt("parallelism_level")
	filesAmount := viper.GetInt("files_amount")

	contentPrefix := randstr.Hex(contentLength - contentSuffixLength)
	failed := doInParallel(ctx, repoName, parallelism, filesAmount, contentPrefix, uploader)
	logger.WithField("failedCount", failed).Info("Finished uploading files")

	failed = doInParallel(ctx, repoName, parallelism, filesAmount, "", reader)
	logger.WithField("failedCount", failed).Info("Finished reading files")

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

			if err := linearRetry(func() error {
				_, err := client.Objects.UploadObject(
					objects.NewUploadObjectParamsWithContext(ctx).
						WithRepository(repoName).
						WithBranch("master").
						WithPath(file).
						WithContent(contentReader), nil)
				return err
			}); err != nil {
				failed++
				logger.WithField("fileNum", file).Error("Failed uploading file")
			}
		}
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

			if err := linearRetry(func() error {
				var b bytes.Buffer
				_, err := client.Objects.GetObject(
					objects.NewGetObjectParamsWithContext(ctx).
						WithRepository(repoName).
						WithRef("master").
						WithPath(file), nil, &b)
				return err
			}); err != nil {
				failed++
				logger.WithField("fileNum", file).Error("Failed reading file")
			}
		}
	}
}

const (
	tries        = 3
	retryTimeout = 200 * time.Millisecond
)

func linearRetry(do func() error) error {
	var err error
	for i := 1; i <= tries; i++ {
		if err = do(); err == nil {
			return nil
		}

		if i != tries {
			// skip sleep in the last iteration
			time.Sleep(retryTimeout)
		}
	}
	return err
}

var monitoredOps = map[string]bool{
	"getObject":    true,
	"uploadObject": true,
}

func scrapePrometheus() {
	lakefsEndpoint := viper.GetString("endpoint_url")
	resp, err := http.DefaultClient.Get(lakefsEndpoint + "/metrics")
	if err != nil {
		panic(err)
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	ch := make(chan *dto.MetricFamily)
	go func() { _ = prom2json.ParseResponse(resp, ch) }()
	metrics := []*dto.Metric{}

	for {
		a, ok := <-ch
		if !ok {
			break
		}

		if *a.Name == "api_request_duration_seconds" {
			for _, m := range a.Metric {
				for _, label := range m.Label {
					if *label.Name == "operation" && monitoredOps[*label.Value] {
						metrics = append(metrics, m)
					}
				}
			}
		}
	}

	// TODO: report instead of print
	for _, m := range metrics {
		fmt.Printf("%v\n", *m)
	}
}
