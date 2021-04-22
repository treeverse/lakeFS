package gateway_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/gateway/simulator"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type dependencies struct {
	blocks  block.Adapter
	auth    simulator.GatewayAuthService
	catalog catalog.Interface
}

type mockCollector struct{}

const (
	RecordingsDir        = "testdata/recordings"
	ReplayRepositoryName = "example"
)

var (
	pool         *dockertest.Pool
	databaseURI  string
	IdTranslator *testutil.UploadIDTranslator
)

func TestGatewayRecording(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping playback test in short mode.")
	}
	testData := []string{
		"s3://lakefs-recordings/presto.zip",
		"s3://lakefs-recordings/aws.zip",
		"s3://lakefs-recordings/emr-spark.zip",
	}

	downloader := simulator.NewExternalRecordDownloader("us-east-1")

	for _, recording := range testData {
		s3Url, err := url.Parse(recording)
		if err != nil {
			t.Fatal(err)
		}
		basename := filepath.Base(s3Url.Path)
		filename := filepath.Join(RecordingsDir, basename)
		testName := strings.TrimSuffix(basename, filepath.Ext(basename))
		t.Run(testName, func(t *testing.T) {
			// download record
			err := downloader.DownloadRecording(s3Url.Host, basename, filename)
			if err != nil {
				t.Fatal(err)
			}

			setGlobalPlaybackParams(basename)
			_ = os.RemoveAll(simulator.PlaybackParams.RecordingDir)
			_ = os.MkdirAll(simulator.PlaybackParams.RecordingDir, 0755)
			deCompressRecordings(filename, simulator.PlaybackParams.RecordingDir)
			handler, _ := getBasicHandlerPlayback(t)
			DoTestRun(handler, false, 1.0, t)
		})
	}
}

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseURI, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func (m *mockCollector) CollectMetadata(*stats.Metadata) {}

func (m *mockCollector) CollectEvent(string, string) {}

func (m *mockCollector) SetInstallationID(string) {}

func getBasicHandlerPlayback(t *testing.T) (http.Handler, *dependencies) {
	authService := newGatewayAuthFromFile(t, simulator.PlaybackParams.RecordingDir)
	return getBasicHandler(t, authService)
}

func getBasicHandler(t *testing.T, authService *simulator.PlayBackMockConf) (http.Handler, *dependencies) {
	ctx := context.Background()
	IdTranslator = &testutil.UploadIDTranslator{TransMap: make(map[string]string),
		ExpectedID: "",
		T:          t,
	}

	conf, err := config.NewConfig()
	testutil.MustDo(t, "config", err)

	conn, _ := testutil.GetDB(t, databaseURI)
	c, err := catalog.New(ctx, catalog.Config{
		Config: conf,
		DB:     conn,
	})
	testutil.MustDo(t, "build c", err)
	multipartsTracker := multiparts.NewTracker(conn)

	blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
	blockAdapter := testutil.NewBlockAdapterByType(t, IdTranslator, blockstoreType)

	t.Cleanup(func() {
		_ = c.Close()
	})

	storageNamespace := os.Getenv("USE_STORAGE_NAMESPACE")
	if storageNamespace == "" {
		storageNamespace = "replay"
	}

	// keeping the name 'master' and not 'main' below as the recordings point to that
	_, err = c.CreateRepository(ctx, ReplayRepositoryName, storageNamespace, "master")
	testutil.Must(t, err)

	handler := gateway.NewHandler(
		authService.Region,
		c,
		multipartsTracker,
		blockAdapter,
		authService,
		[]string{authService.BareDomain},
		&mockCollector{},
		nil,
	)

	return handler, &dependencies{
		blocks:  blockAdapter,
		auth:    authService,
		catalog: c,
	}
}

func newGatewayAuthFromFile(t *testing.T, directory string) *simulator.PlayBackMockConf {
	m := new(simulator.PlayBackMockConf)
	fName := filepath.Join(directory, simulator.SimulationConfig)
	confStr, err := ioutil.ReadFile(fName)
	if err != nil {
		t.Fatal(fName + " not found\n")
	}
	err = json.Unmarshal(confStr, m)
	if err != nil {
		t.Fatalf("Failed to unmarshal configuration: %s", err.Error())
	}
	return m
}

func deCompressRecordings(archive, dir string) {
	// Open a zip archive for reading.
	r, err := zip.OpenReader(archive)
	if err != nil {
		logging.Default().WithError(err).Fatal("could not decompress archive " + archive)
	}
	defer func() {
		_ = r.Close()
	}()

	// Iterate through the files in the archive,
	// copy to temporary recordings directory
	for _, f := range r.File {
		// skip directories
		if f.FileInfo().IsDir() {
			continue
		}
		decompressRecordingsFile(f)
	}
}

func decompressRecordingsFile(f *zip.File) {
	compressedFile, err := f.Open()
	if err != nil {
		logging.Default().WithError(err).Fatal("Couldn't read from archive file " + f.Name)
	}
	defer func() {
		_ = compressedFile.Close()
	}()
	fileName := filepath.Join(simulator.PlaybackParams.RecordingDir, filepath.Base(f.Name))
	decompressedFile, err := os.Create(fileName)
	if err != nil {
		logging.Default().WithError(err).Fatal("failed creating file " + f.Name)
	}
	defer func() {
		_ = decompressedFile.Close()
	}()
	_, err = io.Copy(decompressedFile, compressedFile)
	if err != nil {
		logging.Default().WithError(err).Fatal("failed copying file " + f.Name)
	}
}
