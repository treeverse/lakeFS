package gateway_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"github.com/ory/dockertest/v3"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/dedup"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/testutil"
)

type dependencies struct {
	blocks    block.Adapter
	auth      simulator.GatewayAuthService
	cataloger catalog.Cataloger
}

const (
	RecordingsDir = "testdata/recordings"
)

func TestGatewayRecording(t *testing.T) {
	if !*integrationTest {
		t.Skip("Not running integration tests")
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

var (
	pool        *dockertest.Pool
	databaseURI string
)

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

type mockCollector struct{}

func (m *mockCollector) SetInstallationID(installationID string) {

}

func (m *mockCollector) CollectMetadata(accountMetadata map[string]string) {

}

func (m *mockCollector) CollectEvent(class, action string) {

}

var IdTranslator *testutil.UploadIDTranslator

func getBasicHandlerPlayback(t *testing.T) (http.Handler, *dependencies) {
	authService := newGatewayAuthFromFile(t, simulator.PlaybackParams.RecordingDir)
	return getBasicHandler(t, authService)
}

func getBasicHandler(t *testing.T, authService *simulator.PlayBackMockConf) (http.Handler, *dependencies) {
	IdTranslator = &testutil.UploadIDTranslator{TransMap: make(map[string]string),
		ExpectedID: "",
		T:          t,
	}

	conn, _ := testutil.GetDB(t, databaseURI)
	cataloger := catalog.NewCataloger(conn)

	blockAdapter := testutil.NewBlockAdapterByEnv(IdTranslator)

	dedupCleaner := dedup.NewCleaner(blockAdapter, cataloger.DedupReportChannel())
	t.Cleanup(func() {
		// order is important - close cataloger channel before dedup
		_ = cataloger.Close()
		_ = dedupCleaner.Close()
	})

	ctx := context.Background()
	testutil.Must(t, cataloger.CreateRepository(ctx, "example", "example-tzahi", "master"))
	handler := gateway.NewHandler(
		authService.Region,
		cataloger,
		blockAdapter,
		authService,
		authService.BareDomain,
		&mockCollector{},
		dedupCleaner,
	)

	return handler, &dependencies{
		blocks:    blockAdapter,
		auth:      authService,
		cataloger: cataloger,
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
		t.Fatal("Failed to unmarshal configuration\n ")
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
