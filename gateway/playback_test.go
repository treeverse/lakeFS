package gateway_test

import (
	"archive/zip"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/testutil"
)

type dependencies struct {
	blocks block.Adapter
	auth   simulator.GatewayAuthService
	meta   index.Index
}

const (
	RecordingsDir = "testdata/recordings"
)

func TestGatewayRecording(t *testing.T) {
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
		t.Run(basename, func(t *testing.T) {
			// download record
			err := downloader.DownloadRecording(s3Url.Host, basename, filename)
			if err != nil {
				t.Fatal(err)
			}

			setGlobalPlaybackParams(basename)
			os.RemoveAll(simulator.PlaybackParams.RecordingDir)
			os.MkdirAll(simulator.PlaybackParams.RecordingDir, 0755)
			deCompressRecordings(filename, simulator.PlaybackParams.RecordingDir)
			handler, _ := getBasicHandler(t, basename)
			DoTestRun(handler, false, 1.0, t)

		})
	}
}

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

type mockCollector struct{}

func (m *mockCollector) Collect(class, action string) {

}

var IdTranslator *testutil.UploadIdTranslator

func getBasicHandler(t *testing.T, testDir string) (http.Handler, *dependencies) {
	IdTranslator = &testutil.UploadIdTranslator{TransMap: make(map[string]string),
		ExpectedId: "",
		T:          t,
	}

	mdb, _ := testutil.GetDB(t, databaseUri)
	meta := index.NewDBIndex(mdb)

	blockAdapter := testutil.GetBlockAdapter(t, IdTranslator)

	authService := newGatewayAuth(t, simulator.PlaybackParams.RecordingDir)

	testutil.Must(t, meta.CreateRepo("example", "example-tzahi", "master"))
	server := gateway.NewServer(authService.Region,
		meta,
		blockAdapter,
		authService,
		authService.ListenAddress, authService.BareDomain, &mockCollector{})

	return server.Server.Handler, &dependencies{
		blocks: blockAdapter,
		auth:   authService,
		meta:   meta,
	}
}

func newGatewayAuth(t *testing.T, directory string) *simulator.PlayBackMockConf {
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
