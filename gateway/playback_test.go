package gateway_test

import (
	"archive/zip"
	"encoding/json"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/logging"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/testutil"
)

type playBackMockConf struct {
	ListenAddress   string `json:"listen_address"`
	BareDomain      string `json:"bare_domain"`
	AccessKeyId     string `json:"access_key_id"`
	AccessSecretKey string `json:"access_secret_Key"`
	CredentialType  string `json:"credential_type"`
	UserId          int    `json:"user_id"`
	Region          string `json:"Region"`
}

type dependencies struct {
	blocks block.Adapter
	auth   utils.GatewayAuthService
	meta   index.Index
}

const RecordingsDir = "testdata/recordings"

func TestGatewayRecording(t *testing.T) {
	dirList, err := ioutil.ReadDir(RecordingsDir)
	if err != nil {
		t.Fatalf("Failed reading recording directories: %v", err)
	}
	for _, dir := range dirList {
		zipName := dir.Name()
		if filepath.Ext(dir.Name()) != ".zip" {
			continue
		}
		dirName := zipName[:len(zipName)-4]
		t.Run(dirName+" recording", func(t *testing.T) {

			setGlobalPlaybackParams(dirName)
			os.RemoveAll(utils.PlaybackParams.RecordingDir)
			os.MkdirAll(utils.PlaybackParams.RecordingDir, 0775)
			archive := filepath.Join(RecordingsDir, zipName)
			deCompressRecordings(archive, utils.PlaybackParams.RecordingDir)
			handler, _ := getBasicHandler(t, zipName)
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

	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	meta := index.NewDBIndex(mdb)

	blockAdapter := testutil.GetBlockAdapter(t, IdTranslator)

	authService := newGatewayAuth(t, utils.PlaybackParams.RecordingDir)

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

func newGatewayAuth(t *testing.T, directory string) *playBackMockConf {
	m := new(playBackMockConf)
	fName := filepath.Join(directory, "simulation_config.json")
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

func (m *playBackMockConf) GetAPICredentials(accessKey string) (*model.Credential, error) {
	if accessKey != m.AccessKeyId {
		logging.Default().Fatal("access key in recording different than configuration")
	}
	aCred := new(model.Credential)
	aCred.AccessKeyId = accessKey
	aCred.AccessSecretKey = m.AccessSecretKey
	aCred.Type = m.CredentialType
	aCred.UserId = &m.UserId
	return aCred, nil

}

func (m *playBackMockConf) Authorize(req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{true, nil}, nil
}

func deCompressRecordings(archive, dir string) {
	// Open a zip archive for reading.
	r, err := zip.OpenReader(archive)
	if err != nil {
		logging.Default().WithError(err).Fatal("could not decompress archive " + archive)
	}
	defer r.Close()

	// Iterate through the files in the archive,
	// copy to temporary recordings directory
	for _, f := range r.File {
		if f.Name[len(f.Name)-1:] == "/" { // It is a directory
			continue
		}
		compressedFile, err := f.Open()
		if err != nil {
			logging.Default().WithError(err).Fatal("Couldn't read from archive file " + f.Name)
		}
		fileName := filepath.Join(utils.PlaybackParams.RecordingDir, filepath.Base(f.Name))
		DeCompressedFile, err := os.Create(fileName)
		if err != nil {
			logging.Default().WithError(err).Fatal("failed creating file " + f.Name)
		}
		_, err = io.Copy(DeCompressedFile, compressedFile)
		if err != nil {
			logging.Default().WithError(err).Fatal("failed copying file " + f.Name)
		}
		compressedFile.Close()
	}
}
