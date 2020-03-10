package gateway_test

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/testutil"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"
)

type anyType interface{}

type playBackMockConf struct {
	ListenAddress   string                    `json:"listen_address"`
	BareDomain      string                    `json:"bare_domain"`
	AccessKeyId     string                    `json:"access_key_id"`
	AccessSecretKey string                    `json:"access_secret_Key"`
	CredentialType  model.APICredentials_Type `json:"credential_type"`
	EntityId        string                    `json:"entity_id"`
	Region          string                    `json:"Region"`
}

type dependencies struct {
	blocks block.Adapter
	auth   utils.GatewayService
	meta   index.Index
	mpu    index.MultipartManager
}

func TestGatewayRecording(t *testing.T) {
	//handler,dependencies,closer := getBasicHandler(t)
	dirList, err := ioutil.ReadDir("testdata/recordings")
	if err != nil {
		log.WithError(err).Fatal("Failed reading recording directories")
	}
	for _, dir := range dirList {
		if !dir.IsDir() {
			continue
		}
		dirName := dir.Name()
		setGlobalPlaybackParams(dirName)
		handler, _, closer := getBasicHandler(t, dirName)
		defer closer()
		t.Run(dirName+" recording", func(t *testing.T) {
			DoTestRun(handler, false, 1.0, t)
		})
	}
}

func getBasicHandler(t *testing.T, testDir string) (http.Handler, *dependencies, func()) {
	directory := filepath.Join("testdata", "recordings", testDir)

	db, dbCloser := testutil.GetDB(t)
	blockAdapter, fsCloser := testutil.GetBlockAdapter(t)
	indexStore := store.NewKVStore(db)
	meta := index.NewKVIndex(indexStore)
	mpu := index.NewKVMultipartManager(indexStore)
	authService := newGatewayAuth(t, directory)

	closer := func() {
		dbCloser()
		fsCloser()
	}
	testutil.Must(t, meta.CreateRepo("example", "s3://example", "master"))
	server := gateway.NewServer(authService.Region,
		meta,
		blockAdapter,
		authService,
		mpu,
		authService.ListenAddress, authService.BareDomain)

	return server.Server.Handler, &dependencies{
		blocks: blockAdapter,
		auth:   authService,
		meta:   meta,
		mpu:    mpu,
	}, closer
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

func (m *playBackMockConf) GetAPICredentials(accessKey string) (*model.APICredentials, error) {
	if accessKey != m.AccessKeyId {
		log.Fatal("access key in recording different than configuration")
	}
	aCred := new(model.APICredentials)
	aCred.AccessKeyId = accessKey
	aCred.AccessSecretKey = m.AccessSecretKey
	aCred.CredentialType = m.CredentialType
	return aCred, nil

}

func (m *playBackMockConf) Authorize(req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{true, nil}, nil
}
