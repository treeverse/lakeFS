package gateway_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/testutil"
	"io"
	"io/ioutil"
	"net/http"
	"os"
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

func compareFiles(fName string) bool {
	var buf1, buf2 [1024]byte
	b1 := buf1[:]
	b2 := buf2[:]

	f1, err1 := os.Open(fName)
	defer f1.Close()
	l := len(fName)
	recName := fName[:l-41] + fName[l-26:]
	f2, err2 := os.Open(recName)
	defer f2.Close()
	if err1 != nil || err2 != nil {
		panic("file did not open")
	}
	for true {
		n1, err1 := f1.Read(b1)
		n2, err2 := f2.Read(b2)
		if n1 != n2 || err1 != err2 {
			fmt.Print("File ", recName, "length  not the same\n")
			return false
		} else if bytes.Compare(b1[:n1], b2[:n2]) != 0 {
			fmt.Print("File ", recName, " content not the same\n")
			return false
		}
		if err1 == io.EOF {
			return true
		}
	}
	return false // need it for the compiler
}

func TestDirCompare(t *testing.T) {
	var notSame int
	names, err := filepath.Glob("testdata/recordings/spark/03-08-18-01-17/*.resp")
	if err != nil {
		panic("filed Globe\n")
	}
	for _, n := range names {
		res := compareFiles(n)
		if !res {
			notSame++
		}
	}
	fmt.Print(len(names), " files compared ", notSame, " files different\n")
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
		handler, _, closer := getBasicHandler(t, dirName)
		defer closer()
		t.Run(dirName+" recording", func(t *testing.T) {
			DoTestRun(handler, dirName, false, 1.0, t)
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
	authService := newGatewayAuth(directory)

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

func newGatewayAuth(directory string) *playBackMockConf {
	m := new(playBackMockConf)
	fName := filepath.Join(directory, "simulation_config.json")
	confStr, err := ioutil.ReadFile(fName)
	if err != nil {
		log.WithError(err).Fatal(fName + " not found\n")
		log.Panic()
	}
	err = json.Unmarshal(confStr, m)
	if err != nil {
		log.WithError(err).Fatal("Failed to unmarshal configuration\n ")
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
