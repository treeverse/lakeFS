package utils

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/gateway/sig"

	"github.com/treeverse/lakefs/logging"
)

type StoredEvent struct {
	Status   int    `json:"status"`
	UploadID string `json:"uploadId"`
	Request  string `json:"request"`
}

// RECORDING - helper decorator types

type recordingBodyReader struct {
	recorder     *LazyOutput
	originalBody io.ReadCloser
}

func (r *recordingBodyReader) Read(b []byte) (int, error) {
	size, err := r.originalBody.Read(b)
	if size > 0 {
		var err1 error
		readSlice := b[:size]
		_, err1 = r.recorder.Write(readSlice)
		if err1 != nil {
			panic(" can not write to recorder file")
		}
	}
	return size, err
}

func (r *recordingBodyReader) Close() error {
	err := r.originalBody.Close()
	r.recorder.Close()
	r.recorder = nil
	return err
}

// RECORDING

var uniquenessCounter int32 // persistent request counter during run. used only below,

func RegisterRecorder(next http.Handler, authService GatewayAuthService, region, bareDomain, listenAddr string) http.Handler {
	logger := logging.Default()
	testDir, exist := os.LookupEnv("RECORD")
	if !exist {
		return next
	}
	recordingDir := filepath.Join(RecordingRoot, testDir)
	err := os.MkdirAll(recordingDir, 0644) // if needed - create recording directory
	if err != nil {
		logger.WithError(err).Fatal("FAILED create directory for recordings")
	}
	uploadIdRegexp := regexp.MustCompile("<UploadId>([\\dA-Za-z_.+/]+)</UploadId>")

	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/_health" || strings.HasPrefix(r.URL.Path, "/debug/pprof/") {
				return
			}

			uniqueCount := atomic.AddInt32(&uniquenessCounter, 1)
			if uniqueCount == 1 { //first activation. Now we can store the simulation configuration, since we have
				// user details
				createConfFile(r, authService, region, bareDomain, listenAddr, recordingDir)
			}
			timeStr := time.Now().Format("15-04-05")
			nameBase := timeStr + fmt.Sprintf(
				"-%05d", (uniqueCount%100000))
			respWriter := new(ResponseWriter)
			respWriter.OriginalWriter = w
			respWriter.ResponseLog = NewLazyOutput(filepath.Join(recordingDir, nameBase+ResponseExtension))
			respWriter.Regexp = uploadIdRegexp
			respWriter.Headers = make(http.Header)
			rawQuery := r.URL.RawQuery
			if (rawQuery == "uploads=") || (rawQuery == "uploads") { // initial post for s3 multipart upload
				respWriter.lookForUploadId = true
			}
			newBody := &recordingBodyReader{recorder: NewLazyOutput(filepath.Join(recordingDir, nameBase+RequestBodyExtension)),
				originalBody: r.Body}
			r.Body = newBody
			defer func() {
				_ = respWriter.ResponseLog.Close()
				respWriter.SaveHeaders(filepath.Join(recordingDir, nameBase+ResponseHeaderExtension))
				_ = newBody.recorder.Close()
			}()
			next.ServeHTTP(respWriter, r)
			logRequest(r, respWriter.uploadId, nameBase, respWriter.StatusCode, recordingDir)
		})
}
func ShutdownRecorder() {
	testDir, exist := os.LookupEnv("RECORD")
	if !exist {
		return
	}
	logging.Default().Debug("Shutdown recorder")
	recordingDir := filepath.Join(RecordingRoot, testDir)
	compressRecordings(testDir, recordingDir)
}

func logRequest(r *http.Request, uploadId []byte, nameBase string, statusCode int, recordingDir string) {
	request, err := httputil.DumpRequest(r, false)
	if err != nil || len(request) == 0 {
		logging.Default().
			WithError(err).
			WithFields(logging.Fields{"request": string(request)}).
			Fatal("request dumping failed")
	}
	event := StoredEvent{
		Request:  string(request),
		UploadID: string(uploadId),
		Status:   statusCode,
	}
	if event.Status == 0 {
		event.Status = http.StatusOK
	}
	jsonEvent, err := json.Marshal(event)
	if err != nil {
		logging.Default().
			WithError(err).
			Fatal("marshal event as json")
	}
	fName := filepath.Join(recordingDir, nameBase+RequestExtension)
	err = ioutil.WriteFile(fName, jsonEvent, 0600)
	if err != nil {
		logging.Default().
			WithError(err).
			WithFields(logging.Fields{"fileName": fName, "request": string(jsonEvent)}).
			Fatal("writing request file failed")
	}
}

func createConfFile(r *http.Request, authService GatewayAuthService, region, bareDomain, listenAddr, recordingDir string) {
	authenticator := sig.ChainedAuthenticator(
		sig.NewV4Authenticator(r),
		sig.NewV2SigAuthenticator(r))
	authContext, err := authenticator.Parse()
	if err != nil {
		logging.Default().WithError(err).
			Fatal("failed getting access key using authenticator ")
	}
	accessKeyId := authContext.GetAccessKeyId()
	creds, err := authService.GetAPICredentials(accessKeyId)
	if err != nil {
		logging.Default().
			WithError(err).
			WithFields(logging.Fields{"Access Key": accessKeyId}).
			Fatal("failed getting credentials")
	}
	conf := &PlayBackMockConf{
		ListenAddress:   listenAddr,
		BareDomain:      bareDomain,
		AccessKeyId:     accessKeyId,
		AccessSecretKey: creds.AccessSecretKey,
		CredentialType:  creds.Type,
		UserId:          *creds.UserId,
		Region:          region,
	}
	confByte, err := json.Marshal(conf)
	if err != nil {
		logging.Default().
			WithError(err).
			Fatal("couldn't marshal configuration")
	}
	err = ioutil.WriteFile(filepath.Join(recordingDir, SimulationConfig), confByte, 0755)
}

func compressRecordings(testName, recordingDir string) {
	logger := logging.Default()
	zipFileName := filepath.Join(RecordingRoot, testName+".zip")
	zWriter, err := os.Create(zipFileName)
	if err != nil {
		logger.WithError(err).Error("Failed creating zip archive file")
		return
	}
	defer zWriter.Close()
	// Create a new zip archive.
	w := zip.NewWriter(zWriter)
	dirList, err := ioutil.ReadDir(recordingDir)
	if err != nil {
		logger.WithError(err).Error("Failed reading directory ")
		return
	}
	defer w.Close()
	for _, file := range dirList {
		fName := file.Name()
		fullName := filepath.Join(recordingDir, fName)
		inputFile, err := os.Open(fullName)
		if err != nil {
			logger.WithError(err).Error("Failed opening recording file " + fName)
			return
		}
		outZip, err := w.Create(fName)
		if err != nil {
			logger.WithError(err).Error("Failed creating to zip file " + fName)
			return
		}
		_, err = io.Copy(outZip, inputFile)
		if err != nil {
			logger.WithError(err).Error("Failed copying to zip file " + fName)
			return
		}
	}

	// Make sure to check the error on Close.
	err = w.Close()
	if err != nil {
		logger.WithError(err).Error("Failed closing archive")
		return
	}
	os.RemoveAll(recordingDir)
}
