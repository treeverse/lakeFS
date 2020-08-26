package simulator

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
		readSlice := b[:size]
		_, err1 := r.recorder.Write(readSlice)
		if err1 != nil {
			panic("can not write to recorder file")
		}
	}
	return size, err
}

func (r *recordingBodyReader) Close() error {
	err := r.originalBody.Close()
	_ = r.recorder.Close()
	r.recorder = nil
	return err
}

// RECORDING

var uniquenessCounter int32 // persistent request counter during run. used only below,

func RegisterRecorder(next http.Handler, authService GatewayAuthService, region, bareDomain string) http.Handler {
	logger := logging.Default()
	testDir, exist := os.LookupEnv("RECORD")
	if !exist {
		return next
	}
	recordingDir := filepath.Join(RecordingRoot, testDir)
	err := os.MkdirAll(recordingDir, 0755) // if needed - create recording directory
	if err != nil {
		logger.WithError(err).Fatal("FAILED create directory for recordings")
	}
	uploadIDRegexp := regexp.MustCompile(`<UploadId>([^\\b<]+)</UploadId>`)

	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/_health" || strings.HasPrefix(r.URL.Path, "/debug/pprof/") {
				return
			}

			uniqueCount := atomic.AddInt32(&uniquenessCounter, 1)
			if uniqueCount == 1 {
				// first activation. Now we can store the simulation configuration, since we have user details
				createConfFile(r, authService, region, bareDomain, recordingDir)
			}
			timeStr := time.Now().Format("15-04-05")
			nameBase := timeStr + fmt.Sprintf("-%05d", (uniqueCount%100000))
			respWriter := new(ResponseWriter)
			respWriter.OriginalWriter = w
			respWriter.ResponseLog = NewLazyOutput(filepath.Join(recordingDir, nameBase+ResponseExtension))
			respWriter.Headers = make(http.Header)
			rawQuery := r.URL.RawQuery
			if (rawQuery == "uploads=") || (rawQuery == "uploads") {
				// initial post for s3 multipart upload
				respWriter.UploadIDRegexp = uploadIDRegexp
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
			logRequest(r, respWriter.uploadID, nameBase, respWriter.StatusCode, recordingDir)
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

func logRequest(r *http.Request, uploadID []byte, nameBase string, statusCode int, recordingDir string) {
	request, err := httputil.DumpRequest(r, false)
	if err != nil || len(request) == 0 {
		logging.Default().
			WithError(err).
			WithFields(logging.Fields{"request": string(request)}).
			Fatal("request dumping failed")
	}
	event := StoredEvent{
		Request:  string(request),
		UploadID: string(uploadID),
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

func createConfFile(r *http.Request, authService GatewayAuthService, region, bareDomain, recordingDir string) {
	authenticator := sig.ChainedAuthenticator(
		sig.NewV4Authenticator(r),
		sig.NewV2SigAuthenticator(r))
	authContext, err := authenticator.Parse()
	if err != nil {
		logging.Default().WithError(err).
			Fatal("failed getting access key using authenticator ")
	}
	accessKeyID := authContext.GetAccessKeyID()
	creds, err := authService.GetCredentials(accessKeyID)
	if err != nil {
		logging.Default().
			WithError(err).
			WithFields(logging.Fields{"Access Key": accessKeyID}).
			Fatal("failed getting credentials")
	}
	conf := &PlayBackMockConf{
		BareDomain:      bareDomain,
		AccessKeyID:     accessKeyID,
		AccessSecretKey: creds.AccessSecretKey,
		UserID:          creds.UserID,
		Region:          region,
	}
	confByte, err := json.Marshal(conf)
	if err != nil {
		logging.Default().
			WithError(err).
			Fatal("couldn't marshal configuration")
	}
	err = ioutil.WriteFile(filepath.Join(recordingDir, SimulationConfig), confByte, 0644) //nolint:gosec
	if err != nil {
		logging.Default().
			WithError(err).
			Fatal("failed to write configuration record file")
	}
}

func compressRecordings(testName, recordingDir string) {
	logger := logging.Default()
	zipFileName := filepath.Join(RecordingRoot, testName+".zip")
	zWriter, err := os.Create(zipFileName)
	if err != nil {
		logger.WithError(err).Error("Failed creating zip archive file")
		return
	}
	defer func() {
		_ = zWriter.Close()
	}()
	// Create a new zip archive.
	w := zip.NewWriter(zWriter)
	dirList, err := ioutil.ReadDir(recordingDir)
	if err != nil {
		logger.WithError(err).Error("Failed reading directory ")
		return
	}
	defer func() {
		_ = w.Close()
	}()
	for _, file := range dirList {
		if compressRecordingsAddFile(recordingDir, file.Name(), w) {
			return
		}
	}

	// Make sure to check the error on Close.
	err = w.Close()
	if err != nil {
		logger.WithError(err).Error("Failed closing archive")
		return
	}
	_ = os.RemoveAll(recordingDir)
}

func compressRecordingsAddFile(recordingDir string, fName string, w *zip.Writer) bool {
	fullName := filepath.Join(recordingDir, fName)
	inputFile, err := os.Open(fullName)
	if err != nil {
		logging.Default().WithError(err).WithField("filename", fName).Error("Failed opening recording file")
		return true
	}
	defer func() {
		_ = inputFile.Close()
	}()
	outZip, err := w.Create(fName)
	if err != nil {
		logging.Default().WithError(err).WithField("filename", fName).Error("Failed creating to zip file")
		return true
	}
	_, err = io.Copy(outZip, inputFile)
	if err != nil {
		logging.Default().WithError(err).WithField("filename", fName).Error("Failed copying to zip file")
		return true
	}
	return false
}
