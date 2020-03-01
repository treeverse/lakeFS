package utils

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync/atomic"
	"time"
)

type lazyOutput struct {
	Name   string
	F      *os.File
	IsOpen bool
}

func IsPlayback() bool {
	return false
}

func newLazyOutput(name string) *lazyOutput {
	r := new(lazyOutput)
	r.Name = name
	return r
}

func (l *lazyOutput) Write(d []byte) (int, error) {
	if !l.IsOpen {
		l.IsOpen = true
		var err error
		l.F, err = os.OpenFile(l.Name, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.WithError(err).Fatal("file " + l.Name + " failed opened")
		}
	}
	written, err := l.F.Write(d)
	if err != nil {
		log.WithError(err).Fatal("file " + l.Name + " failed write")
	}
	return written, err
}

func (l *lazyOutput) Close() error {
	if !l.IsOpen {
		return nil
	}
	err := l.F.Close()
	if err != nil {
		log.WithError(err).Fatal("Failed closing " + l.Name)
	}
	l.F = nil
	return err
}

type storedEvent struct {
	Status   int    `json:"status"`
	UploadID []byte `json:"uploadId"`
	Request  string `json:"request"`
}

// RECORDING - helper decorator types

type responseWriter struct {
	uploadId        []byte
	originalWriter  http.ResponseWriter
	lookForUploadId bool
	responseLog     *lazyOutput
	statusCode      int
	Regexp          *regexp.Regexp
}

func (w *responseWriter) Header() http.Header {
	return w.originalWriter.Header()
}

func (w *responseWriter) Write(data []byte) (int, error) {
	written, err := w.originalWriter.Write(data)
	if err == nil {
		if w.lookForUploadId && len(w.uploadId) == 0 {
			w.uploadId = w.Regexp.FindSubmatch(data)[1]
		}
		writtenSlice := data[:written]
		_, err1 := w.responseLog.Write(writtenSlice)
		if err1 != nil {
			panic("could nor write response file\n")
		}
	}
	return written, err
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.originalWriter.WriteHeader(statusCode)
}

type recordingBodyReader struct {
	recorder     *lazyOutput
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

var uniquenessCounter int32

func RegisterRecorder(router *mux.Router) {
	testDir, exist := os.LookupEnv("RECORD")
	if !exist {
		return
	}
	runtime.GOMAXPROCS(1)
	recordingDir := "testdata/recordings/" + testDir
	err := os.MkdirAll(recordingDir, 0777) // if needed - create recording directory
	if err != nil {
		log.WithError(err).Fatal("FAILED creat directory for recordings \n")
	}
	uploadIdRegexp := regexp.MustCompile("<UploadId>([\\da-f]+)</UploadId>")

	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				id := atomic.AddInt32(&uniquenessCounter, 1)
				nameBase := time.Now().Format("01-02-15-04-05.000") + "-" + fmt.Sprintf("%05d", id)
				respWriter := new(responseWriter)
				respWriter.originalWriter = w
				respWriter.responseLog = newLazyOutput(filepath.Join(recordingDir, "R"+nameBase+".resp"))
				respWriter.Regexp = uploadIdRegexp
				respWriter.statusCode = -99999    // an indication status was not set
				if r.URL.RawQuery == "uploads=" { // initial post for s3 multipart upload
					respWriter.lookForUploadId = true
				}
				newBody := new(recordingBodyReader)
				newBody.recorder = newLazyOutput(recordingDir + "/" + "B" + nameBase + ".body")
				newBody.originalBody = r.Body
				r.Body = newBody
				defer func() {
					_ = respWriter.responseLog.Close()
					_ = newBody.recorder.Close()
				}()
				next.ServeHTTP(respWriter, r)
				logRequest(r, respWriter.uploadId, nameBase, respWriter.statusCode, recordingDir)
			})
	})

}

func logRequest(r *http.Request, uploadId []byte, nameBase string, statusCode int, recordingDir string) {
	var event storedEvent
	var err error
	t, err := httputil.DumpRequest(r, false)
	if err != nil || len(t) == 0 {
		log.WithError(err).
			WithFields(log.Fields{
				"request": string(t),
			}).Fatal("request dumping failed")
	}
	event.Request = string(t)
	event.UploadID = uploadId
	event.Status = statusCode
	jsonEvent, err := json.Marshal(event)
	fName := filepath.Join(recordingDir, "L"+nameBase+".log")
	err = ioutil.WriteFile(fName, jsonEvent, 0777)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"fileName": fName,
				"request":  string(jsonEvent),
			}).Fatal("writing request file failed")
	}
}
