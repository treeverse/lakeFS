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
	"sync/atomic"
	"time"
)

type StoredEvent struct {
	Status   int    `json:"status"`
	UploadID string `json:"uploadId"`
	Request  string `json:"request"`
}

// RECORDING - helper decorator types

type ResponseWriter struct {
	uploadId        []byte
	OriginalWriter  http.ResponseWriter
	lookForUploadId bool
	ResponseLog     *LazyOutput
	StatusCode      int
	Headers         http.Header
	Regexp          *regexp.Regexp
}

func (w *ResponseWriter) Header() http.Header {
	h := w.OriginalWriter.Header()
	for k, v := range h {
		w.Headers[k] = v
	}
	return h
}
func (w *ResponseWriter) SaveHeaders(fName string) {
	if len(w.Headers) == 0 {
		return
	}
	s, _ := json.Marshal(w.Headers)
	err := ioutil.WriteFile(fName, s, 0777)
	if err != nil {
		log.WithError(err).Fatal("failed crete file " + fName)
	}
}

func (w *ResponseWriter) Write(data []byte) (int, error) {
	written, err := w.OriginalWriter.Write(data)
	if err == nil {
		if w.lookForUploadId && len(w.uploadId) == 0 {
			rx := w.Regexp.FindSubmatch(data)
			if len(rx) > 1 {
				w.uploadId = rx[1]
			}
		}
		writtenSlice := data[:written]
		_, err1 := w.ResponseLog.Write(writtenSlice)
		if err1 != nil {
			panic("could nor write response file\n")
		}
	}
	return written, err
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.OriginalWriter.WriteHeader(statusCode)
}

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

func RegisterRecorder(router *mux.Router) {
	testDir, exist := os.LookupEnv("RECORD")
	if !exist {
		return
	}
	recordingDir := filepath.Join("gateway/testdata/recordings", testDir)
	err := os.MkdirAll(recordingDir, 0777) // if needed - create recording directory
	if err != nil {
		log.WithError(err).Fatal("FAILED creat directory for recordings \n")
	}
	uploadIdRegexp := regexp.MustCompile("<UploadId>([\\da-f]+)</UploadId>")

	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				uniqueCount := atomic.AddInt32(&uniquenessCounter, 1)
				timeStr := time.Now().Format("01-02-15-04-05")
				nameBase := timeStr + fmt.Sprintf("-%05d", (uniqueCount%100000))
				log.WithField("sequence", uniqueCount).Warn("Disregard warning - only to hilite display")
				respWriter := new(ResponseWriter)
				respWriter.OriginalWriter = w
				respWriter.ResponseLog = NewLazyOutput(filepath.Join(recordingDir, "R"+nameBase+".resp"))
				respWriter.Regexp = uploadIdRegexp
				respWriter.Headers = make(http.Header)
				t := r.URL.RawQuery
				if (t == "uploads=") || (t == "uploads") { // initial post for s3 multipart upload
					respWriter.lookForUploadId = true
				}
				newBody := new(recordingBodyReader)
				newBody.recorder = NewLazyOutput(recordingDir + "/" + "B" + nameBase + ".body")
				newBody.originalBody = r.Body
				r.Body = newBody
				defer func() {
					_ = respWriter.ResponseLog.Close()
					respWriter.SaveHeaders(recordingDir + "/" + "H" + nameBase + ".hdr")
					_ = newBody.recorder.Close()
				}()
				next.ServeHTTP(respWriter, r)
				logRequest(r, respWriter.uploadId, nameBase, respWriter.StatusCode, recordingDir)
			})
	})

}

func logRequest(r *http.Request, uploadId []byte, nameBase string, statusCode int, recordingDir string) {
	var event StoredEvent
	var err error
	t, err := httputil.DumpRequest(r, false)
	if err != nil || len(t) == 0 {
		log.WithError(err).
			WithFields(log.Fields{
				"request": string(t),
			}).Fatal("request dumping failed")
	}
	event.Request = string(t)
	event.UploadID = string(uploadId)
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
