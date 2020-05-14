package utils

import (
	"encoding/json"

	"github.com/treeverse/lakefs/logging"

	"io/ioutil"
	"net/http"
	"os"
	"regexp"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
)

type PlayBackMockConf struct {
	ListenAddress   string `json:"listen_address"`
	BareDomain      string `json:"bare_domain"`
	AccessKeyId     string `json:"access_key_id"`
	AccessSecretKey string `json:"access_secret_Key"`
	CredentialType  string `json:"credential_type"`
	UserId          int    `json:"user_id"`
	Region          string `json:"Region"`
}

// a limited service interface for the gateway, used by simulation playback
type GatewayAuthService interface {
	GetAPICredentials(accessKey string) (*model.Credential, error)
	Authorize(req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error)
}

const (
	RequestExtension        = ".request"
	ResponseExtension       = ".response"
	ResponseHeaderExtension = ".response_headers"
	RequestBodyExtension    = ".request_body"
	SimulationConfig        = "simulation_config.json"
	RecordingRoot           = "gateway/testdata/recordings"
)

type LazyOutput struct {
	Name   string
	F      *os.File
	IsOpen bool
}

func NewLazyOutput(name string) *LazyOutput {
	r := new(LazyOutput)
	r.Name = name
	return r
}

func (l *LazyOutput) Write(d []byte) (int, error) {
	logger := logging.Default()
	if !l.IsOpen {
		l.IsOpen = true
		var err error
		l.F, err = os.OpenFile(l.Name, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			logger.WithError(err).Fatal("file " + l.Name + " failed opened")
		}
	}
	written, err := l.F.Write(d)
	if err != nil {
		logger.WithError(err).Fatal("file " + l.Name + " failed write")
	}
	return written, err
}

func (l *LazyOutput) Close() error {
	logger := logging.Default()
	if !l.IsOpen {
		return nil
	}
	err := l.F.Close()
	if err != nil {
		logger.WithError(err).Fatal("Failed closing " + l.Name)
	}
	l.F = nil
	return err
}

var PlaybackParams struct {
	IsPlayback                bool
	CurrentUploadId           []byte // used at playback to set the upload id in
	RecordingDir, PlaybackDir string
}

func IsPlayback() bool {
	return PlaybackParams.IsPlayback
}

func GetUploadId() string {
	if PlaybackParams.CurrentUploadId != nil {
		t := string(PlaybackParams.CurrentUploadId)
		PlaybackParams.CurrentUploadId = nil
		return t
	} else {
		panic("Reading uploadId when there is none\n")
	}
}

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
	logger := logging.Default()
	if len(w.Headers) == 0 {
		return
	}
	s, _ := json.Marshal(w.Headers)
	err := ioutil.WriteFile(fName, s, 0600)
	if err != nil {
		logger.WithError(err).Fatal("failed crete file " + fName)
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

func (m *PlayBackMockConf) GetAPICredentials(accessKey string) (*model.Credential, error) {
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

func (m *PlayBackMockConf) Authorize(req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{true, nil}, nil
}
