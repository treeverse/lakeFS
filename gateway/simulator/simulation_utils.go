package simulator

import (
	"encoding/json"
	"time"

	"github.com/treeverse/lakefs/logging"

	"io/ioutil"
	"net/http"
	"os"
	"regexp"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
)

type PlayBackMockConf struct {
	BareDomain      string `json:"bare_domain"`
	AccessKeyID     string `json:"access_key_id"`
	AccessSecretKey string `json:"secret_access_key"`
	UserID          int    `json:"user_id"`
	Region          string `json:"region"`
}

// a limited service interface for the gateway, used by simulation playback
type GatewayAuthService interface {
	GetCredentials(accessKey string) (*model.Credential, error)
	GetUserByID(userID int) (*model.User, error)
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
		l.F, err = os.Create(l.Name)
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
	CurrentUploadID           []byte // used at playback to set the upload id in
	RecordingDir, PlaybackDir string
}

type ResponseWriter struct {
	uploadID       []byte
	OriginalWriter http.ResponseWriter
	ResponseLog    *LazyOutput
	StatusCode     int
	Headers        http.Header
	UploadIDRegexp *regexp.Regexp
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
		if w.UploadIDRegexp != nil && len(w.uploadID) == 0 {
			rx := w.UploadIDRegexp.FindSubmatch(data)
			if len(rx) > 1 {
				w.uploadID = rx[1]
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

func (m *PlayBackMockConf) GetCredentials(accessKey string) (*model.Credential, error) {
	if accessKey != m.AccessKeyID {
		logging.Default().Fatal("access key in recording different than configuration")
	}
	aCred := new(model.Credential)
	aCred.AccessKeyID = accessKey
	aCred.AccessSecretKey = m.AccessSecretKey
	aCred.UserID = m.UserID
	return aCred, nil
}

func (m *PlayBackMockConf) GetUserByID(userID int) (*model.User, error) {
	return &model.User{
		CreatedAt: time.Now(),
		Username:  "user",
	}, nil
}

func (m *PlayBackMockConf) Authorize(req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{Allowed: true}, nil
}
