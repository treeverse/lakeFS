package utils

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"os"
)

// a limited service interface for the gateway, used by simulation playback
type GatewayService interface {
	GetAPICredentials(accessKey string) (*model.APICredentials, error)
	Authorize(req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error)
}

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

func (l *LazyOutput) Close() error {
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

var PlaybackParams struct {
	IsPlayback                  bool
	CurrentUploadId             []byte // used at playback to set the upload id in
	RecordingDir, RunResultsDir string
}

func IsPlayback() bool {
	return PlaybackParams.IsPlayback
}

func GetUploadId() string {
	fmt.Print("in getUploadId \n")
	if PlaybackParams.CurrentUploadId != nil {
		t := string(PlaybackParams.CurrentUploadId)
		PlaybackParams.CurrentUploadId = nil
		return t
	} else {
		panic("Reading uploadId when there is none\n")
	}
}
