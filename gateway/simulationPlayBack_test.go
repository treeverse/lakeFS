package gateway_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/gateway/utils"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

type simulationEvent struct {
	eventTime    time.Time
	request      []byte
	uploadId     []byte
	statusCode   int
	baseName     string
	bodyReader   *os.File
	originalBody io.ReadCloser
}

func setGlobalPlaybackParams(testDir string) {
	utils.PlaybackParams.IsPlayback = true
	utils.PlaybackParams.RecordingDir = filepath.Join("testdata", "recordings", testDir)
	utils.PlaybackParams.RunResultsDir = filepath.Join(os.TempDir(), "lakeFS", "gatewayRecordings", time.Now().Format("01-02-15-04-05"))
}

func DoTestRun(handler http.Handler, timed bool, speed float64, t *testing.T) {
	err := os.MkdirAll(utils.PlaybackParams.RunResultsDir, 0777)
	if err != nil {
		panic("\n could not create directory: " + utils.PlaybackParams.RunResultsDir + "\n")
	}
	simulationEvents := buildEventList()
	if len(simulationEvents) > 0 {
		runEvents(simulationEvents, handler, timed, speed, t)
	} else {
		t.Fatal("no events found \n")
	}

}

func regexpGlob(directory string, logPattern *regexp.Regexp) []string {
	dirList, err := ioutil.ReadDir(directory) //ReadDir returns files sorted by name. in the events time order
	if err != nil {
		log.WithError(err).Fatal("Directory read failed :" + directory)
	}
	// filter only request (.log) files
	var fileList []string
	for _, f := range dirList {
		if logPattern.MatchString(f.Name()) {
			fileList = append(fileList, f.Name())
		}
	}
	return fileList
}

func buildEventList() []simulationEvent {
	var simulationEvents []simulationEvent
	var se utils.StoredEvent
	logPattern := regexp.MustCompile("^L\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}\\-\\d{5}.log$")
	fileList := regexpGlob(utils.PlaybackParams.RecordingDir, logPattern)
	for _, file := range fileList {
		evt := new(simulationEvent)
		evt.baseName = file[1:strings.Index(file, ".log")]
		eventTimeStr := file[1:15]                                    // time part of file name
		evt.eventTime, _ = time.Parse("01-02-15-04-05", eventTimeStr) // add to function
		fName := filepath.Join(utils.PlaybackParams.RecordingDir, file)
		event, err := ioutil.ReadFile(fName)
		if err != nil {
			log.Panic("Recording file not found\n")
		}
		err = json.Unmarshal(event, &se)
		if err != nil {
			log.WithError(err).Fatal("Failed to unmarshal event " + file + "\n")
		}
		evt.statusCode = se.Status
		evt.uploadId = []byte(se.UploadID)
		evt.request = []byte(se.Request)
		simulationEvents = append(simulationEvents, *evt)

	}
	return simulationEvents
}

func runEvents(eventsList []simulationEvent, handler http.Handler, timedPlayback bool, playbackSpeed float64, t *testing.T) {
	simulationMisses := utils.NewLazyOutput(filepath.Join(utils.PlaybackParams.RunResultsDir, "status_mismatch.log"))
	defer func() {
		_ = simulationMisses.Close()
	}()
	firstEventTime := eventsList[0].eventTime
	durationToAdd := time.Now().Sub(firstEventTime)
	for _, event := range eventsList {
		bReader := bufio.NewReader(bytes.NewReader(event.request))
		request, err := http.ReadRequest(bReader)
		if err != nil {
			log.WithError(err).Fatal("could not create Request from URL")
		}
		if len(event.uploadId) > 0 {
			utils.PlaybackParams.CurrentUploadId = event.uploadId
		}

		secondDiff := time.Duration(float64(event.eventTime.Add(durationToAdd).Sub(time.Now())) / playbackSpeed)
		if secondDiff > 0 && timedPlayback {
			t.Log("\nwait: ", secondDiff, "\n")
			time.Sleep(secondDiff)
		}
		ServeRecordedHTTP(request, handler, &event, simulationMisses, t)
	}

}

func ServeRecordedHTTP(r *http.Request, handler http.Handler, event *simulationEvent, simulationMisses *utils.LazyOutput, t *testing.T) {
	event.originalBody = r.Body
	r.Body = event
	w := httptest.NewRecorder()
	respWrite := new(utils.ResponseWriter)
	respWrite.OriginalWriter = w
	l := utils.NewLazyOutput(filepath.Join(utils.PlaybackParams.RunResultsDir, "R"+event.baseName+".resp"))
	defer func() {
		_ = l.Close()
		_ = event.Close()
	}()
	respWrite.ResponseLog = l
	respWrite.Headers = make(http.Header)
	handler.ServeHTTP(respWrite, r)
	if respWrite.StatusCode != event.statusCode {
		fmt.Fprintf(simulationMisses, "different status event %s recorded \t %d current \t %d\n",
			event.baseName, event.statusCode, respWrite.StatusCode)
	}
}

func (r *simulationEvent) Read(b []byte) (int, error) {
	if r.bodyReader == nil {
		fName := filepath.Join(utils.PlaybackParams.RecordingDir, "B"+r.baseName+".body")
		f, err := os.Open(fName)
		if err != nil { // couldnt find recording file
			return 0, io.EOF
		}
		r.bodyReader = f
	}
	return r.bodyReader.Read(b)
}

func (r *simulationEvent) Close() error {
	err := r.originalBody.Close()
	if r.bodyReader != nil {
		_ = r.bodyReader.Close()
	}
	return err
}
