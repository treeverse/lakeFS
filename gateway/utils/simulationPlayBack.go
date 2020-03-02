package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	isPlayback      bool
	recordingDir    string
	currentUploadId []byte  // used at playback to set the upload id in
	timedPlayback   = false //schedule events by their recording time, or just one after the other
	playbackSpeed   = 5.0   // if timed playback - how fast the clock runs ? 5 implies that 1 minute time difference
	// will run in 12 seconds
	runResultsDir string
)

func init() {
	testDir, exist := os.LookupEnv("PLAYBACK")
	if !exist {
		return
	}
	isPlayback = true
	recordingDir = "testdata/recordings/" + testDir
	t, exist := os.LookupEnv("TIMED")
	if exist {
		timedPlayback = true
		z, err := strconv.ParseFloat(t, 32)
		if err == nil {
			playbackSpeed = z
		}
	}
}

func IsPlayback() bool {
	return isPlayback
}

type simulationEvent struct {
	eventTime    time.Time
	request      []byte
	uploadId     []byte
	statusCode   int
	baseName     string
	bodyReader   *os.File
	originalBody io.ReadCloser
}

func GetUploadId() string {
	fmt.Print("in getUploadId \n")
	if currentUploadId != nil {
		t := string(currentUploadId)
		currentUploadId = nil
		return t
	} else {
		panic("Reading uploadId when there is none\n")
	}
}

func DoTestRun(handler http.Handler) {
	runResultsDir = recordingDir + "/" + time.Now().Format("01-02-15-04-05")
	err := os.MkdirAll(runResultsDir, 0777)
	if err != nil {
		panic("\n could not create directory: " + runResultsDir + "\n")
	}
	simulationEvents := buildEventList(recordingDir)
	if len(simulationEvents) > 0 {
		runEvents(simulationEvents, handler)
	} else {
		panic("no events found \n")
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

func buildEventList(directory string) []simulationEvent {
	var simulationEvents []simulationEvent
	var se storedEvent
	logPattern := regexp.MustCompile("^L\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}\\.\\d{3}-\\d{5}.log$")
	fileList := regexpGlob(directory, logPattern)
	for _, file := range fileList {
		evt := new(simulationEvent)
		evt.baseName = file[1:strings.Index(file, ".log")]
		eventTimeStr := file[1:19]                                        // time part of file name
		evt.eventTime, _ = time.Parse("01-02-15-04-05.000", eventTimeStr) // add to function
		fName := filepath.Join(directory, file)
		event, err := ioutil.ReadFile(fName)
		if err != nil {
			log.Panic("Recording file not found\n")
		}
		err = json.Unmarshal(event, &se)
		if err != nil {
			log.WithError(err).Fatal("Failed to unmarshal event " + file + "\n")
		}
		evt.statusCode = se.Status
		evt.uploadId = se.UploadID
		evt.request = []byte(se.Request)
		simulationEvents = append(simulationEvents, *evt)

	}
	return simulationEvents
}

func runEvents(eventsList []simulationEvent, handler http.Handler) {
	simulationMisses := newLazyOutput(filepath.Join(runResultsDir, "status_mismatch.log"))
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
			currentUploadId = event.uploadId
		}

		secondDiff := time.Duration(float64(event.eventTime.Add(durationToAdd).Sub(time.Now())) / playbackSpeed)
		if secondDiff > 0 && timedPlayback {
			fmt.Print("\n\nwait: ", secondDiff)
			time.Sleep(secondDiff)
		}
		ServeRecordedHTTP(request, handler, &event, simulationMisses)
		time.Sleep(100 * time.Millisecond)

	}

}

func ServeRecordedHTTP(r *http.Request, handler http.Handler, event *simulationEvent, simulationMisses *lazyOutput) {
	event.originalBody = r.Body
	r.Body = event
	w := httptest.NewRecorder()
	respWrite := new(responseWriter)
	respWrite.originalWriter = w
	l := newLazyOutput(filepath.Join(runResultsDir, "R"+event.baseName+".resp"))
	defer func() {
		_ = l.Close()
		_ = event.Close()
	}()
	respWrite.responseLog = l
	handler.ServeHTTP(respWrite, r)
	if respWrite.statusCode != event.statusCode {
		_, _ = fmt.Fprintf(simulationMisses, "different status event %s recorded \t %d current \t %d",
			event.baseName, event.statusCode, respWrite.statusCode)
	}
}

func (r *simulationEvent) Read(b []byte) (int, error) {
	if r.bodyReader == nil {
		fName := filepath.Join(recordingDir, "B"+r.baseName+".body")
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
