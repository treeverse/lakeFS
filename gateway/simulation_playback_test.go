package gateway_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/treeverse/lakefs/logging"

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

	"github.com/treeverse/lakefs/gateway/utils"
)

const (
	MaxTextResponse = 30 * 1024
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
	utils.PlaybackParams.PlaybackDir = filepath.Join(os.TempDir(), "lakeFS", "gatewayRecordings", time.Now().Format("01-02-15-04-05.000"))
}

func DoTestRun(handler http.Handler, timed bool, speed float64, t *testing.T) {
	err := os.MkdirAll(utils.PlaybackParams.PlaybackDir, 0777)
	if err != nil {
		t.Fatal("\n could not create directory: " + utils.PlaybackParams.PlaybackDir + "\n")
	}
	simulationEvents := buildEventList(t)
	if len(simulationEvents) == 0 {
		t.Fatal("no events found \n")
	}
	allStatusEqual := runEvents(simulationEvents, handler, timed, speed, t)
	playbackDirCompare(t, utils.PlaybackParams.PlaybackDir)
	if !allStatusEqual {
		t.Fatal("Some statuses where not the same, see " + utils.PlaybackParams.PlaybackDir + " \n")
	} else {
		_, toKeep := os.LookupEnv("KEEP_RESULTS")
		if !toKeep {
			_ = os.RemoveAll(utils.PlaybackParams.PlaybackDir)
		}
	}

}

func regexpGlob(directory string, logPattern *regexp.Regexp) []string {
	dirList, err := ioutil.ReadDir(directory) //ReadDir returns files sorted by name. in the events time order
	if err != nil {
		logging.Default().WithError(err).Fatal("Directory read failed :" + directory)
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

func buildEventList(t *testing.T) []simulationEvent {
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
			t.Fatal("Recording file not found\n")
		}
		err = json.Unmarshal(event, &se)
		if err != nil {
			logging.Default().WithError(err).Fatal("Failed to unmarshal event " + file + "\n")
		}
		evt.statusCode = se.Status
		evt.uploadId = []byte(se.UploadID)
		evt.request = []byte(se.Request)
		simulationEvents = append(simulationEvents, *evt)

	}
	return simulationEvents
}

func runEvents(eventsList []simulationEvent, handler http.Handler, timedPlayback bool, playbackSpeed float64, t *testing.T) bool {
	simulationMisses := utils.NewLazyOutput(filepath.Join(utils.PlaybackParams.PlaybackDir, "status_mismatch.log"))
	defer func() {
		_ = simulationMisses.Close()
	}()
	allStatusEqual := true
	firstEventTime := eventsList[0].eventTime
	durationToAdd := time.Now().Sub(firstEventTime)
	for _, event := range eventsList {
		bReader := bufio.NewReader(bytes.NewReader(event.request))
		request, err := http.ReadRequest(bReader)
		if err != nil {
			logging.Default().WithError(err).Fatal("could not create Request from URL")
		}
		if len(event.uploadId) > 0 {
			IdTranslator.ExpectedId = string(event.uploadId)
		}

		secondDiff := time.Duration(float64(event.eventTime.Add(durationToAdd).Sub(time.Now())) / playbackSpeed)
		if secondDiff > 0 && timedPlayback {
			t.Log("\nwait: ", secondDiff, "\n")
			time.Sleep(secondDiff)
		}
		currentResult := ServeRecordedHTTP(request, handler, &event, simulationMisses, t)
		allStatusEqual = currentResult && allStatusEqual
	}
	return allStatusEqual
}

func ServeRecordedHTTP(r *http.Request, handler http.Handler, event *simulationEvent, simulationMisses *utils.LazyOutput, t *testing.T) bool {
	statusEqual := true
	event.originalBody = r.Body
	r.Body = event
	w := httptest.NewRecorder()
	respWrite := new(utils.ResponseWriter)
	respWrite.OriginalWriter = w
	l := utils.NewLazyOutput(filepath.Join(utils.PlaybackParams.PlaybackDir, "R"+event.baseName+".resp"))
	defer func() {
		_ = l.Close()
		_ = event.Close()
	}()
	respWrite.ResponseLog = l
	respWrite.Headers = make(http.Header)
	handler.ServeHTTP(respWrite, r)
	if respWrite.StatusCode == 0 {
		respWrite.StatusCode = http.StatusOK
	}
	if event.statusCode == 0 {
		event.statusCode = http.StatusOK
	}
	if respWrite.StatusCode != event.statusCode {
		eventNumber := event.baseName[len(event.baseName)-5:]
		logging.Default().Warnf("unexpected status %d on event  %s", respWrite.StatusCode, eventNumber)
		fmt.Fprintf(simulationMisses, "different status event %s recorded \t %d current \t %d\n",
			event.baseName, event.statusCode, respWrite.StatusCode)
		statusEqual = false
	}
	return statusEqual
}

func (r *simulationEvent) Read(b []byte) (int, error) {
	if r.bodyReader == nil {
		fName := filepath.Join(utils.PlaybackParams.RecordingDir, "B"+r.baseName+".body")
		f, err := os.Open(fName)
		if err != nil {
			// couldn't find recording file
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

type tagPatternType struct {
	base  string
	regex *regexp.Regexp
}

func buildTagRemover(tags []string) (ret []*tagPatternType) {

	for _, tag := range tags {
		pattern := "<" + tag + ">([\\dA-Za-z_\\+/&#;\\-:\\.]+)</" + tag + ">"
		re := regexp.MustCompile(pattern)
		tagPattrn := &tagPatternType{base: tag, regex: re}
		ret = append(ret, tagPattrn)
	}
	return
}

func playbackDirCompare(t *testing.T, playbackDir string) {
	var notSame, areSame int
	globPattern := filepath.Join(playbackDir, "*.resp")
	names, err := filepath.Glob(globPattern)
	if err != nil {
		t.Fatal("failed Globe on " + globPattern + "\n")
	}
	tagRemoveList := buildTagRemover([]string{"RequestId", "HostId", "LastModified", "ETag"})
	for _, fName := range names {
		res := compareFiles(t, fName, tagRemoveList)
		if !res {
			notSame++
		} else {
			areSame++
			_ = os.Remove(fName)
		}
	}
	t.Log(len(names), " files compared: ", notSame, " files different ", areSame, " files same", "\n")
}

func compareFiles(t *testing.T, playbackFileName string, tagRemoveList []*tagPatternType) bool {
	_, fileName := filepath.Split(playbackFileName)
	recordingFileName := filepath.Join(utils.PlaybackParams.RecordingDir, fileName)

	playbackInfo, err := os.Stat(playbackFileName)
	if err != nil {
		t.Error("can't stat playback file", playbackFileName, err)
		return false
	}
	recordingInfo, err := os.Stat(recordingFileName)
	if err != nil {
		t.Error("can't stat recording file", recordingFileName, err)
		return false
	}
	playbackSize := playbackInfo.Size()
	recordingSize := recordingInfo.Size()
	playBytes, err := ioutil.ReadFile(playbackFileName)
	if err != nil {
		t.Error("Couldn't read playback file", playbackFileName)
		return false
	}
	recBytes, err := ioutil.ReadFile(recordingFileName)
	if err != nil {
		t.Error("Couldn't read recording file", recordingFileName)
		return false
	}
	if recordingSize < MaxTextResponse && playbackSize < MaxTextResponse {
		playStr := normalizeResponse(playBytes, tagRemoveList)
		recStr := normalizeResponse(recBytes, tagRemoveList)
		return recStr == playStr
	}
	return bytes.Equal(recBytes, playBytes)
}

func normalizeResponse(respByte []byte, tagRemoveList []*tagPatternType) string {
	resp := string(respByte)
	for _, tagPattern := range tagRemoveList {
		resp = tagPattern.regex.ReplaceAllString(resp, "<"+tagPattern.base+"/>")
	}
	return resp
}
