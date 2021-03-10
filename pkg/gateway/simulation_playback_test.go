package gateway_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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

	"github.com/treeverse/lakefs/pkg/gateway/simulator"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	MaxTextResponse      = 30 * 1024
	statusMismatchReport = "status_mismatch.log"
)

type simulationEvent struct {
	eventTime    time.Time
	request      []byte
	uploadID     []byte
	statusCode   int
	baseName     string
	bodyReader   *os.File
	originalBody io.ReadCloser
}

func setGlobalPlaybackParams(testDir string) {
	simulator.PlaybackParams.IsPlayback = true
	simulator.PlaybackParams.RecordingDir = filepath.Join(os.TempDir(), "lakeFS", "sourceRecordings", testDir)
	simulator.PlaybackParams.PlaybackDir = filepath.Join(os.TempDir(), "lakeFS", "gatewayRecordings", time.Now().Format("01-02-15-04-05.000"))
}

func DoTestRun(handler http.Handler, timed bool, speed float64, t *testing.T) {
	err := os.MkdirAll(simulator.PlaybackParams.PlaybackDir, 0755)
	if err != nil {
		t.Fatal("could not create playback directory:", simulator.PlaybackParams.PlaybackDir)
	}
	simulationEvents := buildEventList(t)
	if len(simulationEvents) == 0 {
		t.Fatal("no events found")
	}
	allStatusEqual := runEvents(simulationEvents, handler, timed, speed, t)
	playbackDirCompare(t, simulator.PlaybackParams.PlaybackDir)
	if !allStatusEqual {
		t.Fatal("Some statuses where not the same, see", filepath.Join(simulator.PlaybackParams.PlaybackDir, statusMismatchReport))
	}
	_, toKeep := os.LookupEnv("KEEP_RESULTS")
	if !toKeep {
		_ = os.RemoveAll(simulator.PlaybackParams.PlaybackDir)
		_ = os.RemoveAll(simulator.PlaybackParams.RecordingDir)
	}
}

func regexpGlob(directory string, pattern *regexp.Regexp) []string {
	dirList, err := ioutil.ReadDir(directory) //ReadDir returns files sorted by name. in the events time order
	if err != nil {
		logging.Default().WithError(err).Fatal("Directory read failed :" + directory)
	}
	// filter only request  files
	var fileList []string
	for _, f := range dirList {
		if pattern.MatchString(f.Name()) {
			fileList = append(fileList, f.Name())
		}
	}
	return fileList
}

func buildEventList(t *testing.T) []simulationEvent {
	var simulationEvents []simulationEvent
	var se simulator.StoredEvent
	requestPattern := regexp.MustCompile("^\\d{2}-\\d{2}-\\d{2}-\\d{5}\\" + simulator.RequestExtension + "$")
	fileList := regexpGlob(simulator.PlaybackParams.RecordingDir, requestPattern)
	for _, file := range fileList {
		evt := new(simulationEvent)
		baseNamePosition := strings.Index(file, simulator.RequestExtension)
		evt.baseName = file[:baseNamePosition]
		eventTimeStr := file[:baseNamePosition-6]               // time part of file name
		evt.eventTime, _ = time.Parse("15-04-05", eventTimeStr) // add to function
		fName := filepath.Join(simulator.PlaybackParams.RecordingDir, file)
		event, err := ioutil.ReadFile(fName)
		if err != nil {
			t.Fatal("Recording file not found\n")
		}
		err = json.Unmarshal(event, &se)
		if err != nil {
			logging.Default().WithError(err).Fatal("Failed to unmarshal event " + file + "\n")
		}
		evt.statusCode = se.Status
		evt.uploadID = []byte(se.UploadID)
		evt.request = []byte(se.Request)
		simulationEvents = append(simulationEvents, *evt)
	}
	return simulationEvents
}

func runEvents(eventsList []simulationEvent, handler http.Handler, timedPlayback bool, playbackSpeed float64, t *testing.T) bool {
	simulationMisses := simulator.NewLazyOutput(filepath.Join(simulator.PlaybackParams.PlaybackDir, statusMismatchReport))
	defer func() {
		_ = simulationMisses.Close()
	}()
	allStatusEqual := true
	firstEventTime := eventsList[0].eventTime
	durationToAdd := time.Since(firstEventTime)
	for _, event := range eventsList {
		bReader := bufio.NewReader(bytes.NewReader(event.request))
		request, err := http.ReadRequest(bReader)
		if err != nil {
			logging.Default().WithError(err).Fatal("could not create Request from URL")
		}
		if len(event.uploadID) > 0 {
			IdTranslator.ExpectedID = string(event.uploadID)
		}

		secondDiff := time.Duration(float64(time.Until(event.eventTime.Add(durationToAdd))) / playbackSpeed)
		if secondDiff > 0 && timedPlayback {
			t.Log("\nwait: ", secondDiff, "\n")
			time.Sleep(secondDiff)
		}
		currentResult := ServeRecordedHTTP(request, handler, &event, simulationMisses)
		allStatusEqual = currentResult && allStatusEqual
	}
	return allStatusEqual
}

func ServeRecordedHTTP(r *http.Request, handler http.Handler, event *simulationEvent, simulationMisses *simulator.LazyOutput) bool {
	statusEqual := true
	event.originalBody = r.Body
	r.Body = event
	w := httptest.NewRecorder()
	respWrite := new(simulator.ResponseWriter)
	respWrite.OriginalWriter = w
	l := simulator.NewLazyOutput(filepath.Join(simulator.PlaybackParams.PlaybackDir, event.baseName+simulator.ResponseExtension))
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
		logging.Default().Warnf("Unexpected status %d, expected %d on event number %s - %s %s", respWrite.StatusCode, event.statusCode, eventNumber, r.Method, r.RequestURI)
		_, _ = fmt.Fprintf(simulationMisses, "different status event %s recorded\t%d current\t%d\n",
			event.baseName, event.statusCode, respWrite.StatusCode)
		statusEqual = false
	}
	return statusEqual
}

func (r *simulationEvent) Read(b []byte) (int, error) {
	if r.bodyReader == nil {
		fName := filepath.Join(simulator.PlaybackParams.RecordingDir, r.baseName+simulator.RequestBodyExtension)
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
		tagPattern := &tagPatternType{base: tag, regex: re}
		ret = append(ret, tagPattern)
	}
	return
}

func playbackDirCompare(t *testing.T, playbackDir string) {
	globPattern := filepath.Join(playbackDir, "*"+simulator.ResponseExtension)
	names, err := filepath.Glob(globPattern)
	if err != nil {
		t.Fatal("failed Glob on", globPattern)
	}
	var notSame, areSame int
	tagRemoveList := buildTagRemover([]string{"RequestId", "HostId", "LastModified", "ETag"})
	for _, fName := range names {
		res := compareFiles(t, fName, tagRemoveList)
		if !res {
			notSame++
			t.Log("diff found on", fName)
		} else {
			areSame++
		}
	}
	t.Log(len(names), "files compared:", notSame, "files different,", areSame, "files same")
}

func deepCompare(t *testing.T, file1, file2 string) bool {
	f1, err := os.Open(file1)
	if err != nil {
		t.Fatal("file deep compare, failed to open", file1, err)
	}
	defer func() {
		_ = f1.Close()
	}()

	f2, err := os.Open(file2)
	if err != nil {
		t.Fatal("file deep compare, failed to open", file2, err)
	}
	defer func() {
		_ = f2.Close()
	}()

	const chunkSize = 32 * 1024
	b1 := make([]byte, chunkSize)
	b2 := make([]byte, chunkSize)
	for {
		n1, err1 := f1.Read(b1)
		n2, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			}
			if err1 == io.EOF || err2 == io.EOF {
				return false
			}
			t.Fatal("file deep compare failed to reader", err1, err2)
		}

		if n1 != n2 || !bytes.Equal(b1[:n1], b2[:n2]) {
			return false
		}
	}
}

func compareFiles(t *testing.T, playbackFileName string, tagRemoveList []*tagPatternType) bool {
	playbackInfo, err := os.Stat(playbackFileName)
	if err != nil || playbackInfo == nil {
		return false
	}
	_, fileName := filepath.Split(playbackFileName)
	recordingFileName := filepath.Join(simulator.PlaybackParams.RecordingDir, fileName)
	recordingInfo, err := os.Stat(recordingFileName)
	if err != nil || recordingInfo == nil {
		return false
	}
	playbackSize := playbackInfo.Size()
	recordingSize := recordingInfo.Size()
	if recordingSize < MaxTextResponse && playbackSize < MaxTextResponse {
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
		playNorm := normalizeResponse(playBytes, tagRemoveList)
		recNorm := normalizeResponse(recBytes, tagRemoveList)
		return recNorm == playNorm
	}
	return deepCompare(t, recordingFileName, playbackFileName)
}

func normalizeResponse(respByte []byte, tagRemoveList []*tagPatternType) string {
	resp := string(respByte)
	for _, tagPattern := range tagRemoveList {
		resp = tagPattern.regex.ReplaceAllString(resp, "<"+tagPattern.base+"/>")
	}
	return resp
}
