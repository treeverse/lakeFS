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
	MaxTextResponse      = 30 * 1024
	statusMismatchReport = "status_mismatch.log"
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
	utils.PlaybackParams.RecordingDir = filepath.Join(os.TempDir(), "lakeFS", "sourceRecordings", testDir)
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
		t.Fatal("Some statuses where not the same, see " + utils.PlaybackParams.PlaybackDir + statusMismatchReport + "\n")
	} else {
		_, toKeep := os.LookupEnv("KEEP_RESULTS")
		if !toKeep {
			os.RemoveAll(utils.PlaybackParams.PlaybackDir)
			os.RemoveAll(utils.PlaybackParams.RecordingDir)
		}
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
	var se utils.StoredEvent
	requestPattern := regexp.MustCompile("^\\d{2}-\\d{2}-\\d{2}-\\d{5}\\" + utils.RequestExtension + "$")
	fileList := regexpGlob(utils.PlaybackParams.RecordingDir, requestPattern)
	for _, file := range fileList {
		evt := new(simulationEvent)
		baseNamePosition := strings.Index(file, utils.RequestExtension)
		evt.baseName = file[:baseNamePosition]
		eventTimeStr := file[:baseNamePosition-6]               // time part of file name
		evt.eventTime, _ = time.Parse("15-04-05", eventTimeStr) // add to function
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
	simulationMisses := utils.NewLazyOutput(filepath.Join(utils.PlaybackParams.PlaybackDir, statusMismatchReport))
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
	l := utils.NewLazyOutput(filepath.Join(utils.PlaybackParams.PlaybackDir, event.baseName+utils.ResponseExtension))
	defer func() {
		_ = l.Close()
		_ = event.Close()
	}()
	respWrite.ResponseLog = l
	respWrite.Headers = make(http.Header)
	handler.ServeHTTP(respWrite, r)
	if respWrite.StatusCode == 0 {
		respWrite.StatusCode = 200
	}
	if event.statusCode == 0 {
		event.statusCode = 200
	}
	if respWrite.StatusCode != event.statusCode {
		eventNumber := event.baseName[len(event.baseName)-5:]
		logging.Default().Warnf("unexpected status %d on event  %s ", respWrite.StatusCode, eventNumber)
		fmt.Fprintf(simulationMisses, "different status event %s recorded \t %d current \t %d\n",
			event.baseName, event.statusCode, respWrite.StatusCode)
		statusEqual = false
	}
	return statusEqual
}

func (r *simulationEvent) Read(b []byte) (int, error) {
	if r.bodyReader == nil {
		fName := filepath.Join(utils.PlaybackParams.RecordingDir, r.baseName+utils.RequestBodyExtension)
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
	globPattern := filepath.Join(playbackDir, "*"+utils.ResponseExtension)
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
	var buf1, buf2 [1024]byte
	_, fileName := filepath.Split(playbackFileName)
	recordingFileName := filepath.Join(utils.PlaybackParams.RecordingDir, fileName)
	playbackInfo, err := os.Stat(playbackFileName)
	recordingInfo, err1 := os.Stat(recordingFileName)
	if err != nil || playbackInfo == nil || err1 != nil || recordingInfo == nil {
		return false
	}
	playbackSize := playbackInfo.Size()
	recordingSize := recordingInfo.Size()
	if recordingSize < MaxTextResponse && playbackSize < MaxTextResponse {
		playByte, err := ioutil.ReadFile(playbackFileName)
		if err != nil {
			t.Error("Couldn't read playback file: " + playbackFileName)
			return false
		}
		recByte, err := ioutil.ReadFile(recordingFileName)
		if err != nil {
			t.Error("Couldn't read recording file: " + recordingFileName)
			return false
		}
		playStr := normalizeResponse(playByte, tagRemoveList)
		recStr := normalizeResponse(recByte, tagRemoveList)
		return recStr == playStr
	} else {
		f1, err1 := os.Open(playbackFileName)
		defer f1.Close()

		f2, err2 := os.Open(recordingFileName)
		defer f2.Close()
		if err1 != nil || err2 != nil {
			t.Fatal("file " + playbackFileName + " did not open\n")
		}
		for {
			b1 := buf1[:]
			b2 := buf2[:]
			n1, err1 := f1.Read(b1)
			n2, err2 := f2.Read(b2)
			if n1 != n2 || err1 != err2 {
				return false
			} else {
				b1 := buf1[:n1]
				b2 := buf2[:n1]
				if bytes.Compare(b1, b2) != 0 {
					return false
				}
			}
			if err1 == io.EOF {
				return true
			}
		}
	}
}

func normalizeResponse(respByte []byte, tagRemoveList []*tagPatternType) string {
	resp := string(respByte)
	for _, tagPattern := range tagRemoveList {
		resp = tagPattern.regex.ReplaceAllString(resp, "<"+tagPattern.base+"/>")
	}
	return resp
}
