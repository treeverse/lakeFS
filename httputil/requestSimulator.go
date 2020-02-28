package httputil

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	isRecording, isPlayback bool
	recordingDir            string
	currentUploadId         []byte // used at playback to set the upload id in
	uploadIdRegexp          *regexp.Regexp
	timedPlayback           = false //schedule events by their recording time, or just one after the other
	playbackSpeed           = 5.0   // if timed playback - how fast the clock runs ? 5 implies that 1 minute time difference
	// will run in 12 seconds
)

const (
	startUploadTag = "<UploadId>"
	endUploadTag   = "</UploadId>"
	startStatusTag = "<statusCode>"
	endStatusTag   = "</statusCode>"
	// between original events will translate to 12 seconds. a number less than 1 will make slower clock
)

type lazyOutFile struct {
	Name   string
	Prot   os.FileMode
	F      *os.File
	IsOpen bool
}

func newLazyOutFile(name string, prot os.FileMode) *lazyOutFile {
	r := new(lazyOutFile)
	r.Name = name
	r.Prot = prot
	return r
}

func (l *lazyOutFile) Write(d []byte) (int, error) {
	if !l.IsOpen {
		l.IsOpen = true
		var err error
		l.F, err = os.OpenFile(l.Name, os.O_CREATE|os.O_WRONLY, l.Prot)
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

func (l *lazyOutFile) Close() error {
	if !l.IsOpen {
		return nil
	}
	return l.F.Close()
}

type storedEvent struct {
	Status   int    `json:"status"`
	UploadID []byte `json:"uploadId"`
	Request  []byte `json:"request"`
}

func init() {
	state, exist := os.LookupEnv("UNIT_TEST")
	if !exist {
		return
	}
	switch state {
	case "record":
		isRecording = true
		uploadIdRegexp = regexp.MustCompile(startUploadTag + "([\\da-f]+)" + endUploadTag)
	case "playback":
		t, exist := os.LookupEnv("TIMED")
		if exist {
			timedPlayback = true
			z, err := strconv.ParseFloat(t, 32)
			if err == nil {
				playbackSpeed = z
			}
		}

		isPlayback = true
	default:
		panic("UNIT_TEST environment variable has unknown value: " + state + "\n")
	}
	testDir, exist := os.LookupEnv("TEST_DIR")
	if !exist {
		panic("test directory not defined")
	}
	recordingDir = "testdata/recordings/" + testDir

}

func IsPlayback() bool {
	return isPlayback
}

// RECORDING - helper decorator types

type responseWriter struct {
	uploadId         []byte
	originalWriter   http.ResponseWriter
	lookForUploadId  bool
	responseFileName string
	responseLog      *os.File
	statusCode       int
}

func (w *responseWriter) Header() http.Header {
	return w.originalWriter.Header()
}

func (w *responseWriter) Write(data []byte) (int, error) {
	written, err := w.originalWriter.Write(data)
	if err == nil {
		if w.lookForUploadId && len(w.uploadId) == 0 {
			w.uploadId = uploadIdRegexp.FindSubmatch(data)[1]
		}
		if w.responseLog == nil && written > 0 {
			f, err := os.OpenFile(w.responseFileName, os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				panic("can not create response file ")
			}
			w.responseLog = f
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
	recorderName string
	recorder     *os.File
	originalBody io.ReadCloser
}

func (r *recordingBodyReader) Read(b []byte) (int, error) {
	size, err := r.originalBody.Read(b)
	if size > 0 {
		var err1 error
		if r.recorder == nil {
			r.recorder, err1 = os.OpenFile(r.recorderName, os.O_CREATE|os.O_WRONLY, 0777)
			if err1 != nil {
				panic("can not create recorder file ")
			}
		}
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

func RegisterRecorder(router *mux.Router) {
	if !isRecording {
		return
	}
	err := os.MkdirAll(recordingDir, 0777) // if needed - create recording directory
	if err != nil {
		panic("FAILED creat directory for recordings \n")
	}

	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				nameBase := time.Now().Format("01-02-15-04-05.000") + "-" + fmt.Sprintf("%05d", rand.Intn(99999))
				responseWriter := &responseWriter{originalWriter: w}
				responseWriter.responseFileName = recordingDir + "/" + "R" + nameBase + ".resp"
				if r.URL.RawQuery == "uploads=" {
					responseWriter.lookForUploadId = true
				}
				newBody := new(recordingBodyReader)
				newBody.recorderName = recordingDir + "/" + "B" + nameBase + ".body"
				newBody.originalBody = r.Body
				r.Body = newBody
				// initial post for s3 multipart upload

				defer func() {
					if responseWriter.responseLog != nil {
						responseWriter.responseLog.Close()
					}
					if newBody.recorder != nil {
						newBody.recorder.Close()
					}

				}()
				next.ServeHTTP(responseWriter, r)
				logRequest(r, responseWriter.uploadId, nameBase, responseWriter.statusCode)
			})
	})

}

func logRequest(r *http.Request, uploadId []byte, nameBase string, statusCode int) {
	var event storedEvent
	var err error
	event.Request, err = httputil.DumpRequest(r, false)
	if err != nil || len(event.Request) == 0 {
		log.WithError(err).
			WithFields(log.Fields{
				"request": string(event.Request),
			}).Fatal("request dumping failed")
	}
	// it was the initial POST of a miltipart upload
	event.UploadID = uploadId
	//if uploadId != nil {
	//	dumpReq = append(uploadId, dumpReq...)
	//}

	event.Status = statusCode
	jsonEvent, err := json.Marshal(event)
	fName := recordingDir + "/L" + nameBase + ".log"
	err = ioutil.WriteFile(fName, jsonEvent, 0777)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"fileName": fName,
				"request":  string(jsonEvent),
			}).Fatal("writing request file failed")
	}
}

//PLAYBACK

type simulationEvent struct {
	eventTime    time.Time
	url          []byte
	uploadId     []byte
	statusCode   int
	baseName     string
	bodyReader   *os.File
	originalBody io.ReadCloser
}

func GetUploadId() string {
	if currentUploadId != nil {
		t := string(currentUploadId)
		currentUploadId = nil
		return t
	} else {
		panic("Reading uploadId when there is none\n")
	}
}

var runResultsDir string
var statusMisatchLog *os.File

func DoTestRun(handler http.Handler) {
	runResultsDir = recordingDir + "/" + time.Now().Format("01-02-15-04-05")
	err := os.MkdirAll(runResultsDir, 0777)
	if err != nil {
		panic("\n could not create directory: " + runResultsDir + "\n")
	}

	defer func() {
		if statusMisatchLog != nil {
			statusMisatchLog.Close()
		}
	}()
	simulationEvents := buildEventList(recordingDir)
	runEvents(simulationEvents, handler)
}

const startPlayDelay = 1 //second delay between building the events to starting to play them

func regexpGlob(directory string, logPattern *regexp.Regexp) []string {
	dirList, err := ioutil.ReadDir(directory) //ReadDir returns files sorted by name. in the events time order
	if err != nil {
		panic("Directory read failed")
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
	logPattern := regexp.MustCompile("^L\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}\\.\\d{3}-\\d{5}.log$")
	fileList := regexpGlob(directory, logPattern)
	//var firstEventTime time.Time
	//simulationStartTime := time.Now().Add(startPlayDelay * time.Second) // playing events will start startPlayDelay seconds after load
	//firstEventFlag := true
	for _, file := range fileList {
		var evt simulationEvent
		evt.baseName = file[1:strings.Index(file, ".log")]
		eventTimeStr := file[1:19]                                        // time part of file name
		evt.eventTime, _ = time.Parse("01-02-15-04-05.000", eventTimeStr) // add to function
		//if firstEventFlag {
		//	firstEventTime = recordingTime
		//	firstEventFlag = false
		//}
		//evt.eventTime = simulationStartTime.Add(recordingTime.Sub(firstEventTime)) // refactor start time
		fName := filepath.Join(directory, file)
		url, err := ioutil.ReadFile(fName)
		if err != nil {
			log.Panic("Recording file not found\n")
		}
		buildEventFromURL(url, &evt)
		simulationEvents = append(simulationEvents, evt)

	}
	return simulationEvents
}

func buildEventFromURL(url []byte, evt *simulationEvent) {
	// get statusCode and remove from url
	url = url[len(startStatusTag):]
	endPos := bytes.Index(url, []byte(endStatusTag))
	if endPos < 0 {
		panic("</statusCode> not found in recording\n")
	}
	evt.statusCode, _ = strconv.Atoi(string(url[:endPos]))
	url = url[endPos+len(endStatusTag):]
	// get uploadId if exists and remove from url
	if 0 == bytes.Compare(url[0:len(startUploadTag)], []byte(startUploadTag)) {
		url = url[len(startUploadTag):]
		endPos := bytes.Index(url, []byte(endUploadTag))
		if endPos < 0 {
			panic("</UploadId> not found in start multipart post url\n")
		}
		evt.uploadId = url[:endPos]
		url = url[endPos+len(endUploadTag):]
	}
	evt.url = url
}

func runEvents(eventsList []simulationEvent, handler http.Handler) {
	for _, event := range eventsList {
		bReader := bufio.NewReader(bytes.NewReader(event.url))

		request, err := http.ReadRequest(bReader)
		if err != nil {
			log.Panic("could not create Request from URL")
		}
		if len(event.uploadId) > 0 {
			currentUploadId = event.uploadId
		}

		secondDiff := time.Duration(float64(event.eventTime.Sub(time.Now())) / playbackSpeed)
		if secondDiff > 0 && timedPlayback {
			fmt.Print("\n\nwait: ", secondDiff)
			time.Sleep(secondDiff)
		}
		asyncServeHTTP(request, handler, &event)
	}
	//time.Sleep(5.0*time.Second)
}

func asyncServeHTTP(r *http.Request, handler http.Handler, event *simulationEvent) {
	event.originalBody = r.Body
	r.Body = event
	w := httptest.NewRecorder()
	respWrite := new(responseWriter)
	respWrite.originalWriter = w
	respWrite.responseFileName = runResultsDir + "/" + "R" + event.baseName + ".resp"
	handler.ServeHTTP(respWrite, r)
	if respWrite.statusCode != event.statusCode {
		if statusMisatchLog == nil {
			var err error
			statusMisatchLog, err = os.OpenFile(runResultsDir+"/"+"status_mismatch.log", os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				panic("\n could not create status mismatch log file\n")
			}
		}
		fmt.Fprintf(statusMisatchLog, "different status event %s recorded \t %d current \t %d",
			event.baseName, event.statusCode, respWrite.statusCode)
	}
}

func (r *simulationEvent) Read(b []byte) (int, error) {
	if r.bodyReader == nil {
		fName := recordingDir + "/B" + r.baseName + ".body"
		f, err := os.Open(fName)
		if err != nil { // couldn find recording file
			return 0, io.EOF
		}
		r.bodyReader = f
	}
	return r.bodyReader.Read(b)
}

func (r *simulationEvent) Close() error {
	if r.bodyReader != nil {
		return r.bodyReader.Close()
	} else {
		return nil
	}
}
