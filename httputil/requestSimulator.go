package httputil

import (
	"bufio"
	"bytes"
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
	"regexp"
	"strconv"
	str "strings"
	"time"
)

var (
	isRecording, isPlayback bool
	recordingDir            string
	lastUploadId            []byte
	uploadIdRegexp          *regexp.Regexp
)

const (
	startUploadTag = "<UploadId>"
	endUploadTag   = "</UploadId>"
)

func init() {
	if len(os.Args) >= 4 {
		switch os.Args[2] {
		case "record":
			isRecording = true
			uploadIdRegexp = regexp.MustCompile(startUploadTag + "[\\da-f]+" + endUploadTag)
		case "playback":
			isPlayback = true
		default:
			panic("second command line illegal: " + os.Args[2])
		}
		recordingDir = os.Args[3]
	}
}

func IsPlayback() bool {
	return isPlayback
}

// lastUploadId can be read only once
func GetUploadId() string {
	if lastUploadId == nil {
		panic("GetUploadId was called twice\n")
	}
	temp := string(lastUploadId)
	lastUploadId = nil
	return temp
}

// decorator on response writer to capture upload id

type inspectingWriter struct {
	uploadId []byte
	Writer   http.ResponseWriter
}

func (w *inspectingWriter) Header() http.Header {
	return w.Writer.Header()
}

func (w *inspectingWriter) Write(data []byte) (int, error) {
	written, err := w.Writer.Write(data)
	if err == nil && len(w.uploadId) == 0 {
		w.uploadId = uploadIdRegexp.Find(data)
	}
	return written, err
}

func (w *inspectingWriter) WriteHeader(statusCode int) {
	w.Writer.WriteHeader(statusCode)
}

// RECORDING

func RegisterRecorder(router *mux.Router) {
	if isRecording {
		router.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					if r.URL.RawQuery == "uploads=" {
						writer := &inspectingWriter{Writer: w}
						next.ServeHTTP(writer, r)
						logRequest(r, writer.uploadId)
					} else {
						next.ServeHTTP(w, r)
						logRequest(r, nil)
					}
				})
		})
		err := os.MkdirAll("testdata\\recordings\\"+recordingDir, 0664)
		if err != nil {
			panic("FAILED creat directory for recordings \n")
		}
	}

}

func logRequest(r *http.Request, uploadId []byte) {
	dumpReq, err := httputil.DumpRequest(r, false)
	if err != nil || len(dumpReq) == 0 {
		log.WithError(err).
			WithFields(log.Fields{
				"request": string(dumpReq),
			}).Warn("request dumping failed")
		return
	}
	// it was the initial POST of a miltipart upload
	if uploadId != nil {
		dumpReq = append(uploadId, dumpReq...)
	}

	currentTime := time.Now()
	fName := "testdata\\recordings\\" + recordingDir + "\\L" + currentTime.Format("01-02-15-04-05.000") + "-" + fmt.Sprintf("%05d", rand.Intn(99999)) + ".log"
	err = ioutil.WriteFile(fName, dumpReq, 0664)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"fileName": fName,
				"request":  string(dumpReq),
			}).Warn("writing request file failed")
	}
}

//PLAYBACK

type simulationEvent struct {
	eventTime time.Time
	url       []byte
	uploadId  []byte
}

func DoTestRun(handler http.Handler) {
	simulationEvents := buildEventList("testdata\\recordings\\" + recordingDir)
	runEvents(simulationEvents, handler)
}

func buildEventList(directory string) []simulationEvent {
	var simulationEvents []simulationEvent
	logPattern := regexp.MustCompile("^L\\d\\d-\\d\\d-\\d\\d-\\d\\d-\\d\\d\\.\\d\\d\\d-\\d\\d\\d\\d\\d\\.log$")
	fileList, err := ioutil.ReadDir(directory) //ReadDir returns files sorted by name
	if err != nil {
		panic("log directory read failed")
	}
	var firstEventTime time.Time
	startTime := time.Now().Add(30 * time.Second) // playing events will start 30 second after load
	firstEventFlag := true
	for _, file := range fileList {
		if logPattern.MatchString(file.Name()) {
			//fmt.Print("did it")
			tm := file.Name()[1:19]
			recordingTime, _ := time.Parse("01-02-15-04-05.000", tm)
			if firstEventFlag {
				firstEventTime = recordingTime
				firstEventFlag = false
			}
			var evt simulationEvent
			evt.eventTime = startTime.Add(recordingTime.Sub(firstEventTime))
			fName := directory + "\\" + file.Name()
			url, err := ioutil.ReadFile(fName)
			if err != nil {
				log.Panic("Recording file not found\n")
			}
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
			simulationEvents = append(simulationEvents, evt)
		}
	}
	return simulationEvents
}

func runEvents(eventsList []simulationEvent, handler http.Handler) {
	for _, event := range eventsList {
		bReader := bufio.NewReader(bytes.NewReader(event.url))
		request, err := http.ReadRequest(bReader)
		if err != nil {
			log.Panic("could not create Request from URL")
		}
		if len(event.uploadId) > 0 {
			lastUploadId = event.uploadId
		}
		request.Body = NewReader(request.ContentLength)
		//todo: activate as go routine
		asyncServeHTTP(request, handler)
	}
}
func asyncServeHTTP(r *http.Request, handler http.Handler) {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	z := w.Result()
	fmt.Print(z)
}

// CREATING SIMULATED CONTENT

type contentCreator struct {
	pos       int64
	maxLength int64
}

func NewReader(max int64) *contentCreator {
	var r *contentCreator
	r = new(contentCreator)
	r.maxLength = max
	return r
}

const stepSize = 20

func (c *contentCreator) Read(b []byte) (int, error) {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if c.pos == c.maxLength {
		return 0, io.EOF
	}
	retSize := minInt(int64(len(b)), int64(c.maxLength-c.pos))
	var nextCopyPos int64
	currentBlockNo := c.pos/stepSize + 1 // first block in number 1
	//create the first block, which may be the continuation of a previous block
	remaining := (c.pos % stepSize)
	if remaining != 0 || retSize < stepSize {
		// the previous read did not end on integral stepSize boundry, or the retSize
		// is less than a block
		copiedSize := int64(copy(b, makeBlock(currentBlockNo)[remaining:minInt(stepSize, remaining+retSize)]))
		nextCopyPos = copiedSize
		currentBlockNo++
	}
	// create the blocks between first and last. Those are always full
	fullBlocksNo := (retSize - nextCopyPos) / stepSize
	for i := int64(0); i < fullBlocksNo; i++ {
		copy(b[nextCopyPos:nextCopyPos+stepSize], makeBlock(currentBlockNo))
		currentBlockNo++
		nextCopyPos += stepSize
	}
	// create the trailing block (if needed)
	remainingBytes := (c.pos + retSize) % stepSize
	if remainingBytes != 0 && (c.pos%stepSize+retSize) > stepSize {
		// destination will be smaller than the returned block, copy size is the minimum size of dest and source
		copy(b[nextCopyPos:nextCopyPos+remainingBytes], makeBlock(currentBlockNo)) // destination will be smaller than step size
	}
	c.pos += retSize
	if c.pos == c.maxLength {
		return int(retSize), io.EOF
	} else {
		if c.pos < c.maxLength {
			return int(retSize), nil
		} else {
			log.Panic("reader programming error - got past maxLength")
			return int(retSize), nil
		}
	}

}

func makeBlock(n int64) string {
	f := strconv.Itoa(int(n * stepSize))
	block := str.Repeat("*", stepSize-len(f)) + f
	return block
}

func (c *contentCreator) Close() error {
	c.pos = -1
	return nil
}

func minInt(i1, i2 int64) int64 {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
}

func (c *contentCreator) Seek(seekPoint int64) error {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if seekPoint >= c.maxLength {
		return io.EOF
	}
	c.pos = seekPoint
	return nil
}
