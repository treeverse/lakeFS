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
	"regexp"
	"strconv"
	str "strings"
	"time"
)

//todo: func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request)

func RegisterRecorder(router *mux.Router) {
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {

				dumpReq, err := httputil.DumpRequest(r, false)
				if err != nil || dumpReq == nil {
					log.WithError(err).
						WithFields(log.Fields{
							"request": string(dumpReq),
						}).Warn("request dumping failed")
				} else {
					logRequest(dumpReq)
				}
				next.ServeHTTP(w, r)

			})
	})
}

func logRequest(b []byte) {
	currentTime := time.Now()
	fName := "c:\\log\\L" + currentTime.Format("01-02-15-04-05.000") + "-" + fmt.Sprintf("%05d", rand.Intn(99999)) + ".log"
	err := ioutil.WriteFile(fName, b, 0644)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"fileName": fName,
				"request":  string(b),
			}).Warn("request recording failed")
	}
}

type simulationEvent struct {
	eventTime time.Time
	url       []byte
}

//func DoTestRun(inRouter http.Handler) {
func DoTestRun(inRouter mux.Router) {
	simulationEvents := buildEventList("httputil\\testdata\\")
	//simulationEvents := buildEventList("testdata\\")
	runEvents(simulationEvents, inRouter)

}

func buildEventList(directory string) []simulationEvent {
	var simulationEvents []simulationEvent
	logPattern := regexp.MustCompile("^L\\d\\d-\\d\\d-\\d\\d-\\d\\d-\\d\\d\\.\\d\\d\\d-\\d\\d\\d\\d\\d\\.log$")
	fileList, err := ioutil.ReadDir(directory) //ReadDir returns files sorted by name
	if err != nil {
		log.WithError(err).Panic("log directory read failed")
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
			fName := directory + file.Name()
			url, err := ioutil.ReadFile(fName)
			if err != nil {
				log.Panic("Recording file not found\n")
			}
			evt.url = url
			simulationEvents = append(simulationEvents, evt)
		}
	}
	return simulationEvents
}

//func runEvents(eventsList []simulationEvent, router  http.Handler) {
func runEvents(eventsList []simulationEvent, router mux.Router) {
	//todo: remove illeagal character test
	/*legalUriChar := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&'()*+,;=" + "%\n"
	lengthRegex := regexp.MustCompile(`Content-Length: \d+`)
	numberRegex := regexp.MustCompile(`\d+`)*/
	fmt.Print(router)
	for _, event := range eventsList {
		bReader := bufio.NewReader(bytes.NewReader(event.url))
		request, err := http.ReadRequest(bReader)
		if err != nil {
			log.Panic("could not create Request from URL")
		}
		/* fmt.Print(rq)
		tUrl := str.ReplaceAll(str.TrimSpace(string(event.url)),"\r\n","\n")
		contLine := lengthRegex.FindString(tUrl)
		contentLength,_ := strconv.Atoi(numberRegex.FindString(contLine))
		x := str.SplitN(tUrl, " ", 2)
		method := x[0]
		u := str.ReplaceAll(str.ReplaceAll(x[1]," ","%20"),"\n","%0A")
		baseUrl, err := url.Parse(u)
		if err != nil {
			log.Panic("bad url parse")
		}
		rUrl := baseUrl.String()
		for i,c := range(rUrl) {
			if !str.ContainsAny(string(c), legalUriChar) {
				fmt.Print("illegal character:", i, ":", string(c))
			}
		}
		request, err := http.NewRequest(method, rUrl, NewReader(contentLength))
		if err != nil {
			log.Panic("could not create NewRequest")
		}
		request.ContentLength = int64(contentLength)*/
		//fmt.Print(request)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, request)
		z := w.Result()
		fmt.Print(z)
	}
}

type contentCreator struct {
	pos       int
	maxLength int
}

func NewReader(max int) *contentCreator {
	var r *contentCreator
	r = new(contentCreator)
	r.maxLength = max
	return r
}

const stepSize = 10

func (c *contentCreator) Read(b []byte) (int, error) {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if c.pos == c.maxLength {
		return 0, io.EOF
	}
	retSize := minInt(len(b), c.maxLength-c.pos)
	var nextCopyPos int
	fmt.Print(retSize)
	currentBlockNo := c.pos/stepSize + 1 // first block in number 1
	//create the first block, which may be the continuation of a previous block
	remaining := (c.pos % stepSize)
	if remaining != 0 || retSize < stepSize {
		// the previous read did not end on integral stepSize boundry, or the retSize
		// is less than a block
		copiedSize := copy(b, makeBlock(currentBlockNo)[remaining:minInt(stepSize, remaining+retSize)])
		nextCopyPos = copiedSize
		currentBlockNo++
	}
	// create the blocks between first and last. Those are always full
	fullBlocksNo := (retSize - nextCopyPos) / stepSize
	for i := 0; i < fullBlocksNo; i++ {
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
		return retSize, io.EOF
	} else {
		if c.pos < c.maxLength {
			return retSize, nil
		} else {
			log.Panic("reader programming error - got past maxLength")
			return retSize, nil
		}
	}

}

func makeBlock(n int) string {
	f := strconv.Itoa(n * stepSize)
	block := str.Repeat("*", stepSize-len(f)) + f
	return block
}

func (c *contentCreator) Close() error {
	c.pos = -1
	return nil
}

func minInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
}

func (c *contentCreator) Seek(seekPoint int) error {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if seekPoint >= c.maxLength {
		return io.EOF
	}
	c.pos = seekPoint
	return nil
}
