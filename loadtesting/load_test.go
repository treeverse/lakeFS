package loadtesting

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/auth/model"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// Buffer is a goroutine safe bytes.Buffer
type Buffer struct {
	buffer bytes.Buffer
	mutex  sync.Mutex
}

func (s *Buffer) Write(p []byte) (n int, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	n, err = s.buffer.Write(p)
	if err != nil {
		return n, err
	}
	return s.buffer.WriteString("\n")
}

func (s *Buffer) Read(p []byte) (n int, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.buffer.Read(p)
}

type LoadTester struct {
	RequestHistory []vegeta.Target
	Buffer         Buffer
	ServerAddress  string
}

type LoadTesterConfig struct {
	FreqPerSecond int
	Duration      time.Duration
}

func loadTest(config LoadTesterConfig, serverAddress string, credentials *model.Credential) error {
	tester := LoadTester{
		ServerAddress: serverAddress,
	}
	rate := vegeta.Rate{Freq: config.FreqPerSecond, Per: time.Second}
	duration := config.Duration
	out := make(chan vegeta.Target)
	errs := make(chan error)
	go tester.playScenario(out)
	go tester.streamRequests(out, errs)
	targeter := vegeta.NewJSONTargeter(&tester.Buffer, nil,
		http.Header{http.CanonicalHeaderKey("Authorization"): []string{"Basic " + getAuth(credentials)}})
	attacker := vegeta.NewAttacker()
	var metrics vegeta.Metrics
	defer metrics.Close()
	for res := range attacker.Attack(targeter, rate, duration, "lakeFS load test") {
		if len(res.Error) > 0 {
			fmt.Println(fmt.Sprintf("Error in request %s, error: %s, status: %d", tester.RequestHistory[res.Seq], res.Error, res.Code))
		}
		metrics.Add(res)
	}
	f, err := os.Create("./loadtest-output")
	if err != nil {
		fmt.Println("error trying to create file")
		return err
	}
	metrics.Close()
	err = vegeta.NewJSONReporter(&metrics).Report(f)
	if err != nil {
		fmt.Println("error trying to write report to file")
		return err
	}
	if len(metrics.Errors) > 0 {
		return errors.New("got errors")
	}
	return nil
}

func (t *LoadTester) playScenario(out chan<- vegeta.Target) {
	defer close(out)
	out <- vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://%s/api/v1/repositories", t.ServerAddress),
		Body:   []byte("{\"id\":\"example-repo3\",\"bucket_name\":\"example-bucket\",\"default_branch\":\"master\"}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
	for i := 0; true; i++ {
		for _, tgt := range t.generateFiles("example-repo3", "master", 20) {
			out <- *tgt
		}
		out <- t.generateCommit("example-repo3", fmt.Sprintf("commit%d", i))
		out <- t.generateBranch("example-repo3", fmt.Sprintf("branch%d", i))

		for _, tgt := range t.generateFiles("example-repo3", fmt.Sprintf("branch%d", i), 20) {
			out <- *tgt
		}
		out <- t.generateMergeToMaster("example-repo3", fmt.Sprintf("branch%d", i))
	}
}
func (t *LoadTester) streamRequests(in <-chan vegeta.Target, errs chan<- error) {
	defer close(errs)
	for tgt := range in {
		t.RequestHistory = append(t.RequestHistory, tgt)
		target, err := json.Marshal(tgt)
		if err != nil {
			errs <- err
		}
		t.Buffer.Write(target)
	}
}

func getAuth(credentials *model.Credential) string {
	return base64.StdEncoding.EncodeToString([]byte(credentials.AccessKeyId + ":" + credentials.AccessSecretKey))
}

func (t *LoadTester) generateFiles(repo, branch string, num int) []*vegeta.Target {
	now := time.Now().UnixNano()
	result := make([]*vegeta.Target, num)
	boundary := "---------------------abcdefg123456789"
	for i := 0; i < num; i++ {
		fileContent := "--" + boundary + "\n" +
			"Content-Disposition: form-data; name=\"content\"; filename=\"file\"\n" +
			"Content-Type: text/plain\n\n" +
			strconv.Itoa(rand.Int()) + "\n" + "--" + boundary + "--\n"
		result[i] = &vegeta.Target{
			Method: "POST",
			URL:    fmt.Sprintf("http://%s/api/v1/repositories/%s/branches/%s/objects?path=file_%d_%d", t.ServerAddress, repo, branch, now, i),
			Body:   []byte(fileContent),
			Header: http.Header{
				http.CanonicalHeaderKey("Accept"):          []string{"*/*"},
				http.CanonicalHeaderKey("Accept-Encoding"): []string{"gzip, deflate"},
				http.CanonicalHeaderKey("Content-Type"):    []string{"multipart/form-data; boundary=" + boundary},
				http.CanonicalHeaderKey("Content-Length"):  []string{strconv.Itoa(len(fileContent))},
			},
		}
	}
	return result
}

func (t *LoadTester) generateCommit(repo, msg string) vegeta.Target {
	return vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://%s/api/v1/repositories/%s/branches/master/commits", t.ServerAddress, repo),
		Body:   []byte(fmt.Sprintf(`{"message":"%s","metadata":{}}`, msg)),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
}

func (t *LoadTester) generateBranch(repo, name string) vegeta.Target {
	return vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://%s/api/v1/repositories/%s/branches", t.ServerAddress, repo),
		Body:   []byte(fmt.Sprintf(`{"id":"%s","sourceRefId":"master"}`, name)),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
}

func (t *LoadTester) generateMergeToMaster(repo, branch string) vegeta.Target {
	return vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://%s/api/v1/repositories/%s/refs/%s/merge/master", t.ServerAddress, repo, branch),
		Body:   []byte("{}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
}
