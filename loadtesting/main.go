package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	vegeta "github.com/tsenart/vegeta/lib"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type LoadTester struct {
	RequestHistory []vegeta.Target
	Buffer         *bytes.Buffer
}

func main() {
	var tester LoadTester
	tester.Buffer = new(bytes.Buffer)
	rate := vegeta.Rate{Freq: 30, Per: time.Second}
	duration := 5 * time.Second
	out := make(chan vegeta.Target)
	errs := make(chan error)
	go tester.playScenario(out)
	go tester.streamRequests(out, errs)
	targeter := vegeta.NewJSONTargeter(tester.Buffer, nil,
		http.Header{http.CanonicalHeaderKey("Authorization"): []string{"Basic " + getAuth()}})
	attacker := vegeta.NewAttacker()
	var metrics vegeta.Metrics
	for res := range attacker.Attack(targeter, rate, duration, "Big Bang!") {
		if len(res.Error) > 0 {
			fmt.Println(fmt.Sprintf("Error in request %s", tester.RequestHistory[res.Seq]))
		}
		metrics.Add(res)
	}
	metrics.Close()
	fmt.Printf("99th percentile: %s\n", metrics.Latencies.P99)
	fmt.Printf("errors: %s\n", metrics.Errors)
}
func (t *LoadTester) playScenario(out chan<- vegeta.Target) {
	defer close(out)
	out <- vegeta.Target{
		Method: "DELETE",
		URL:    "http://localhost:3000/api/v1/repositories/example-repo3",
		Body:   []byte("{}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
	out <- vegeta.Target{
		Method: "POST",
		URL:    "http://localhost:3000/api/v1/repositories",
		Body:   []byte("{\"id\":\"example-repo3\",\"bucket_name\":\"example-bucket\",\"default_branch\":\"master\"}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
	for i := 0; i <= 20; i++ {
		for tgt := range generateFiles("example-repo3", "master", 20) {
			out <- tgt
		}
		out <- generateCommit("example-repo3", fmt.Sprintf("commit%d", i))
		out <- generateBranch("example-repo3", fmt.Sprintf("branch%d", i))

		for tgt := range generateFiles("example-repo3", fmt.Sprintf("branch%d", i), 20) {
			out <- tgt
		}
		out <- generateMergeToMaster("example-repo3", fmt.Sprintf("branch%d", i))
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
		t.Buffer.WriteString("\n")
	}
}

func getAuth() string {
	return base64.StdEncoding.EncodeToString([]byte("AKIAJ5SI5UWYOAXGHOXQ:+/EGbyyQgXPrDgQEpJUcqsrck51W6GnBsZdAFUbA"))
}

func generateFiles(repo, branch string, num int) <-chan vegeta.Target {
	out := make(chan vegeta.Target)
	now := time.Now().UnixNano()
	go func() {
		defer close(out)
		for i := 1; i <= num; i++ {
			fileContent := "--noonoo" + "\n" +
				"Content-Disposition: form-data; name=\"content\"; filename=\"file_981\"\n" +
				"Content-Type: text/plain\n\n" +
				string(rand.Int()) + "\n" + "--noonoo--\n"
			tgt := vegeta.Target{
				Method: "POST",
				URL:    fmt.Sprintf("http://localhost:3000/api/v1/repositories/%s/branches/%s/objects?path=file_%d_%d", repo, branch, now, i),
				Body:   []byte(fileContent),
				Header: http.Header{
					http.CanonicalHeaderKey("Accept"):          []string{"*/*"},
					http.CanonicalHeaderKey("Accept-Encoding"): []string{"gzip, deflate"},
					http.CanonicalHeaderKey("Content-Type"):    []string{"multipart/form-data; boundary=noonoo"},
					http.CanonicalHeaderKey("Content-Length"):  []string{strconv.Itoa(len(fileContent))},
				},
			}
			out <- tgt
		}
	}()
	return out
}

func generateCommit(repo, msg string) vegeta.Target {
	return vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://localhost:3000/api/v1/repositories/%s/branches/master/commits", repo),
		Body:   []byte(fmt.Sprintf(`{"message":"%s","metadata":{}}`, msg)),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
}

func generateBranch(repo, name string) vegeta.Target {
	return vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://localhost:3000/api/v1/repositories/%s/branches", repo),
		Body:   []byte(fmt.Sprintf(`{"id":"%s","sourceRefId":"master"}`, name)),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
}

func generateMergeToMaster(repo, branch string) vegeta.Target {
	return vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("http://localhost:3000/api/v1/repositories/%s/refs/%s/merge/master", repo, branch),
		Body:   []byte("{}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
}
