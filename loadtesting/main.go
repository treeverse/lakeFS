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
	Buffer         bytes.Buffer
}

func main() {
	var tester LoadTester
	rate := vegeta.Rate{Freq: 30, Per: time.Second}
	duration := 5 * time.Second

	tgt := vegeta.Target{
		Method: "DELETE",
		URL:    "http://localhost:3000/api/v1/repositories/example-repo3",
		Body:   []byte("{}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
	streamRequest(tgt, tester)

	tgt = vegeta.Target{
		Method: "POST",
		URL:    "http://localhost:3000/api/v1/repositories",
		Body:   []byte("{\"id\":\"example-repo3\",\"bucket_name\":\"example-bucket\",\"default_branch\":\"master\"}"),
		Header: http.Header{
			http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
			http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
		},
	}
	streamRequest(tgt, tester)

	go func() {
		for i := 0; i <= 20; i++ {
			for msg := range generateFiles("example-repo3", "master", 20) {
				streamRequest(msg, tester)
			}
			streamRequest(generateCommit("example-repo3", fmt.Sprintf("commit%d", i)), tester)
			streamRequest(generateBranch("example-repo3", fmt.Sprintf("branch%d", i)), tester)

			for msg := range generateFiles("example-repo3", fmt.Sprintf("branch%d", i), 20) {
				streamRequest(msg, tester)
			}
			streamRequest(generateMergeToMaster("example-repo3", fmt.Sprintf("branch%d", i)), tester)
		}
	}()
	targeter := vegeta.NewJSONTargeter(*tester.Buffer, nil,
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

func streamRequest(tgt vegeta.Target, tester LoadTester) {
	target, err := json.Marshal(tgt)
	if err != nil {
		//TODO handle err
		return
	}
	tester.Buffer.Write(target)
	tester.Buffer.WriteString("\n")
	tester.RequestHistory = append(tester.RequestHistory, tgt)
}
func getAuth() string {
	return base64.StdEncoding.EncodeToString([]byte("AKIAJ5SI5UWYOAXGHOXQ:+/EGbyyQgXPrDgQEpJUcqsrck51W6GnBsZdAFUbA"))
}

func generateFiles(repo, branch string, num int) <-chan vegeta.Target {
	out := make(chan vegeta.Target)
	now := time.Now().UnixNano()
	go func() {
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
		close(out)
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
