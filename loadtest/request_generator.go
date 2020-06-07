package loadtest

import (
	"fmt"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const boundary = "---------------------abcdefg123456789"

type RequestGenerator struct {
	ServerAddress string
}

func randomFilepath(basename string) string {
	var sb strings.Builder
	depth := rand.Intn(10)
	for i := 0; i < depth; i++ {
		dirSuffix := rand.Intn(3)
		sb.WriteString(fmt.Sprintf("dir%d/", dirSuffix))
	}
	return sb.String() + basename
}

func defaultRequest(method, url, body, typ string) Request {
	tgt := vegeta.Target{
		Method: method, URL: url, Body: []byte(body), Header: getDefaultHeader(),
	}
	return NewRequest(tgt, typ)
}

func (t *RequestGenerator) GenerateCreateFileTargets(repo, branch string, num int) []Request {
	now := time.Now().UnixNano()
	result := make([]Request, num)
	for i := 0; i < num; i++ {
		fileContent := "--" + boundary + "\n" +
			"Content-Disposition: form-data; name=\"content\"; filename=\"file\"\n" +
			"Content-Type: text/plain\n\n" +
			strconv.Itoa(rand.Int()) + "\n" + "--" + boundary + "--\n"
		filename := randomFilepath(fmt.Sprintf("file_%d_%d", now, i))
		tgt := vegeta.Target{
			Method: "POST",
			URL:    fmt.Sprintf("%s/repositories/%s/branches/%s/objects?path=%s", t.ServerAddress, repo, branch, filename),
			Body:   []byte(fileContent),
			Header: http.Header{
				http.CanonicalHeaderKey("Accept"):          []string{"*/*"},
				http.CanonicalHeaderKey("Accept-Encoding"): []string{"gzip, deflate"},
				http.CanonicalHeaderKey("Content-Type"):    []string{"multipart/form-data; boundary=" + boundary},
				http.CanonicalHeaderKey("Content-Length"):  []string{strconv.Itoa(len(fileContent))},
			},
		}
		result[i] = NewRequest(tgt, "createFile")
	}
	return result
}

func (t *RequestGenerator) GenerateCommitTarget(repo, msg string) Request {
	return defaultRequest("POST",
		fmt.Sprintf("%s/repositories/%s/branches/master/commits", t.ServerAddress, repo),
		fmt.Sprintf(`{"message":"%s","metadata":{}}`, msg),
		"commit")
}

func (t *RequestGenerator) GenerateBranchTarget(repo, name string) Request {
	return defaultRequest("POST",
		fmt.Sprintf("%s/repositories/%s/branches", t.ServerAddress, repo),
		fmt.Sprintf(`{"id":"%s","sourceRefId":"master"}`, name),
		"createBranch")
}

func (t *RequestGenerator) GenerateMergeToMasterTarget(repo, branch string) Request {
	return defaultRequest("POST",
		fmt.Sprintf("%s/repositories/%s/refs/%s/merge/master", t.ServerAddress, repo, branch),
		"{}",
		"merge")

}

func (t *RequestGenerator) GenerateListTarget(repo, branch string, amount int) Request {
	return defaultRequest("GET",
		fmt.Sprintf("%s/repositories/%s/refs/%s/objects/ls?tree=%s&amount=%d&after=&", t.ServerAddress, repo, branch, randomFilepath(""), amount),
		"{}",
		fmt.Sprintf("list%d", amount))
}

func (t *RequestGenerator) GenerateDiffTarget(repo, branch string) Request {
	return defaultRequest("GET",
		fmt.Sprintf("%s/repositories/%s/branches/%s/diff", t.ServerAddress, repo, branch),
		"{}",
		"diff")
}

func getDefaultHeader() http.Header {
	return http.Header{
		http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
		http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
	}
}
