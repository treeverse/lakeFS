package loadtest

import (
	"fmt"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const boundary = "---------------------abcdefg123456789"

type TargetGenerator struct {
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

func defaultTarget(method, url, body, typ string) vegeta.Target {
	tgt := vegeta.Target{
		Method: method, URL: url, Body: []byte(body), Header: getDefaultHeader(),
	}
	AddRequestType(&tgt, typ)
	return tgt
}

func (t *TargetGenerator) GenerateCreateFileTargets(repo, branch string, num int) []vegeta.Target {
	now := time.Now().UnixNano()
	result := make([]vegeta.Target, num)
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
		AddRequestType(&tgt, "createFile")
		result[i] = tgt
	}
	return result
}

func (t *TargetGenerator) GenerateCommitTarget(repo, msg string) vegeta.Target {
	return defaultTarget("POST",
		fmt.Sprintf("%s/repositories/%s/branches/master/commits", t.ServerAddress, repo),
		fmt.Sprintf(`{"message":"%s","metadata":{}}`, msg),
		"commit")
}

func (t *TargetGenerator) GenerateBranchTarget(repo, name string) vegeta.Target {
	return defaultTarget("POST",
		fmt.Sprintf("%s/repositories/%s/branches", t.ServerAddress, repo),
		fmt.Sprintf(`{"id":"%s","sourceRefId":"master"}`, name),
		"createBranch")
}

func (t *TargetGenerator) GenerateMergeToMasterTarget(repo, branch string) vegeta.Target {
	return defaultTarget("POST",
		fmt.Sprintf("%s/repositories/%s/refs/%s/merge/master", t.ServerAddress, repo, branch),
		"{}",
		"merge")

}

func (t *TargetGenerator) GenerateListTarget(repo, branch string, amount int) vegeta.Target {
	return defaultTarget("GET",
		fmt.Sprintf("%s/repositories/%s/refs/%s/objects/ls?tree=%s&amount=%d&after=&", t.ServerAddress, repo, branch, randomFilepath(""), amount),
		"{}",
		fmt.Sprintf("list%d", amount))
}

func (t *TargetGenerator) GenerateDiffTarget(repo, branch string) vegeta.Target {
	return defaultTarget("GET",
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

func AddRequestType(tgt *vegeta.Target, typ string) {
	tgt.URL = addTypeToURL(tgt.URL, typ)
}

func GetRequestType(res vegeta.Result) string {
	return getTypeFromURL(res.URL)
}

func addTypeToURL(u, typ string) string {
	parsedUrl, err := url.Parse(u)
	if err != nil {
		return u
	}
	parsedQuery, err := url.ParseQuery(parsedUrl.RawQuery)
	if err != nil {
		return u
	}
	parsedQuery.Add("loader-request-type", typ)
	parsedUrl.RawQuery = parsedQuery.Encode()
	return parsedUrl.String()
}

func getTypeFromURL(u string) string {
	parsedUrl, err := url.Parse(u)
	if err != nil {
		return ""
	}
	parsedQuery, err := url.ParseQuery(parsedUrl.RawQuery)
	if err != nil {
		return ""
	}
	return parsedQuery.Get("loader-request-type")
}
