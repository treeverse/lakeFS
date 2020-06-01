package loadtesting

import (
	"fmt"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type TargetCreator struct {
}

type TargetGenerator struct {
	ServerAddress string
}

func (t *TargetGenerator) GenerateCreateFileTargets(repo, branch string, num int) []Request {
	now := time.Now().UnixNano()
	result := make([]Request, num)
	boundary := "---------------------abcdefg123456789"
	for i := 0; i < num; i++ {
		fileContent := "--" + boundary + "\n" +
			"Content-Disposition: form-data; name=\"content\"; filename=\"file\"\n" +
			"Content-Type: text/plain\n\n" +
			strconv.Itoa(rand.Int()) + "\n" + "--" + boundary + "--\n"
		tgt := vegeta.Target{
			Method: "POST",
			URL:    fmt.Sprintf("%s/repositories/%s/branches/%s/objects?path=file_%d_%d", t.ServerAddress, repo, branch, now, i),
			Body:   []byte(fileContent),
			Header: http.Header{
				http.CanonicalHeaderKey("Accept"):          []string{"*/*"},
				http.CanonicalHeaderKey("Accept-Encoding"): []string{"gzip, deflate"},
				http.CanonicalHeaderKey("Content-Type"):    []string{"multipart/form-data; boundary=" + boundary},
				http.CanonicalHeaderKey("Content-Length"):  []string{strconv.Itoa(len(fileContent))},
			},
		}
		result[i] = Request{
			Target:      tgt,
			RequestType: "createFile",
		}
	}
	return result
}

func (t *TargetGenerator) GenerateCommitTarget(repo, msg string) Request {
	tgt := vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("%s/repositories/%s/branches/master/commits", t.ServerAddress, repo),
		Body:   []byte(fmt.Sprintf(`{"message":"%s","metadata":{}}`, msg)),
		Header: getDefaultHeader(),
	}
	res := Request{
		Target:      tgt,
		RequestType: "commit",
	}
	return res
}

func (t *TargetGenerator) GenerateBranchTarget(repo, name string) Request {
	tgt := vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("%s/repositories/%s/branches", t.ServerAddress, repo),
		Body:   []byte(fmt.Sprintf(`{"id":"%s","sourceRefId":"master"}`, name)),
		Header: getDefaultHeader(),
	}
	res := Request{
		Target:      tgt,
		RequestType: "createBranch",
	}
	return res
}

func (t *TargetGenerator) GenerateMergeToMasterTarget(repo, branch string) Request {
	tgt := vegeta.Target{
		Method: "POST",
		URL:    fmt.Sprintf("%s/repositories/%s/refs/%s/merge/master", t.ServerAddress, repo, branch),
		Body:   []byte("{}"),
		Header: getDefaultHeader(),
	}
	res := Request{
		Target:      tgt,
		RequestType: "commit",
	}
	return res
}

func getDefaultHeader() http.Header {
	return http.Header{
		http.CanonicalHeaderKey("Accept"):       []string{"application/json"},
		http.CanonicalHeaderKey("Content-Type"): []string{"application/json"},
	}
}
