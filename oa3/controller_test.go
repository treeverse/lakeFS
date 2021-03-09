package oa3_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/treeverse/lakefs/oa3"
)

func TestClientExample(t *testing.T) {
	t.SkipNow() // This is just example code for how to use the generated client.
	basicAuth, _ := securityprovider.NewSecurityProviderBasicAuth(
		"AKIAXXXXXXXXXXXXXXXX",
		"ClYzG60uUlcAO3gAHPHH3U3WuILQfCrPtEXAMPLE")
	c, err := oa3.NewClientWithResponses(
		"http://localhost:8000/api/v2",
		oa3.WithRequestEditorFn(basicAuth.Intercept))
	if err != nil {
		panic(err)
	}
	commit, err := c.GetCommitWithResponse(context.Background(), "my-repo", "my-commit")
	if err != nil {
		panic(err)
	}
	if commit.StatusCode() == 200 {
		// get response body
		fmt.Printf("got commit %s: created: %s\n",
			time.Unix(commit.JSON200.CreationDate, 0),
			commit.JSON200.Message,
		)
	}
}
