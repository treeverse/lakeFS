package esti

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func testHooksFailure(t *testing.T, testFunc func(context.Context, *testing.T, string, apigen.ClientWithResponsesInterface)) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	testFunc(ctx, t, repo, client)
}

func TestHooksFailure(t *testing.T) {
	t.Run("webhook", func(t *testing.T) {
		testHooksFailure(t, WebhookHooksFailureTest)
	})
	t.Run("lua", func(t *testing.T) {
		testHooksFailure(t, LuaHooksFailureTest)
	})
}
