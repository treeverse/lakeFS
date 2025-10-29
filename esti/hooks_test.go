package esti

import (
	"testing"
)

func TestHooksSuccess(t *testing.T) {
	t.Parallel()
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	HooksSuccessTest(ctx, t, repo, client)
}
