package catalog

import (
	"context"
	"testing"
)

func Must(err error, t *testing.T, message string) {
	if err != nil {
		t.Fatal(message)
	}
}
func TestCataloger_CreateEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	t.Run("simple write", func(t *testing.T) {
		Must(c.CreateRepo(ctx, "example", "example-tzahi", "master"), t, "error creating repository")
		//meta := make(map[string]string)
		Must(c.CreateEntry(ctx, "example", "master", "aaa/bbb/ccc", "1234", "5678", 100, nil), t, "error creating repository")
		Must(c.CreateEntry(ctx, "example", "master", "aaa/bbb/ccc", "6789", "5678", 100, nil), t, "error creating repository")
	})

}
