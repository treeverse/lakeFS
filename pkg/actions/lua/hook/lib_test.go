package hook_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/actions/lua/hook"

	"github.com/Shopify/go-lua"
)

const scriptWithExplicitFailure = `
hook = require("hook")

hook.fail("this hook shall not pass")
`

const scriptWithExplicitError = `
error("oh no")
`

const scriptWithSyntaxError = `
local a = 15
a += "a"
`

func TestUnwrap(t *testing.T) {
	t.Run("explicit fail", func(t *testing.T) {
		l := lua.NewState()
		lua.OpenLibraries(l)
		hook.Open(l)
		err := lua.DoString(l, scriptWithExplicitFailure)
		if err == nil {
			t.Error("expected error but got none!")
		}
		before := err
		after := hook.Unwrap(before)
		if after.Error() != "this hook shall not pass" {
			t.Errorf("could not unwrap lua hook error, got %s", after.Error())
		}
	})
	t.Run("regular error", func(t *testing.T) {
		l := lua.NewState()
		lua.OpenLibraries(l)
		hook.Open(l)
		err := lua.DoString(l, scriptWithExplicitError)
		if err == nil {
			t.Error("expected error but got none!")
		}
		before := err
		after := hook.Unwrap(err)
		if after.Error() != before.Error() {
			t.Error("unwrapping things not returned by hook.fail should not change the error")
		}
	})
	t.Run("syntax error", func(t *testing.T) {
		l := lua.NewState()
		lua.OpenLibraries(l)
		hook.Open(l)
		err := lua.DoString(l, scriptWithSyntaxError)
		if err == nil {
			t.Error("expected error but got none!")
		}
		before := err
		after := hook.Unwrap(err)
		if after.Error() != before.Error() {
			t.Error("unwrapping things not returned by hook.fail should not change the error")
		}
	})
	t.Run("nil error", func(t *testing.T) {
		after := hook.Unwrap(nil)
		if after != nil {
			t.Error("unwrapping nil should return nil")
		}
	})
}
