package hook

import (
	"strings"

	"github.com/Shopify/go-lua"
)

// helpers for writing lua actions

// HookFailureError indicates an explicit failure from a hook
// (as opposed to a generic error that occurred during execution)
type HookFailureError string

func (e HookFailureError) Error() string {
	return string(e)
}

func Open(l *lua.State) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, library)
		return 1
	}
	lua.Require(l, "hook", open, false)
	l.Pop(1)
}

var library = []lua.RegistryFunction{
	{Name: "fail", Function: fail},
}

func fail(l *lua.State) int {
	p := lua.CheckString(l, 1)
	lua.Errorf(l, "<HookFailure>%s</HookFailure>", p)
	panic("unreachable")
}

func Unwrap(err error) error {
	switch err.(type) {
	case lua.RuntimeError, *lua.RuntimeError:
		str := err.Error()
		_, after, found := strings.Cut(str, "<HookFailure>")
		if found {
			before, _, _ := strings.Cut(after, "</HookFailure>")
			return HookFailureError(before)
		}
	}
	return err
}
