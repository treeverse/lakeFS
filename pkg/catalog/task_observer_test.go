package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotifyObserver_RecoversPanic(t *testing.T) {
	called := false
	require.NotPanics(t, func() {
		notifyObserver("test-task", func() {
			called = true
			panic("observer panic")
		})
	}, "notifyObserver should recover from panic")
	require.True(t, called, "callback should have been called")
}

func TestNotifyObserver_RecoversPanicWithNilValue(t *testing.T) {
	called := false
	require.NotPanics(t, func() {
		notifyObserver("test-task", func() {
			called = true
			panic(nil) //nolint:nilpanic // intentionally testing panic(nil) recovery
		})
	}, "notifyObserver should recover from panic(nil)")
	require.True(t, called, "callback should have been called")
}

func TestNotifyObserver_RecoversPanicWithError(t *testing.T) {
	called := false
	require.NotPanics(t, func() {
		notifyObserver("test-task", func() {
			called = true
			panic(&panicError{msg: "test error"})
		})
	}, "notifyObserver should recover from panic with error")
	require.True(t, called, "callback should have been called")
}

type panicError struct {
	msg string
}

func (e *panicError) Error() string {
	return e.msg
}
