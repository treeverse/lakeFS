package testcode

import (
	"errors"
	"testing"
	"time"
)

type BaseArithmetic struct{}

func (b BaseArithmetic) Add(x, y int) int {
	time.Sleep(100 * time.Millisecond)
	return x + y
}

const fail = -1

var (
	errForTesting = errors.New("for testing")
)

func (b BaseArithmetic) Double(x int) (int, error) {
	time.Sleep(50 * time.Millisecond)
	if x == fail {
		return 0, errForTesting
	}
	return 2 * x, nil
}

var b Arithmetic = BaseArithmetic{}

func TestPassthrough(t *testing.T) {
	w := &MonitoredArithmetic{
		Wrapped: b,
		Observe: func(op string, dur time.Duration, success bool) {
			t.Errorf("Unexpected callback Observe(%s, %s, %v)", op, dur, success)
		},
	}
	four := w.Add(2, 2)
	if four != 4 {
		t.Errorf("Unexpected Add(2, 2) == %v", four)
	}
}

func TestMonitoring(t *testing.T) {
	var (
		called      bool
		lastSuccess bool
	)

	w := &MonitoredArithmetic{
		Wrapped: b,
		Observe: func(op string, dur time.Duration, success bool) {
			if op != "double" {
				t.Errorf("Got op %s expected \"double\"", op)
			}
			if dur < 50*time.Millisecond {
				t.Errorf("Measured duration %s is too short for method that sleeps 50msecs", dur)
			}
			called = true
			lastSuccess = success
		},
	}

	called = false
	four, err := w.Double(2)
	if !called {
		t.Error("Observe not called")
	}
	if !lastSuccess {
		t.Error("Observed unexpected failure")
	}
	if four != 4 {
		t.Errorf("Unexpected Double(s) == %v", four)
	}

	called = false
	_, err = w.Double(fail)
	if !called {
		t.Error("Observe not called")
	}
	if lastSuccess {
		t.Error("Observed success")
	}
	if err != errForTesting {
		t.Errorf("Unexpected error %s", err)
	}
}
