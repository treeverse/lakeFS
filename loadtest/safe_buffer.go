package loadtest

import (
	"bytes"
	"sync"
)

type SafeBuffer struct {
	bytes.Buffer
	sync.Mutex
}

func (s *SafeBuffer) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	n, err = s.Buffer.Write(p)
	if err != nil {
		return n, err
	}
	return s.WriteString("\n")
}

func (s *SafeBuffer) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.Buffer.Read(p)
}
