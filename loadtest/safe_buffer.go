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
	return s.Buffer.Write(p)
}

func (s *SafeBuffer) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.Buffer.Read(p)
}
