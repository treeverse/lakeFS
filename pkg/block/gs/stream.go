package gs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

type StreamingReader struct {
	Reader       io.Reader
	Size         int
	StreamSigner *v4.StreamSigner
	Time         time.Time
	ChunkSize    int
	ChunkTimeout time.Duration

	currentChunk io.Reader
	totalRead    int
	eof          bool
}

func chunkBoundary(signature []byte, length int) []byte {
	return []byte(fmt.Sprintf("%x;chunk-signature=%s\r\n", length, hex.EncodeToString(signature)))
}

func isEOF(err error) bool {
	return err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF)
}

func (s *StreamingReader) GetLastChunk() []byte {
	res := make([]byte, 0)
	sig, _ := s.StreamSigner.GetSignature([]byte{}, []byte{}, s.Time)
	lastBoundary := chunkBoundary(sig, 0)
	res = append(res, lastBoundary...)
	res = append(res, '\r', '\n') // additional \r\n after the last boundary
	return res
}

// ReadAllWithTimeout is taken from io.ReadAtLeast and adapted to support a timeout
func ReadAllWithTimeout(r io.Reader, buf []byte, timeout time.Duration) (n int, err error) {
	desired := len(buf)

	lg := logging.Default().WithFields(logging.Fields{
		"timeout":      timeout,
		"desired_size": desired,
	})

	start := time.Now()
	timedOut := false
	for n < desired && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		n += nn

		if time.Since(start) > timeout {
			timedOut = true
			break
		}
	}
	if n >= desired {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err == nil && timedOut {
		err = ErrReaderTimeout
		lg.WithField("n", n).Warn("duration passed, reader timed out")
	}
	return
}

var ErrReaderTimeout = errors.New("reader timeout")

func (s *StreamingReader) readNextChunk() error {
	buf := make([]byte, s.ChunkSize)
	n, err := ReadAllWithTimeout(s.Reader, buf, s.ChunkTimeout)
	s.totalRead += n
	buf = buf[:n]
	if err != nil && !isEOF(err) && !errors.Is(err, ErrReaderTimeout) {
		// actual error happened
		return err
	}
	if n == 0 {
		if s.currentChunk == nil {
			s.currentChunk = bytes.NewBuffer(s.GetLastChunk())
		}
		return io.EOF
	}
	sig, sigErr := s.StreamSigner.GetSignature([]byte{}, buf, s.Time)
	if sigErr != nil {
		return sigErr
	}

	buf = append(buf, '\r', '\n') // additional \r\n after the content
	boundary := chunkBoundary(sig, n)
	if isEOF(err) || s.totalRead == s.Size {
		// we're done with the upstream Reader, let's write one last chunk boundary.
		buf = append(buf, s.GetLastChunk()...)
	}
	buf = append(boundary, buf...)
	s.currentChunk = bytes.NewBuffer(buf)
	if s.totalRead == s.Size {
		err = io.EOF
	}
	if errors.Is(err, ErrReaderTimeout) {
		return nil // a slow reader shouldn't fail.
	}
	return err
}

func (s *StreamingReader) Read(p []byte) (int, error) {
	if s.eof {
		return 0, io.EOF
	}

	var nerr error
	var currentN int
	if s.currentChunk == nil {
		nerr = s.readNextChunk()
		if nerr != nil && !isEOF(nerr) {
			return currentN, nerr
		}
	}

	for currentN < len(p) {
		n, err := s.currentChunk.Read(p[currentN:])
		currentN += n
		if err != nil && !isEOF(err) {
			return currentN, err
		}
		if isEOF(err) { // we drained the current chunk
			if isEOF(nerr) { // no more chunks to read
				s.eof = true
				return currentN, io.EOF
			}
			// otherwise, we read the entire chunk, let's fill it back up (if n < size of p)
			nerr = s.readNextChunk()
			if nerr != nil && !isEOF(nerr) {
				return currentN, nerr // something bad happened when reading the next chunk
			}
		}
	}

	return currentN, nil
}
