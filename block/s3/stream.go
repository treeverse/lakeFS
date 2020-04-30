package s3

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

const (
	StreamingSha256          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingContentEncoding = "aws-chunked"
)

func CalculateStreamSizeForPayload(sizeBytes int64, chunkSize int) int64 {
	// for each chunk we need 81 fixed chars, i.e. ;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497
	// we also add a constant \r\n after the sig, and another one after the payload, for a total for 85 fixed chars.
	// the actual boundary is prefixed by the hexadecimal string size representation of the chunk which can vary in size
	// so we need to calculate it
	const chunkSigSize = 85
	fullChunks := sizeBytes / int64(chunkSize)
	partialChunkSize := sizeBytes % int64(chunkSize)

	overhead := fullChunks * (chunkSigSize + int64(len(fmt.Sprintf("%x", chunkSize)))) // one sig for every chunk
	if partialChunkSize > 0 {
		overhead += chunkSigSize + int64(len(fmt.Sprintf("%x", partialChunkSize))) // last partial chunk if there is one
	}
	overhead += chunkSigSize + int64(len(fmt.Sprintf("%x", 0))) // trailing sig
	return sizeBytes + overhead
}

type StreamingReader struct {
	Reader       io.Reader
	Size         int
	StreamSigner *v4.StreamSigner
	Time         time.Time
	ChunkSize    int

	currentChunk io.Reader
	totalRead    int
	eof          bool
}

func chunkBoundary(signature []byte, length int) []byte {
	return []byte(fmt.Sprintf("%x;chunk-signature=%s\r\n", length, hex.EncodeToString(signature)))
}

func isEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF
}

func (s *StreamingReader) readNextChunk() error {
	buf := make([]byte, s.ChunkSize)
	n, err := io.ReadFull(s.Reader, buf)
	s.totalRead += n
	buf = buf[:n]
	if err != nil && !isEOF(err) {
		// actual error happened
		return err
	}
	if n == 0 {
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
		sig, _ := s.StreamSigner.GetSignature([]byte{}, []byte{}, s.Time)
		lastBoundary := chunkBoundary(sig, 0)
		buf = append(buf, lastBoundary...)
		buf = append(buf, '\r', '\n') // additional \r\n after the last boundary
	}
	buf = append(boundary, buf...)
	s.currentChunk = bytes.NewBuffer(buf)
	if s.totalRead == s.Size {
		err = io.EOF
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
