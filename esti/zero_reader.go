package esti

import (
	"errors"
	"io"
)

var ErrInvalidWhence = errors.New("bytes.Reader.Seek: invalid whence")

// ZeroReader reads only zeros into the provided buffer, until `Amount` is reached.
type ZeroReader struct {
	Amount       int64
	NumBytesRead int64
}

func NewZeroReader(amount int64) *ZeroReader {
	return &ZeroReader{
		Amount:       amount,
		NumBytesRead: 0,
	}
}

func (zr *ZeroReader) Read(p []byte) (n int, err error) {
	if zr.NumBytesRead >= zr.Amount {
		return 0, io.EOF
	}
	n = len(p)
	if zr.NumBytesRead+int64(n) > zr.Amount {
		n = int(zr.Amount - zr.NumBytesRead)
	}
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	zr.NumBytesRead += int64(n)
	return n, nil
}

func (zr *ZeroReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		zr.NumBytesRead = offset
	case io.SeekCurrent:
		zr.NumBytesRead += offset
	case io.SeekEnd:
		zr.NumBytesRead = max(zr.Amount-offset, 0)
	default:
		return 0, ErrInvalidWhence
	}
	return zr.NumBytesRead, nil
}
