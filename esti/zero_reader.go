package esti

import "io"

// ZeroReader reads only zeros into the provided buffer, until `Amount` is reached.
type ZeroReader struct {
	Amount       int
	NumBytesRead int
}

func NewZeroReader(amount int) *ZeroReader {
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
	if zr.NumBytesRead+n > zr.Amount {
		n = zr.Amount - zr.NumBytesRead
	}
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	zr.NumBytesRead += n
	return n, nil
}
