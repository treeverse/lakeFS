package testutil

import (
	"hash/crc64"
	"io"
)

const bufSize = 4 << 16

var table *crc64.Table = crc64.MakeTable(crc64.ECMA)

// ChecksumReader returns the checksum (CRC-64) of the contents of reader.
func ChecksumReader(reader io.Reader) (uint64, error) {
	buf := make([]byte, bufSize)
	var val uint64
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				return val, nil
			}
			return val, err
		}
		if n == 0 {
			return val, nil
		}
		val = crc64.Update(val, table, buf[:n])
	}
}
