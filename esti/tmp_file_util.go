package esti

import (
	crand "crypto/rand"
	"math"
	"os"
)

const defaultChunkSize = 1024

func MakeTmpFile(numBytes int) (*os.File, func() error, error) {
	file, err := os.CreateTemp("/tmp", "test")
	if err != nil {
		return nil, nil, err
	}

	chunkSizeBytes := int(math.Min(defaultChunkSize, float64(numBytes)))
	chunk := make([]byte, chunkSizeBytes)
	for written := 0; written < numBytes; written += chunkSizeBytes {
		_, err := crand.Read(chunk)
		if err != nil {
			return nil, nil, err
		}

		_, err = file.Write(chunk)
		if err != nil {
			return nil, nil, err
		}
	}
	_, seekErr := file.Seek(0, 0)
	if seekErr != nil {
		return nil, nil, seekErr
	}

	return file, func() error {
		closeErr := file.Close()
		if closeErr != nil {
			return closeErr
		}
		removeErr := os.Remove(file.Name())
		if removeErr != nil {
			return removeErr
		}
		return nil
	}, nil
}
