package mergeBranch_test

import (
	"crypto/rand"
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
	str "strings"
)

// CREATING SIMULATED CONTENT

type contentCreator struct {
	pos       int64
	maxLength int64
	name      string
}

func NewReader(max int64, name string) *contentCreator {
	var r *contentCreator
	r = new(contentCreator)
	r.maxLength = max
	r.name = name
	return r
}

const stepSize = 20

func (c *contentCreator) Name() string {
	return c.name
}

func (c *contentCreator) Read(b []byte) (int, error) {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if c.pos == c.maxLength {
		return 0, io.EOF
	}
	retSize := minInt(int64(len(b)), int64(c.maxLength-c.pos))
	var nextCopyPos int64
	currentBlockNo := c.pos/stepSize + 1 // first block in number 1
	//create the first block, which may be the continuation of a previous block
	remaining := (c.pos % stepSize)
	if remaining != 0 || retSize < stepSize {
		// the previous read did not end on integral stepSize boundry, or the retSize
		// is less than a block
		copiedSize := int64(copy(b, makeBlock(currentBlockNo)[remaining:minInt(stepSize, remaining+retSize)]))
		nextCopyPos = copiedSize
		currentBlockNo++
	}
	// create the blocks between first and last. Those are always full
	fullBlocksNo := (retSize - nextCopyPos) / stepSize
	for i := int64(0); i < fullBlocksNo; i++ {
		copy(b[nextCopyPos:nextCopyPos+stepSize], makeBlock(currentBlockNo))
		currentBlockNo++
		nextCopyPos += stepSize
	}
	// create the trailing block (if needed)
	remainingBytes := (c.pos + retSize) % stepSize
	if remainingBytes != 0 && (c.pos%stepSize+retSize) > stepSize {
		// destination will be smaller than the returned block, copy size is the minimum size of dest and source
		copy(b[nextCopyPos:nextCopyPos+remainingBytes], makeBlock(currentBlockNo)) // destination will be smaller than step size
	}
	c.pos += retSize
	if c.pos == c.maxLength {
		return int(retSize), io.EOF
	} else {
		if c.pos < c.maxLength {
			return int(retSize), nil
		} else {
			log.Panic("reader programming error - got past maxLength")
			return int(retSize), nil
		}
	}

}

func makeBlock(n int64) string {
	if n == 1 { // make first block random
		b := make([]byte, stepSize)
		rand.Read(b)
		return string(b)
	}
	f := strconv.Itoa(int(n * stepSize))
	block := str.Repeat("*", stepSize-len(f)) + f
	return block
}

func (c *contentCreator) Close() error {
	c.pos = -1
	return nil
}

func minInt(i1, i2 int64) int64 {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
}

func (c *contentCreator) Seek(seekPoint int64) error {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if seekPoint >= c.maxLength {
		return io.EOF
	}
	c.pos = seekPoint
	return nil
}
