package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	guuid "github.com/google/uuid"
)

var ErrEmptyBuffer = errors.New("BufferManager returned a 0 size buffer, this is a bug in the manager")

// This code adapted from azblob chunkwriting.go
// The reason is that the original code commit the data at the end of the copy
// In order to support multipart upload we need to save the blockIDs instead of committing them
// And once complete multipart is called we commit all the blockIDs

// blockWriter provides methods to upload blocks that represent a file to a server and commit them.
// This allows us to provide a local implementation that fakes the server for hermetic testing.
type blockWriter interface {
	StageBlock(context.Context, string, io.ReadSeekCloser, *blockblob.StageBlockOptions) (blockblob.StageBlockResponse, error)
	Upload(context.Context, io.ReadSeekCloser, *blockblob.UploadOptions) (blockblob.UploadResponse, error)
	CommitBlockList(context.Context, []string, *blockblob.CommitBlockListOptions) (blockblob.CommitBlockListResponse, error)
}

// copyFromReader copies a source io.Reader to blob storage using concurrent uploads.
func copyFromReader(ctx context.Context, from io.Reader, to blockWriter, o blockblob.UploadStreamOptions) (*blockblob.CommitBlockListResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	buffers := newMMBPool(o.Concurrency, o.BlockSize)
	defer buffers.Free()

	cp := &copier{
		ctx:     ctx,
		cancel:  cancel,
		reader:  from,
		to:      to,
		id:      newID(),
		o:       o,
		errCh:   make(chan error, 1),
		buffers: buffers,
	}

	// Send all our chunks until we get an error.
	var (
		err    error
		buffer []byte
	)
	for {
		select {
		case buffer = <-buffers.Acquire():
			// got a buffer
		default:
			// no buffer available; allocate a new buffer if possible
			if _, err := buffers.Grow(); err != nil {
				return nil, err
			}
			// either grab the newly allocated buffer or wait for one to become available
			buffer = <-buffers.Acquire()
		}
		err = cp.sendChunk(buffer)
		if err != nil {
			break
		}
	}
	cp.wg.Wait()
	// If the error is not EOF, then we have a problem.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	// Close out our upload.
	if err := cp.close(); err != nil {
		return nil, err
	}

	return &cp.result, nil
}

// copier streams a file via chunks in parallel from a reader representing a file.
// Do not use directly, instead use copyFromReader().
type copier struct {
	// ctx holds the context of a copier. This is normally a faux pas to store a Context in a struct. In this case,
	// the copier has the lifetime of a function call, so it's fine.
	ctx    context.Context
	cancel context.CancelFunc

	// o contains our options for uploading.
	o blockblob.UploadStreamOptions

	// id provides the ids for each chunk.
	id *id

	// reader is the source to be written to storage.
	reader io.Reader
	// to is the location we are writing our chunks to.
	to blockWriter

	// errCh is used to hold the first error from our concurrent writers.
	errCh chan error
	// wg provides a count of how many writers we are waiting to finish.
	wg sync.WaitGroup

	// result holds the final result from blob storage after we have submitted all chunks.
	result blockblob.CommitBlockListResponse

	buffers bufferManager[mmb]
}

type copierChunk struct {
	buffer []byte
	id     string
}

// getErr returns an error by priority. First, if a function set an error, it returns that error. Next, if the Context has an error
// it returns that error. Otherwise it is nil. getErr supports only returning an error once per copier.
func (c *copier) getErr() error {
	select {
	case err := <-c.errCh:
		return err
	default:
	}
	return c.ctx.Err()
}

// sendChunk reads data from out internal reader, creates a chunk, and sends it to be written via a channel.
// sendChunk returns io.EOF when the reader returns an io.EOF or io.ErrUnexpectedEOF.
func (c *copier) sendChunk(buffer []byte) error {
	// TODO(niro): Need to find a solution to all the buffers.Release
	if err := c.getErr(); err != nil {
		c.buffers.Release(buffer)
		return err
	}

	if len(buffer) == 0 {
		c.buffers.Release(buffer)
		return ErrEmptyBuffer
	}

	n, err := io.ReadFull(c.reader, buffer)
	switch {
	case err == nil && n == 0:
		c.buffers.Release(buffer)
		return nil

	case err == nil:
		nextID := c.id.next()
		c.wg.Add(1)
		// NOTE: we must pass id as an arg to our goroutine else
		// it's captured by reference and can change underneath us!
		go func(nextID string) {
			// signal that the block has been staged.
			// we MUST do this after attempting to write to errCh
			// to avoid it racing with the reading goroutine.
			defer c.wg.Done()
			defer c.buffers.Release(buffer)
			// Upload the outgoing block, matching the number of bytes read
			c.write(copierChunk{buffer: buffer[0:n], id: nextID})
		}(nextID)
		return nil

	case err != nil && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) && n == 0:
		c.buffers.Release(buffer)
		return io.EOF
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		nextID := c.id.next()
		c.wg.Add(1)
		go func(nextID string) {
			defer c.wg.Done()
			defer c.buffers.Release(buffer)
			// Upload the outgoing block, matching the number of bytes read
			c.write(copierChunk{buffer: buffer[0:n], id: nextID})
		}(nextID)
		return io.EOF
	}
	if err := c.getErr(); err != nil {
		c.buffers.Release(buffer)
		return err
	}
	c.buffers.Release(buffer)
	return err
}

// write uploads a chunk to blob storage.
func (c *copier) write(chunk copierChunk) {
	if err := c.ctx.Err(); err != nil {
		return
	}
	_, err := c.to.StageBlock(c.ctx, chunk.id, streaming.NopCloser(bytes.NewReader(chunk.buffer)), &blockblob.StageBlockOptions{
		CpkInfo:                 c.o.CpkInfo,
		CpkScopeInfo:            c.o.CpkScopeInfo,
		TransactionalValidation: c.o.TransactionalValidation,
	})
	if err != nil {
		c.errCh <- fmt.Errorf("write error: %w", err)
		return
	}
}

// close commits our blocks to blob storage and closes our writer.
func (c *copier) close() error {
	if err := c.getErr(); err != nil {
		return err
	}

	var err error
	c.result, err = c.to.CommitBlockList(c.ctx, c.id.issued(), &blockblob.CommitBlockListOptions{
		Tags:             c.o.Tags,
		Metadata:         c.o.Metadata,
		Tier:             c.o.AccessTier,
		HTTPHeaders:      c.o.HTTPHeaders,
		CpkInfo:          c.o.CpkInfo,
		CpkScopeInfo:     c.o.CpkScopeInfo,
		AccessConditions: c.o.AccessConditions,
	})
	return err
}

// id allows the creation of unique IDs based on UUID4 + an int32. This auto-increments.
type id struct {
	u   [64]byte
	num uint32
	all []string
}

// newID constructs a new id.
func newID() *id {
	uu := guuid.New()
	u := [64]byte{}
	copy(u[:], uu[:])
	return &id{u: u}
}

// next returns the next ID.
func (id *id) next() string {
	defer atomic.AddUint32(&id.num, 1)

	binary.BigEndian.PutUint32(id.u[len(guuid.UUID{}):], atomic.LoadUint32(&id.num))
	str := base64.StdEncoding.EncodeToString(id.u[:])
	id.all = append(id.all, str)

	return str
}

// issued returns all ids that have been issued. This returned value shares the internal slice, so it is not safe to modify the return.
// The value is only valid until the next time next() is called.
func (id *id) issued() []string {
	return id.all
}

// Code taken from Azure SDK for go blockblob/chunkwriting.go

// bufferManager provides an abstraction for the management of buffers.
// this is mostly for testing purposes, but does allow for different implementations without changing the algorithm.
type bufferManager[T ~[]byte] interface {
	// Acquire returns the channel that contains the pool of buffers.
	Acquire() <-chan T

	// Release releases the buffer back to the pool for reuse/cleanup.
	Release(T)

	// Grow grows the number of buffers, up to the predefined max.
	// It returns the total number of buffers or an error.
	// No error is returned if the number of buffers has reached max.
	// This is called only from the reading goroutine.
	Grow() (int, error)

	// Free cleans up all buffers.
	Free()
}

// mmb is a memory mapped buffer
type mmb []byte

// TODO (niro): consider implementation refactoring
// newMMB creates a new memory mapped buffer with the specified size
func newMMB(size int64) (mmb, error) {
	return make(mmb, size), nil
}

// delete cleans up the memory mapped buffer
func (m *mmb) delete() {
	return
}

// mmbPool implements the bufferManager interface.
// it uses anonymous memory mapped files for buffers.
// don't use this type directly, use newMMBPool() instead.
type mmbPool struct {
	buffers chan mmb
	count   int
	max     int
	size    int64
}

func newMMBPool(maxBuffers int, bufferSize int64) bufferManager[mmb] {
	return &mmbPool{
		buffers: make(chan mmb, maxBuffers),
		max:     maxBuffers,
		size:    bufferSize,
	}
}

func (pool *mmbPool) Acquire() <-chan mmb {
	return pool.buffers
}

func (pool *mmbPool) Grow() (int, error) {
	if pool.count < pool.max {
		buffer, err := newMMB(pool.size)
		if err != nil {
			return 0, err
		}
		pool.buffers <- buffer
		pool.count++
	}
	return pool.count, nil
}

func (pool *mmbPool) Release(buffer mmb) {
	pool.buffers <- buffer
}

func (pool *mmbPool) Free() {
	for i := 0; i < pool.count; i++ {
		buffer := <-pool.buffers
		buffer.delete()
	}
	pool.count = 0
}
