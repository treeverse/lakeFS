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

	"github.com/Azure/azure-storage-blob-go/azblob"
	guuid "github.com/google/uuid"
)

var ErrEmptyBuffer = errors.New("TransferManager returned a 0 size buffer, this is a bug in the manager")

// This code is taken from azblob chunkwriting.go
// The reason is that the original code commit the data at the end of the copy
// In order to support multipart upload we need to save the blockIDs instead of committing them
// And once complete multipart is called we commit all the blockIDs

// blockWriter provides methods to upload blocks that represent a file to a server and commit them.
// This allows us to provide a local implementation that fakes the server for hermetic testing.
type blockWriter interface {
	StageBlock(context.Context, string, io.ReadSeeker, azblob.LeaseAccessConditions, []byte, azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobStageBlockResponse, error)
	CommitBlockList(context.Context, []string, azblob.BlobHTTPHeaders, azblob.Metadata, azblob.BlobAccessConditions, azblob.AccessTierType, azblob.BlobTagsMap, azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobCommitBlockListResponse, error)
}

// copyFromReader copies a source io.Reader to blob storage using concurrent uploads.
func copyFromReader(ctx context.Context, from io.Reader, to blockWriter, o azblob.UploadStreamToBlockBlobOptions) (*azblob.BlockBlobCommitBlockListResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cp := &copier{
		ctx:    ctx,
		cancel: cancel,
		reader: from,
		to:     to,
		id:     newID(),
		o:      o,
		errCh:  make(chan error, 1),
	}

	// Send all our chunks until we get an error.
	var err error
	for {
		if err = cp.sendChunk(); err != nil {
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

	return cp.result, nil
}

// copier streams a file via chunks in parallel from a reader representing a file.
// Do not use directly, instead use copyFromReader().
type copier struct {
	// ctx holds the context of a copier. This is normally a faux pas to store a Context in a struct. In this case,
	// the copier has the lifetime of a function call, so its fine.
	ctx    context.Context
	cancel context.CancelFunc

	// o contains our options for uploading.
	o azblob.UploadStreamToBlockBlobOptions

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
	result *azblob.BlockBlobCommitBlockListResponse
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
func (c *copier) sendChunk() error {
	if err := c.getErr(); err != nil {
		return err
	}

	buffer := c.o.TransferManager.Get()
	if len(buffer) == 0 {
		return ErrEmptyBuffer
	}

	n, err := io.ReadFull(c.reader, buffer)
	switch {
	case err == nil && n == 0:
		return nil
	case err == nil:
		id := c.id.next()
		c.wg.Add(1)
		c.o.TransferManager.Run(
			func() {
				defer c.wg.Done()
				c.write(copierChunk{buffer: buffer[0:n], id: id})
			},
		)
		return nil
	case err != nil && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) && n == 0:
		return io.EOF
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		id := c.id.next()
		c.wg.Add(1)
		c.o.TransferManager.Run(
			func() {
				defer c.wg.Done()
				c.write(copierChunk{buffer: buffer[0:n], id: id})
			},
		)
		return io.EOF
	}
	if err := c.getErr(); err != nil {
		return err
	}
	return err
}

// write uploads a chunk to blob storage.
func (c *copier) write(chunk copierChunk) {
	defer c.o.TransferManager.Put(chunk.buffer)

	if err := c.ctx.Err(); err != nil {
		return
	}
	_, err := c.to.StageBlock(c.ctx, chunk.id, bytes.NewReader(chunk.buffer), c.o.AccessConditions.LeaseAccessConditions, nil, c.o.ClientProvidedKeyOptions)
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
	c.result, err = c.to.CommitBlockList(c.ctx, c.id.issued(), c.o.BlobHTTPHeaders, c.o.Metadata, c.o.AccessConditions, c.o.BlobAccessTier, c.o.BlobTagsMap, c.o.ClientProvidedKeyOptions)
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

	binary.BigEndian.PutUint32((id.u[len(guuid.UUID{}):]), atomic.LoadUint32(&id.num))
	str := base64.StdEncoding.EncodeToString(id.u[:])
	id.all = append(id.all, str)

	return str
}

// issued returns all ids that have been issued. This returned value shares the internal slice so it is not safe to modify the return.
// The value is only valid until the next time next() is called.
func (id *id) issued() []string {
	return id.all
}
