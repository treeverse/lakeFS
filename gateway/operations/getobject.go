package operations

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway/errors"
	ghttp "github.com/treeverse/lakefs/gateway/http"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/permissions"

	"golang.org/x/xerrors"
)

type GetObject struct{}

func (controller *GetObject) Action(repoId, refId, path string) permissions.Action {
	return permissions.GetObject(repoId)
}

func (controller *GetObject) Handle(o *PathOperation) {
	o.Incr("get_object")
	query := o.Request.URL.Query()
	if _, exists := query["versioning"]; exists {
		o.EncodeResponse(serde.VersioningConfiguration{}, http.StatusOK)
		return
	}

	beforeMeta := time.Now()
	entry, err := o.Index.ReadEntryObject(o.Repo.Id, o.Ref, o.Path)
	metaTook := time.Since(beforeMeta)
	o.Log().
		WithField("took", metaTook).
		WithError(err).
		Debug("metadata operation to retrieve object done")

	if xerrors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader("ETag", httputil.ETag(entry.Checksum))
	o.SetHeader("Accept-Ranges", "bytes")
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	// now we might need the object itself
	obj, err := o.Index.ReadObject(o.Repo.Id, o.Ref, o.Path)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// range query
	rangeSpec := o.Request.Header.Get("Range")
	if len(rangeSpec) > 0 {
		rng, err := ghttp.ParseHTTPRange(rangeSpec, obj.Size)
		if err != nil {
			o.Log().WithError(err).Error("failed to parse spec")
			return
		}
		//ranger, err := NewObjectRanger(rangeSpec, o.Repo.StorageNamespace, obj, o.BlockStore, o.Log())
		data, err := o.BlockStore.GetRange(o.Repo.StorageNamespace, obj.PhysicalAddress, rng.StartOffset, rng.EndOffset)
		if err == nil {
			// range query response
			expected := rng.EndOffset - rng.StartOffset + 1 // both range ends are inclusive
			o.SetHeader("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, obj.Size))
			o.SetHeader("Content-Length",
				fmt.Sprintf("%d", expected))
			o.ResponseWriter.WriteHeader(http.StatusOK)
			n, err := io.Copy(o.ResponseWriter, data)
			if err != nil {
				o.Log().WithError(err).Error("could not copy range to response")
				return
			}
			l := o.Log().WithFields(logging.Fields{
				"range":   rng,
				"written": n,
			})
			if n != expected {
				l.WithField("expected", expected).Error("got object range - didn't write the correct amount of bytes!?!!")
			} else {
				l.Info("read the byte range requested")
			}
			return
		}
	}

	// assemble a response body (range-less query)
	o.SetHeader("Content-Length", fmt.Sprintf("%d", obj.Size))
	data, err := o.BlockStore.Get(o.Repo.StorageNamespace, obj.PhysicalAddress)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	n, err := io.Copy(o.ResponseWriter, data)
	if err != nil {
		o.Log().WithError(err).Error("could not write response body for object")
	}
	if n != obj.Size {
		o.Log().Warnf("expected %d bytes, got %d bytes", obj.Size, n)
	}
}

//Utility tool to help with range requests
//type ObjectRanger struct {
//	Range   ghttp.HttpRange
//	Repo    string
//	obj     *model.Object
//	adapter block.Adapter
//	logger  logging.Logger
//
//	offset       int64 // offset within the range
//	rangeBuffer  []byte
//	rangeAddress string
//}
//
//func NewObjectRanger(spec string, repo string, obj *model.Object, adapter block.Adapter, logger logging.Logger) (*ObjectRanger, error) {
//	// let's start by deciding which blocks we actually need
//	rang, err := ghttp.ParseHTTPRange(spec, obj.Size)
//	if err != nil {
//		logger.WithError(err).Error("failed to parse spec")
//		return nil, err
//	}
//	return &ObjectRanger{
//		Repo:    repo,
//		Range:   rang,
//		obj:     obj,
//		logger:  logger,
//		adapter: adapter,
//	}, nil
//}
//
//func (r *ObjectRanger) Read(p []byte) (int, error) {
//	rangeStart := r.Range.StartOffset + r.offset // assuming we already read some of that range
//	rangeEnd := r.Range.EndOffset + 1            // we read the last byte inclusively
//	var scanned int64
//	bufSize := len(p)
//
//	var returnedErr error
//	var n int
//	for _, block := range r.obj.Blocks {
//		// see what range we need from this block
//		thisBlockStart := scanned
//		thisBlockEnd := scanned + block.Size
//		scanned += block.Size
//
//		if thisBlockEnd <= rangeStart {
//			continue // we haven't yet reached a block we need
//		}
//
//		if thisBlockStart >= rangeEnd {
//			returnedErr = io.EOF
//			break // we're done, no need to read further blocks
//		}
//
//		// by now we're at a block that we at least need a range from, let's determine it, start position first
//		var startPosition, endPosition int64
//		if rangeStart > thisBlockStart {
//			startPosition = rangeStart - thisBlockStart
//		} else {
//			startPosition = 0
//		}
//
//		if rangeEnd > thisBlockEnd {
//			// we need to read this entire block
//			endPosition = block.Size
//		} else {
//			// we need to read up to rangeEnd
//			endPosition = rangeEnd - thisBlockStart
//		}
//
//		// read the actual data required from the block
//		var data []byte
//		cacheKey := fmt.Sprintf("%s:%d-%d", block.Address, startPosition, endPosition)
//		if strings.EqualFold(r.rangeAddress, cacheKey) {
//			data = r.rangeBuffer[startPosition:endPosition]
//		} else {
//			reader, err := r.adapter.GetRange(r.Repo, block.Address, startPosition, endPosition)
//			if err != nil {
//				return n, err
//			}
//			data = make([]byte, endPosition-startPosition)
//			currN, err := reader.Read(data)
//			_ = reader.Close()
//			if err != nil {
//				return n, err
//			}
//			data = data[0:currN] // truncate unread bytes
//			r.rangeBuffer = data
//			r.rangeAddress = cacheKey
//		}
//
//		// feed it into the buffer until no more space in buffer or no more data left
//		i := 0
//		for n < bufSize && i < len(data) {
//			p[n] = data[i]
//			n++
//			i++
//			r.offset++
//		}
//		if n == bufSize {
//			// buffer is full, next iteration.
//			return n, nil
//		}
//	}
//	// done
//	if n < bufSize {
//		returnedErr = io.EOF
//	}
//	return n, returnedErr
//}
