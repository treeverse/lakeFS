package operations

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/gateway/utils"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway/errors"
	ghttp "github.com/treeverse/lakefs/gateway/http"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/permissions"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

type GetObject struct{}

func (controller *GetObject) Action(req *http.Request) permissions.Action {
	return permissions.GetObject(utils.GetRepo(req))
}

func (controller *GetObject) Handle(o *PathOperation) {

	query := o.Request.URL.Query()
	if _, exists := query["versioning"]; exists {
		o.EncodeResponse(serde.VersioningConfiguration{}, http.StatusOK)
		return
	}

	beforeMeta := time.Now()
    entry, err := o.Index.ReadEntryObject(o.Repo.GetRepoId(), o.Ref, o.Path)
	metaTook := time.Since(beforeMeta)
	o.Log().
		WithField("took", metaTook).
		WithField("path", o.Path).
		WithField("branch", o.Ref).
		WithError(err).
		Info("metadata operation to retrieve object done")

	if xerrors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.GetTimestamp()))
	o.SetHeader("ETag", httputil.ETag(entry.GetChecksum()))
	o.SetHeader("Accept-Ranges", "bytes")
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	// now we might need the object itself
	obj, err := o.Index.ReadObject(o.Repo.GetRepoId(), o.Ref, o.Path)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// range query
	rangeSpec := o.Request.Header.Get("Range")
	if len(rangeSpec) > 0 {
		ranger, err := NewObjectRanger(rangeSpec, o.Repo.GetBucketName(), obj, o.BlockStore, o.Log())
		if err != nil {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidRange))
			return
		}
		// range query response
		expected := ranger.Range.EndOffset - ranger.Range.StartOffset + 1 // both range ends are inclusive
		o.SetHeader("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", ranger.Range.StartOffset, ranger.Range.EndOffset, obj.GetSize()))
		o.SetHeader("Content-Length",
			fmt.Sprintf("%d", expected))
		o.ResponseWriter.WriteHeader(http.StatusOK)
		n, err := io.Copy(o.ResponseWriter, ranger)
		if err != nil {
			o.Log().WithError(err).Errorf("could not copy range to response")
			return
		}
		l := o.Log().WithFields(log.Fields{
			"range":   ranger.Range,
			"path":    o.Path,
			"branch":  o.Ref,
			"written": n,
		})
		if n != expected {
			l.WithField("expected", expected).Error("got object range - didn't write the correct amount of bytes!?!!")
		} else {
			l.Info("read the byte range requested")
		}
		return
	}

	// assemble a response body (range-less query)
	o.SetHeader("Content-Length", fmt.Sprintf("%d", obj.GetSize()))
	blocks := obj.GetBlocks()
	for _, block := range blocks {
		data, err := o.BlockStore.Get(o.Repo.GetBucketName(), block.GetAddress())
		if err != nil {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		_, err = io.Copy(o.ResponseWriter, data)
		if err != nil {
			o.Log().WithError(err).Error("could not write response body for object")
		}
	}
}

// Utility tool to help with range requests
type ObjectRanger struct {
	Range   ghttp.HttpRange
	Repo    string
	obj     *model.Object
	adapter block.Adapter
	logger  *logrus.Entry

	offset       int64 // offset within the range
	rangeBuffer  []byte
	rangeAddress string
}

func NewObjectRanger(spec string, repo string, obj *model.Object, adapter block.Adapter, logger *log.Entry) (*ObjectRanger, error) {
	// let's start by deciding which blocks we actually need
	rang, err := ghttp.ParseHTTPRange(spec, obj.GetSize())
	if err != nil {
		logger.WithError(err).Error("failed to parse spec")
		return nil, err
	}
	return &ObjectRanger{
		Repo:    repo,
		Range:   rang,
		obj:     obj,
		logger:  logger,
		adapter: adapter,
	}, nil
}

func (r *ObjectRanger) Read(p []byte) (int, error) {
	rangeStart := r.Range.StartOffset + r.offset // assuming we already read some of that range
	rangeEnd := r.Range.EndOffset + 1            // we read the last byte inclusively
	var scanned int64
	bufSize := len(p)

	var returnedErr error
	var n int
	for _, block := range r.obj.GetBlocks() {
		// see what range we need from this block
		thisBlockStart := scanned
		thisBlockEnd := scanned + block.GetSize()
		scanned += block.GetSize()

		if thisBlockEnd <= rangeStart {
			continue // we haven't yet reached a block we need
		}

		if thisBlockStart >= rangeEnd {
			returnedErr = io.EOF
			break // we're done, no need to read further blocks
		}

		// by now we're at a block that we at least need a range from, let's determine it, start position first
		var startPosition, endPosition int64
		if rangeStart > thisBlockStart {
			startPosition = rangeStart - thisBlockStart
		} else {
			startPosition = 0
		}

		if rangeEnd > thisBlockEnd {
			// we need to read this entire block
			endPosition = block.GetSize()
		} else {
			// we need to read up to rangeEnd
			endPosition = rangeEnd - thisBlockStart
		}

		// read the actual data required from the block
		var data []byte
		cacheKey := fmt.Sprintf("%s:%d-%d", block.GetAddress(), startPosition, endPosition)
		if strings.EqualFold(r.rangeAddress, cacheKey) {
			data = r.rangeBuffer[startPosition:endPosition]
		} else {
			reader, err := r.adapter.GetRange(r.Repo, block.GetAddress(), startPosition, endPosition)
			if err != nil {
				return n, err
			}
			data = make([]byte, endPosition-startPosition)
			currN, err := reader.Read(data)
			_ = reader.Close()
			if err != nil {
				return n, err
			}
			data = data[0:currN] // truncate unread bytes
			r.rangeBuffer = data
			r.rangeAddress = cacheKey
		}

		// feed it into the buffer until no more space in buffer or no more data left
		i := 0
		for n < bufSize && i < len(data) {
			p[n] = data[i]
			n++
			i++
			r.offset++
		}
		if n == bufSize {
			// buffer is full, next iteration.
			return n, nil
		}
	}
	// done
	if n < bufSize {
		returnedErr = io.EOF
	}
	return n, returnedErr
}
