package operations

import (
	"errors"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

const amzMetaHeaderPrefix = "X-Amz-Meta-"

var (
	rfc2047Decoder = new(mime.WordDecoder)
)

// amzMetaAsMetadata prepare metadata based on amazon user metadata request headers
func amzMetaAsMetadata(req *http.Request) (catalog.Metadata, error) {
	metadata := make(catalog.Metadata)
	var err error
	for k := range req.Header {
		if strings.HasPrefix(k, amzMetaHeaderPrefix) {
			value, decodeErr := rfc2047Decoder.DecodeHeader(req.Header.Get(k))
			if decodeErr != nil {
				err = errors.Join(err, decodeErr)
				continue
			}
			metadata[k] = value
		}
	}
	return metadata, err
}

// amzMetaWriteHeaders set amazon user metadata on http response
func amzMetaWriteHeaders(w http.ResponseWriter, metadata catalog.Metadata) {
	h := w.Header()
	for k, v := range metadata {
		if strings.HasPrefix(k, amzMetaHeaderPrefix) {
			h.Set(k, mime.QEncoding.Encode("utf-8", v))
		}
	}
}

const amzMetadataDirectiveHeaderPrefix = "X-Amz-Metadata-Directive"

// Per S3 API, if the header X-Amz-Metadata-Directive is set to 'REPLACE', the metadata should be replaced.
// Otherwise, if the value is 'COPY' or the value is missing, it should be copied.
func shouldReplaceMetadata(req *http.Request) bool {
	return req.Header.Get(amzMetadataDirectiveHeaderPrefix) == "REPLACE"
}

func (o *PathOperation) finishUpload(req *http.Request, mTime *time.Time, checksum, physicalAddress string, size int64, relative bool, metadata map[string]string, contentType string, allowOverwrite bool) error {
	var writeTime time.Time
	if mTime == nil {
		writeTime = time.Now()
	} else {
		writeTime = *mTime
	}
	// write metadata
	entry := catalog.NewDBEntryBuilder().
		Path(o.Path).
		RelativeAddress(relative).
		PhysicalAddress(physicalAddress).
		Checksum(checksum).
		Metadata(metadata).
		Size(size).
		CreationDate(writeTime).
		ContentType(contentType).
		Build()

	err := o.Catalog.CreateEntry(req.Context(), o.Repository.Name, o.Reference, entry, graveler.WithIfAbsent(!allowOverwrite))
	if err != nil {
		o.Log(req).WithError(err).Error("could not update metadata")
		return err
	}
	tookMeta := time.Since(writeTime)
	o.Log(req).WithFields(logging.Fields{
		"took": tookMeta,
	}).Debug("metadata update complete")
	return nil
}
