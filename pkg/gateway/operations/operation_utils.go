package operations

import (
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/logging"
)

const amzMetaHeaderPrefix = "X-Amz-Meta-"

// amzMetaAsMetadata prepare metadata based on amazon user metadata request headers
func amzMetaAsMetadata(req *http.Request) catalog.Metadata {
	metadata := make(catalog.Metadata)
	for k := range req.Header {
		if strings.HasPrefix(k, amzMetaHeaderPrefix) {
			metadata[k] = req.Header.Get(k)
		}
	}
	return metadata
}

// amzMetaWriteHeaders set amazon user metadata on http response
func amzMetaWriteHeaders(w http.ResponseWriter, metadata catalog.Metadata) {
	h := w.Header()
	for k, v := range metadata {
		if strings.HasPrefix(k, amzMetaHeaderPrefix) {
			h.Set(k, v)
		}
	}
}

const amzMetadataDirectiveHeaderPrefix = "X-Amz-Metadata-Directive"

// Per S3 API, if the header X-Amz-Metadata-Directive is set to 'REPLACE', the metadata should be replaced.
// Otherwise, if the value is 'COPY' or the value is missing, it should be copied.
func shouldReplaceMetadata(req *http.Request) bool {
	return req.Header.Get(amzMetadataDirectiveHeaderPrefix) == "REPLACE"
}

func (o *PathOperation) finishUpload(req *http.Request, checksum, physicalAddress string, size int64, relative bool, metadata map[string]string, contentType string) error {
	// write metadata
	writeTime := time.Now()
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

	err := o.Catalog.CreateEntry(req.Context(), o.Repository.Name, o.Reference, entry)
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
