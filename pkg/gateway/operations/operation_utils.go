package operations

import (
	"errors"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	amzMetaHeaderPrefix  = "X-Amz-Meta-"
	amzMissingMetaHeader = "X-Amz-Missing-Meta"
)

// Maximum size for user-defined metadata (2 KB). See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
const maxUserMetadataSize = 2 * 1024

var (
	rfc2047Decoder = new(mime.WordDecoder)
)

// isValidMetadataKey checks if a metadata key can be sent as an HTTP header name
// Valid characters match S3 behavior for HTTP headers:
// - Letters: 'A'-'Z', 'a'-'z'
// - Digits: '0'-'9'
// - Special characters: '-', '_', '.', '#'
func isValidMetadataKey(key string) bool {
	// Empty keys are invalid
	if len(key) == 0 {
		return false
	}

	for _, r := range key {
		// Only allow ASCII characters
		if r > unicode.MaxASCII {
			return false
		}

		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}

		// Allow specific special characters that S3 accepts
		if r == '-' || r == '_' || r == '.' || r == '#' {
			continue
		}

		// Reject all other characters
		return false
	}

	return true
}

// amzMetaAsMetadata prepare metadata based on amazon user metadata request headers
func amzMetaAsMetadata(req *http.Request) (catalog.Metadata, error) {
	metadata := make(catalog.Metadata)
	var err error

	var userMetadataSize int
	for k := range req.Header {
		name, found := strings.CutPrefix(k, amzMetaHeaderPrefix)
		if found {
			value, decodeErr := rfc2047Decoder.DecodeHeader(req.Header.Get(k))
			if decodeErr != nil {
				err = errors.Join(err, decodeErr)
				continue
			}
			userMetadataSize += len(name) + len(value)
			if userMetadataSize > maxUserMetadataSize {
				return nil, gatewayerrors.ErrMetadataTooLarge
			}

			// Extract the metadata key part after the prefix and lowercase it
			// to comply with S3 spec: "Amazon S3 stores user-defined metadata keys in lowercase"
			keyPart := strings.TrimPrefix(k, amzMetaHeaderPrefix)
			keyPart = strings.ToLower(keyPart)
			// Validate the key part - only store valid metadata keys
			if isValidMetadataKey(keyPart) {
				metadata[amzMetaHeaderPrefix+keyPart] = value
			}
		}
	}
	return metadata, err
}

// amzMetaWriteHeaders set amazon user metadata on http response
func amzMetaWriteHeaders(w http.ResponseWriter, metadata catalog.Metadata) {
	h := w.Header()
	missingCount := 0

	for k, v := range metadata {
		if keyPart, ok := strings.CutPrefix(k, amzMetaHeaderPrefix); ok {
			if isValidMetadataKey(keyPart) {
				h.Set(k, mime.QEncoding.Encode("utf-8", v))
			} else {
				missingCount++
			}
		}
	}

	// Set the missing meta header if any keys were skipped
	if missingCount > 0 {
		h.Set(amzMissingMetaHeader, strconv.Itoa(missingCount))
	}
}

const amzMetadataDirectiveHeaderPrefix = "X-Amz-Metadata-Directive"

// Per S3 API, if the header X-Amz-Metadata-Directive is set to 'REPLACE', the metadata should be replaced.
// Otherwise, if the value is 'COPY' or the value is missing, it should be copied.
func shouldReplaceMetadata(req *http.Request) bool {
	return req.Header.Get(amzMetadataDirectiveHeaderPrefix) == "REPLACE"
}

func (o *PathOperation) finishUpload(req *http.Request, mTime *time.Time, checksum, physicalAddress string, size int64, relative bool, metadata map[string]string, contentType string, opts ...graveler.SetOptionsFunc) error {
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

	err := o.Catalog.CreateEntry(req.Context(), o.Repository.Name, o.Reference, entry, opts...)
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
