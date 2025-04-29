/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package sig This file implements helper functions to validate Streaming AWS
// Signature Version '4' authorization header.
package sig

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"strings"
	"time"

	"github.com/minio/crc64nvme"
	"github.com/treeverse/lakefs/pkg/auth/model"
	gwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
)

// Streaming AWS Signature Version '4' constants.
const (
	// These constants are part of the AWS SigV4 spec, so they are safe.
	emptySHA256            = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" //nolint:gosec
	signV4ChunkedAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD"                                         //nolint:gosec
	SlashSeparator         = "/"
)

// ChecksumAlgorithm represents the type of checksum algorithm used for trailers
type ChecksumAlgorithm string

// Supported checksum algorithms
const (
	ChecksumAlgorithmCRC32     ChecksumAlgorithm = "x-amz-checksum-crc32"
	ChecksumAlgorithmCRC32C    ChecksumAlgorithm = "x-amz-checksum-crc32c"
	ChecksumAlgorithmCRC64NVME ChecksumAlgorithm = "x-amz-checksum-crc64nvme"
	ChecksumAlgorithmSHA1      ChecksumAlgorithm = "x-amz-checksum-sha1"
	ChecksumAlgorithmSHA256    ChecksumAlgorithm = "x-amz-checksum-sha256"
	ChecksumAlgorithmInvalid   ChecksumAlgorithm = ""
)

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region, service string) string {
	scope := strings.Join([]string{
		t.Format(v4shortTimeFormat),
		region,
		service,
		v4scopeTerminator,
	}, SlashSeparator)
	return scope
}

// getChunkSignature - get chunk signature.
func getChunkSignature(cred *model.Credential, seedSignature string, region string, service string, date time.Time, hashedChunk string) string {
	// Calculate string to sign.
	stringToSign := signV4ChunkedAlgorithm + "\n" +
		date.Format(v4timeFormat) + "\n" +
		getScope(date, region, service) + "\n" +
		seedSignature + "\n" +
		emptySHA256 + "\n" +
		hashedChunk

	// Get hmac signing key.
	signingKey := createSignature(cred.SecretAccessKey, date.Format(v4shortTimeFormat), region, service)

	// Calculate signature.
	newSignature := hex.EncodeToString(sign(signingKey, stringToSign))

	return newSignature
}

const maxLineLength = 4 * 1024

var (
	// lineTooLong is generated as chunk header is bigger than 4KiB.
	errLineTooLong = errors.New("header line too long")

	// Malformed encoding is generated when a chunk header is wrongly formed.
	errMalformedEncoding = errors.New("malformed chunked encoding")

	ErrInvalidByte           = errors.New("invalid byte in chunk length")
	ErrChunkTooLarge         = errors.New("http chunk length too large")
	ErrChecksumHeaderMissing = errors.New("trailer header is missing")
	ErrUnsupportedChecksum   = errors.New("unsupported checksum algorithm")
	ErrChecksumMismatch      = errors.New("checksum mismatch")
	ErrChecksumTypeMismatch  = errors.New("checksum type mismatch")
)

// newSignV4ChunkedReader returns a new s3ChunkedReader that translates the data read from r
// out of HTTP "chunked" format before returning it.
// The s3ChunkedReader returns io.EOF when the final 0-length chunk is read.
//
// NewChunkedReader is not needed by normal applications. The http package
// automatically decodes chunking when reading response bodies.
func newSignV4ChunkedReader(reader *bufio.Reader, amzDate string, auth V4Auth, signatureMethod string, creds *model.Credential) (io.ReadCloser, error) {
	seedDate, err := time.Parse(v4timeFormat, amzDate)
	if err != nil {
		return nil, err
	}

	var checksumWriter hash.Hash
	if signatureMethod == v4UnsignedPayloadTrailer {
		if auth.ChecksumAlgorithm == "" {
			return nil, ErrChecksumHeaderMissing
		}
		checksumWriter, err = GetChecksumWriter(auth.ChecksumAlgorithm)
		if err != nil {
			return nil, err
		}
	}

	return &s3ChunkedReader{
		reader:            reader,
		cred:              creds,
		seedSignature:     auth.Signature,
		seedDate:          seedDate,
		region:            auth.Region,
		service:           auth.Service,
		chunkSHA256Writer: sha256.New(),
		state:             readChunkHeader,
		trailers:          make(map[string]string),
		checksumAlgorithm: auth.ChecksumAlgorithm,
		checksumWriter:    checksumWriter,
	}, nil
}

// Represents the overall state that is required for decoding an
// AWS Signature V4 chunked reader.
type s3ChunkedReader struct {
	reader            *bufio.Reader
	cred              *model.Credential
	seedSignature     string
	seedDate          time.Time
	region            string
	service           string
	state             chunkState
	lastChunk         bool
	chunkSignature    string
	chunkSHA256Writer hash.Hash // Calculates sha256 of chunk data.
	n                 uint64    // Unread bytes in chunk
	err               error
	trailers          map[string]string
	checksumAlgorithm string
	checksumWriter    hash.Hash
}

// Read chunk reads the chunk token signature portion.
func (cr *s3ChunkedReader) readS3ChunkHeader() {
	// Read the first chunk line until CRLF.
	var hexChunkSize, hexChunkSignature []byte
	hexChunkSize, hexChunkSignature, cr.err = readChunkLine(cr.reader)
	if cr.err != nil {
		return
	}
	// <hex>;token=value - converts the hex into its uint64 form.
	cr.n, cr.err = parseHexUint(hexChunkSize)
	if cr.err != nil {
		return
	}
	if cr.n == 0 {
		cr.err = io.EOF
	}
	// Save the incoming chunk signature.
	cr.chunkSignature = string(hexChunkSignature)
}

type chunkState int

const (
	readChunkHeader chunkState = iota
	readChunkTerm
	readChunk
	verifyChunk
	readTrailerChunk
	eofChunk
)

func (cs chunkState) String() string {
	stateString := ""
	switch cs {
	case readChunkHeader:
		stateString = "readChunkHeader"
	case readChunkTerm:
		stateString = "readChunkTrailer"
	case readChunk:
		stateString = "readChunk"
	case verifyChunk:
		stateString = "verifyChunk"
	case readTrailerChunk:
		stateString = "readTrailerChunk"
	case eofChunk:
		stateString = "eofChunk"
	}
	return stateString
}

func (cr *s3ChunkedReader) Close() (err error) {
	return nil
}

// Read - implements `io.Reader`, which transparently decodes
// the incoming AWS Signature V4 streaming signature.
func (cr *s3ChunkedReader) Read(buf []byte) (n int, err error) {
	for {
		switch cr.state {
		case readChunkHeader:
			cr.readS3ChunkHeader()
			// If we're at the end of a chunk.
			if cr.n == 0 && cr.err == io.EOF {
				cr.state = readChunkTerm
				cr.lastChunk = true
				continue
			}
			if cr.err != nil {
				return 0, cr.err
			}
			cr.state = readChunk
		case readChunkTerm:
			err = peekCRLF(cr.reader)
			isTrailingChunk := cr.n == 0 && cr.lastChunk

			if !isTrailingChunk {
				if readErr := readCRLF(cr.reader); readErr != nil {
					cr.err = readErr
					return 0, cr.err
				}
				cr.err = err
			} else if err != nil && !errors.Is(err, errMalformedEncoding) {
				cr.err = err
			} else if err == nil {
				// Some clients send an additional CRLF after the chunk trailer.
				// This is not part of the spec, but we handle it gracefully.
				if readErr := readCRLF(cr.reader); readErr != nil {
					cr.err = readErr
					return 0, cr.err
				}
			}

			// If we're using unsigned streaming upload, there is no signature to verify at each chunk.
			if cr.chunkSignature != "" {
				cr.state = verifyChunk
			} else if cr.lastChunk {
				cr.state = readTrailerChunk
			} else {
				cr.state = readChunkHeader
			}
		case readTrailerChunk:
			checksumAlgorithm, checksumValue := getTrailerChecksum(cr.reader)

			if checksumAlgorithm != cr.checksumAlgorithm {
				cr.err = ErrChecksumTypeMismatch
				return 0, cr.err
			}

			computedChecksum := cr.checksumWriter.Sum(nil)
			b64checksum := base64.StdEncoding.EncodeToString(computedChecksum)
			if b64checksum != checksumValue {
				cr.err = ErrChecksumMismatch
				return 0, cr.err
			}

			// Read the final CRLF sequence
			if err := readCRLF(cr.reader); err != nil {
				cr.err = err
				return n, err
			}

			cr.state = eofChunk
		case readChunk:
			// There is no more space left in the request buffer.
			if len(buf) == 0 {
				return n, nil
			}
			rbuf := buf
			// The request buffer is larger than the current chunk size.
			// Read only the current chunk from the underlying reader.
			if uint64(len(rbuf)) > cr.n {
				rbuf = rbuf[:cr.n]
			}
			var n0 int
			n0, cr.err = cr.reader.Read(rbuf)
			if cr.err != nil {
				// We have lesser than chunk size advertised in chunkHeader, this is 'unexpected'.
				if cr.err == io.EOF {
					cr.err = io.ErrUnexpectedEOF
				}
				return 0, cr.err
			}

			// Calculate sha256.
			if _, err := cr.chunkSHA256Writer.Write(rbuf[:n0]); err != nil {
				return 0, err
			}

			// Compute checksum
			if cr.checksumWriter != nil {
				cr.checksumWriter.Write(rbuf[:n0])
			}

			// Update the bytes read into request buffer so far.
			n += n0
			buf = buf[n0:]
			// Update bytes to be read of the current chunk before verifying chunk's signature.
			// n >= n0 >= 0, cast is safe
			cr.n -= uint64(n0) //nolint:gosec

			// If we're at the end of a chunk.
			if cr.n == 0 {
				cr.state = readChunkTerm
				continue
			}
		case verifyChunk:
			// Calculate the hashed chunk.
			hashedChunk := hex.EncodeToString(cr.chunkSHA256Writer.Sum(nil))
			// Calculate the chunk signature.
			newSignature := getChunkSignature(cr.cred, cr.seedSignature, cr.region, cr.service, cr.seedDate, hashedChunk)
			if !Equal([]byte(cr.chunkSignature), []byte(newSignature)) {
				return 0, gwerrors.ErrSignatureDoesNotMatch
			}
			// Newly calculated signature becomes the seed for the next chunk
			// this follows the chaining.
			cr.seedSignature = newSignature
			cr.chunkSHA256Writer.Reset()
			if cr.lastChunk {
				cr.state = eofChunk
			} else {
				cr.state = readChunkHeader
			}
		case eofChunk:
			return n, io.EOF
		}
	}
}

// readCRLF - check if reader only has '\r\n' CRLF character.
// returns malformed encoding if it doesn't.
func readCRLF(reader io.Reader) error {
	buf := make([]byte, 2)                 //nolint: mnd
	_, err := io.ReadFull(reader, buf[:2]) //nolint: mnd
	if err != nil {
		return err
	}
	return checkCRLF(buf)
}

// peekCRLF - peeks at the next two bytes to check for CRLF without consuming them.
func peekCRLF(reader *bufio.Reader) error {
	peeked, err := reader.Peek(2) //nolint:mnd
	if err != nil {
		return err
	}
	return checkCRLF(peeked)
}

// checkCRLF - checks if the buffer contains '\r\n' CRLF character.
func checkCRLF(buf []byte) error {
	if len(buf) != 2 || buf[0] != '\r' || buf[1] != '\n' {
		return errMalformedEncoding
	}
	return nil
}

// Read a line of bytes (up to \n) from b.
// Give up if the line exceeds maxLineLength.
// The returned bytes are owned by the bufio.Reader
// so they are only valid until the next bufio read.
func readChunkLine(b *bufio.Reader) ([]byte, []byte, error) {
	buf, err := b.ReadSlice('\n')
	if err != nil {
		// We always know when EOF is coming.
		// If the caller asked for a line, there should be a line.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		} else if errors.Is(err, bufio.ErrBufferFull) {
			err = errLineTooLong
		}
		return nil, nil, err
	}
	if len(buf) >= maxLineLength {
		return nil, nil, errLineTooLong
	}
	// Parse s3 specific chunk extension and fetch the values.
	hexChunkSize, hexChunkSignature := parseS3ChunkExtension(buf)
	return hexChunkSize, hexChunkSignature, nil
}

// trimTrailingWhitespace - trim trailing white space.
func trimTrailingWhitespace(b []byte) []byte {
	for len(b) > 0 && isASCIISpace(b[len(b)-1]) {
		b = b[:len(b)-1]
	}
	return b
}

// isASCIISpace - is ascii space?
func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// Constant s3 chunk encoding signature.
const s3ChunkSignatureStr = ";chunk-signature="

// parses3ChunkExtension removes any s3 specific chunk-extension from buf.
// For example,
//
//	"10000;chunk-signature=..." => "10000", "chunk-signature=..."
func parseS3ChunkExtension(buf []byte) ([]byte, []byte) {
	buf = trimTrailingWhitespace(buf)
	semi := bytes.Index(buf, []byte(s3ChunkSignatureStr))
	// Chunk signature not found, return the whole buffer.
	if semi == -1 {
		return buf, nil
	}
	return buf[:semi], buf[semi+len(s3ChunkSignatureStr):]
}

// parse hex to uint64.
func parseHexUint(v []byte) (n uint64, err error) {
	const maxChunkLength = 16
	const letterOffset = 10
	for i, b := range v {
		switch {
		case '0' <= b && b <= '9':
			b -= '0'
		case 'a' <= b && b <= 'f':
			b -= 'a' - letterOffset
		case 'A' <= b && b <= 'F':
			b -= 'A' - letterOffset
		default:
			return 0, ErrInvalidByte
		}
		if i == maxChunkLength {
			return 0, ErrChunkTooLarge
		}
		n <<= 4
		n |= uint64(b)
	}
	return
}

func getTrailerChecksum(reader *bufio.Reader) (string, string) {
	// When using unsigned upload, this would be the raw contents of  the trailer chunk:
	//
	// x-amz-checksum-crc32:YABb/g==\n\r\n\r\n      // Trailer chunk (note optional \n character)
	// \r\n                                         // CRLF
	//
	// When using signed upload with an additional checksum algorithm, this would be the raw contents of the trailer chunk:
	//
	// x-amz-checksum-crc32:YABb/g==\n\r\n            // Trailer chunk (note optional \n character)
	// trailer-signature\r\n
	// \r\n                                           // CRLF
	//
	bytesRead, err := reader.ReadSlice('\n')
	if err != nil {
		return "", ""
	}

	parts := bytes.SplitN(bytesRead, []byte(":"), 2) //nolint:mnd
	if len(parts) != 2 {                             //nolint:mnd
		return "", ""
	}

	checksumAlgorithm := string(parts[0])
	checksumValue := string(trimTrailingWhitespace(parts[1]))

	return checksumAlgorithm, checksumValue
}

// GetChecksumWriter returns the appropriate hash.Hash implementation for the given checksum algorithm
func GetChecksumWriter(name string) (hash.Hash, error) {
	switch ChecksumAlgorithm(name) {
	case ChecksumAlgorithmCRC32C:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), nil
	case ChecksumAlgorithmCRC32:
		return crc32.NewIEEE(), nil
	case ChecksumAlgorithmCRC64NVME:
		return crc64nvme.New(), nil
	case ChecksumAlgorithmSHA256:
		return sha256.New(), nil
	default:
		return nil, ErrUnsupportedChecksum
	}
}
