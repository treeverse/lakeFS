// Package helpers provide useful wrappers for clients using the lakeFS OpenAPI.
package helpers

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/httputil"
	"golang.org/x/sync/errgroup"
)

// MaxUploadParts is the maximum allowed number of parts in a multi-part upload
const MaxUploadParts int32 = 10000

// MinUploadPartSize is the minimum allowed part size when uploading a part
const MinUploadPartSize int64 = 1024 * 1024 * 5

// DefaultUploadPartSize is the default part size to buffer chunks of a payload into
const DefaultUploadPartSize = MinUploadPartSize

// DefaultUploadConcurrency is the default number of goroutines to spin up when uploading a multipart upload
const DefaultUploadConcurrency = 5

// ClientUpload uploads contents as a file via lakeFS
func ClientUpload(ctx context.Context, client apigen.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker) (*apigen.ObjectStats, error) {
	pr, pw := io.Pipe()
	defer func() {
		_ = pr.Close()
	}()

	mpw := multipart.NewWriter(pw)
	mpContentType := mpw.FormDataContentType()
	go func() {
		defer func() {
			_ = mpw.Close()
			_ = pw.Close()
		}()

		filename := filepath.Base(objPath)
		const fieldName = "content"
		var err error
		var cw io.Writer
		// when no content-type is specified, we let 'CreateFromFile' add the part with the default content type.
		// otherwise, we add a part and set the content-type.
		if contentType != "" {
			h := make(textproto.MIMEHeader)
			contentDisposition := mime.FormatMediaType("form-data", map[string]string{"name": fieldName, "filename": filename})
			h.Set("Content-Disposition", contentDisposition)
			h.Set("Content-Type", contentType)
			cw, err = mpw.CreatePart(h)
		} else {
			cw, err = mpw.CreateFormFile(fieldName, filename)
		}
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(cw, contents); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
	}()

	resp, err := client.UploadObjectWithBodyWithResponse(ctx, repoID, branchID, &apigen.UploadObjectParams{
		Path: objPath,
	}, mpContentType, pr, func(ctx context.Context, req *http.Request) error {
		var metaKey string
		for k, v := range metadata {
			lowerKey := strings.ToLower(k)
			if strings.HasPrefix(lowerKey, apiutil.LakeFSMetadataPrefix) {
				metaKey = apiutil.LakeFSHeaderInternalPrefix + lowerKey[len(apiutil.LakeFSMetadataPrefix):]
			} else {
				metaKey = apiutil.LakeFSHeaderMetadataPrefix + lowerKey
			}
			req.Header.Set(metaKey, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if resp.JSON201 == nil {
		return nil, ResponseAsError(resp)
	}
	return resp.JSON201, nil
}

type PreSignUploader struct {
	RepoID      string
	BranchID    string
	ObjectPath  string
	Metadata    map[string]string
	ContentType string
	Contents    io.ReaderAt
	Size        int64
	PartSize    int64
	NumParts    int
	Concurrency int
	HTTPClient  *http.Client
	Client      apigen.ClientWithResponsesInterface
}

type partReader struct {
	Reader *io.SectionReader
	URL    string
}

func canUseMultipartUpload(contents io.ReadSeeker, contentLength int64) bool {
	_, ok := contents.(io.ReaderAt)
	return ok && contentLength > MinUploadPartSize
}

func (u *PreSignUploader) Upload(ctx context.Context) (*apigen.ObjectStats, error) {
	mpu, err := u.init(ctx)
	if err != nil {
		return nil, err
	}

	// prepare
	parts := make([]partReader, u.NumParts)
	rem := u.Size
	off := int64(0)
	for i := range parts {
		size := min(u.PartSize, rem)
		parts[i].Reader = io.NewSectionReader(u.Contents, off, size)
		parts[i].URL = (*mpu.PresignedUrls)[i]
		rem -= size
		off += size
	}

	// upload parts in parallel, fill uploadParts array with results
	uploadParts := make([]apigen.UploadPart, u.NumParts)
	g, grpCtx := errgroup.WithContext(context.Background())
	g.SetLimit(u.Concurrency)

	for i := 0; i < u.NumParts; i++ {
		i := i // pin i
		g.Go(func() error {
			etag, err := u.uploadPart(grpCtx, parts[i].Reader, parts[i].URL, u.ContentType)
			if err != nil {
				return fmt.Errorf("part %d %w", i+1, err)
			}
			uploadParts[i] = apigen.UploadPart{
				PartNumber: i + 1,
				Etag:       etag,
			}
			return nil
		})
	}

	// wait for all parts to be uploaded
	if err := g.Wait(); err != nil {
		// abort upload using a new context to avoid context cancellation
		abortErr := u.abort(context.Background(), mpu)
		if abortErr != nil {
			err = errors.Join(err, abortErr)
		}
		return nil, err
	}

	return u.complete(ctx, mpu, uploadParts)
}

func (u *PreSignUploader) complete(ctx context.Context, mpu *apigen.PresignMultipartUpload, uploadParts []apigen.UploadPart) (*apigen.ObjectStats, error) {
	completeResult, err := u.Client.CompletePresignMultipartUploadWithResponse(ctx, u.RepoID, u.BranchID, mpu.UploadId,
		&apigen.CompletePresignMultipartUploadParams{
			Path: u.ObjectPath,
		},
		apigen.CompletePresignMultipartUploadJSONRequestBody{
			Parts:           uploadParts,
			PhysicalAddress: mpu.PhysicalAddress,
			UserMetadata: &apigen.CompletePresignMultipartUpload_UserMetadata{
				AdditionalProperties: u.Metadata,
			},
			ContentType: apiutil.Ptr(u.ContentType),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("complete presign multipart upload: %w", err)
	}
	if completeResult.JSON200 == nil {
		return nil, fmt.Errorf("complete presign multipart upload: %w", ResponseAsError(completeResult))
	}
	return completeResult.JSON200, nil
}

func (u *PreSignUploader) abort(ctx context.Context, mpu *apigen.PresignMultipartUpload) error {
	resp, err := u.Client.AbortPresignMultipartUploadWithResponse(ctx, u.RepoID, u.BranchID, mpu.UploadId,
		&apigen.AbortPresignMultipartUploadParams{
			Path: u.ObjectPath,
		},
		apigen.AbortPresignMultipartUploadJSONRequestBody{
			PhysicalAddress: mpu.PhysicalAddress,
		})
	if err != nil {
		return fmt.Errorf("abort presign multipart upload: %w", err)
	}
	if resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("abort presign multipart upload: %w", ResponseAsError(resp))
	}
	return nil
}

func (u *PreSignUploader) init(ctx context.Context) (*apigen.PresignMultipartUpload, error) {
	// create presign multipart upload
	resp, err := u.Client.CreatePresignMultipartUploadWithResponse(ctx, u.RepoID, u.BranchID, &apigen.CreatePresignMultipartUploadParams{
		Path:  u.ObjectPath,
		Parts: swag.Int(u.NumParts),
	})
	if err != nil {
		return nil, fmt.Errorf("create presign multipart upload: %w", err)
	}
	if resp.JSON201 == nil {
		return nil, fmt.Errorf("create presign multipart upload: %w", ResponseAsError(resp))
	}

	// verify we got the expected number of presigned urls
	mpu := resp.JSON201
	var presignedUrls []string
	if mpu.PresignedUrls != nil {
		presignedUrls = *mpu.PresignedUrls
	}
	if len(presignedUrls) != u.NumParts {
		return nil, fmt.Errorf("create presign multipart upload: %w, expected %d presigned urls, got %d", ErrRequestFailed, u.NumParts, len(presignedUrls))
	}
	return mpu, nil
}

func (u *PreSignUploader) uploadPart(ctx context.Context, partReader *io.SectionReader, partURL string, contentType string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, partURL, partReader)
	if err != nil {
		return "", err
	}
	req.ContentLength = partReader.Size()
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := u.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if !httputil.IsSuccessStatusCode(resp) {
		return "", fmt.Errorf("upload %w %s: %s", ErrRequestFailed, partURL, resp.Status)
	}

	etag := extractEtagFromResponseHeader(resp.Header)
	if etag == "" {
		return "", fmt.Errorf("upload %w %s: etag is missing", ErrRequestFailed, partURL)
	}
	return etag, nil
}

func ClientUploadPreSign(ctx context.Context, client apigen.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker, presignMultipartSupport bool) (*apigen.ObjectStats, error) {
	// calculate size using seek
	contentLength, err := contents.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	var uploader *PreSignUploader
	if presignMultipartSupport && canUseMultipartUpload(contents, contentLength) {
		uploader = NewPreSignUploader(client, repoID, branchID, objPath, contents.(io.ReaderAt), contentLength, contentType, metadata)
	}

	// upload loop, retry on conflict
	var stats *apigen.ObjectStats
	for {
		if uploader != nil {
			stats, err = uploader.Upload(ctx)
		} else {
			stats, err = clientUploadPreSignHelper(ctx, client, repoID, branchID, objPath, metadata, contentType, contents, contentLength)
		}
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return stats, nil
	}
}

func NewPreSignUploader(client apigen.ClientWithResponsesInterface, repoID string, branchID string, objPath string, contents io.ReaderAt, size int64, contentType string, metadata map[string]string) *PreSignUploader {
	partSize := DefaultUploadPartSize

	// adjust part size
	if size/partSize >= int64(MaxUploadParts) {
		// Add one to the part size to account for remainders
		partSize = (size / int64(MaxUploadParts)) + 1
	}

	// calculate the number of parts
	numParts := int(size / partSize)
	if size%partSize != 0 {
		numParts++
	}

	return &PreSignUploader{
		RepoID:      repoID,
		BranchID:    branchID,
		ObjectPath:  objPath,
		Metadata:    metadata,
		ContentType: contentType,
		Contents:    contents,
		Size:        size,
		PartSize:    partSize,
		NumParts:    numParts,
		Concurrency: DefaultUploadConcurrency,
		HTTPClient:  http.DefaultClient,
		Client:      client,
	}
}

func isAzureBlobURL(u string) bool {
	parsedURL, err := url.Parse(u)
	if err != nil {
		return false
	}
	return strings.HasSuffix(parsedURL.Host, ".blob.core.windows.net")
}

// clientUploadPreSignHelper helper func to get physical address an upload content. Special case if conflict that
// ErrConflict is returned where a re-try is required.
func clientUploadPreSignHelper(ctx context.Context, client apigen.ClientWithResponsesInterface, repoID string, branchID string, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker, contentLength int64) (*apigen.ObjectStats, error) {
	stagingLocation, err := getPhysicalAddress(ctx, client, repoID, branchID, &apigen.GetPhysicalAddressParams{
		Path:    objPath,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	preSignURL := swag.StringValue(stagingLocation.PresignedUrl)
	if _, err := contents.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var body io.ReadSeeker
	// calculate size using seek
	if contentLength > 0 { // Passing Reader with content length == 0 results in 501 Not Implemented
		body = contents
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, preSignURL, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = contentLength
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if isAzureBlobURL(preSignURL) {
		req.Header.Set("x-ms-blob-type", "BlockBlob")
	}

	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = putResp.Body.Close() }()
	if !httputil.IsSuccessStatusCode(putResp) {
		return nil, fmt.Errorf("upload %w %s: %s", ErrRequestFailed, preSignURL, putResp.Status)
	}

	etag := extractEtagFromResponseHeader(putResp.Header)
	if etag == "" {
		return nil, fmt.Errorf("upload %w %s: etag is missing", ErrRequestFailed, preSignURL)
	}

	linkReqBody := apigen.LinkPhysicalAddressJSONRequestBody{
		Checksum:  etag,
		SizeBytes: contentLength,
		Staging:   *stagingLocation,
		UserMetadata: &apigen.StagingMetadata_UserMetadata{
			AdditionalProperties: metadata,
		},
		ContentType: apiutil.Ptr(contentType),
	}
	linkResp, err := client.LinkPhysicalAddressWithResponse(ctx, repoID, branchID,
		&apigen.LinkPhysicalAddressParams{
			Path: objPath,
		}, linkReqBody)
	if err != nil {
		return nil, fmt.Errorf("link object to backing store: %w", err)
	}
	if linkResp.JSON200 != nil {
		return linkResp.JSON200, nil
	}
	if linkResp.JSON409 != nil {
		return nil, ErrConflict
	}
	return nil, fmt.Errorf("link object to backing store: %w (%s)", ErrRequestFailed, linkResp.Status())
}

// extractEtagFromResponseHeader extracts the ETag from the response header.
// If the response contains a Content-MD5 header, it will be decoded from base64 and returned as hex.
func extractEtagFromResponseHeader(h http.Header) string {
	// prefer Content-MD5 if exists
	contentMD5 := h.Get("Content-MD5")
	if contentMD5 != "" {
		// decode base64, return as hex
		decodeMD5, err := base64.StdEncoding.DecodeString(contentMD5)
		if err == nil {
			return hex.EncodeToString(decodeMD5)
		}
	}
	// fallback to ETag
	etag := h.Get("ETag")
	etag = strings.TrimFunc(etag, func(r rune) bool { return r == '"' || r == ' ' })
	return etag
}

func getPhysicalAddress(ctx context.Context, client apigen.ClientWithResponsesInterface, repoID string, branchID string, params *apigen.GetPhysicalAddressParams) (*apigen.StagingLocation, error) {
	resp, err := client.GetPhysicalAddressWithResponse(ctx, repoID, branchID, params)
	if err != nil {
		return nil, fmt.Errorf("get physical address to upload object: %w", err)
	}
	if resp.JSONDefault != nil {
		return nil, fmt.Errorf("%w: %s", ErrRequestFailed, resp.JSONDefault.Message)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("%w: %s (status code %d)", ErrRequestFailed, resp.Status(), resp.StatusCode())
	}
	return resp.JSON200, nil
}
