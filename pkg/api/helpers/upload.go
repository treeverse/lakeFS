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
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/httputil"
	"golang.org/x/sync/errgroup"
)

// MaxUploadParts is the maximum allowed number of parts in a multipart upload
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

// PreSignUploader uploads contents as a file via lakeFS using presigned urls
// It supports both multipart and single part uploads.
type PreSignUploader struct {
	Concurrency      int
	HTTPClient       *http.Client
	Client           apigen.ClientWithResponsesInterface
	MultipartSupport bool
}

// presignUpload represents a single upload request
type presignUpload struct {
	uploader    PreSignUploader
	repoID      string
	branchID    string
	objectPath  string
	metadata    map[string]string
	contentType string
	reader      io.ReadSeeker
	readerAt    io.ReaderAt
	size        int64
	partSize    int64
	numParts    int
}

func NewPreSignUploader(client apigen.ClientWithResponsesInterface, httpClient *http.Client, multipartSupport bool) *PreSignUploader {
	return &PreSignUploader{
		Concurrency:      DefaultUploadConcurrency,
		HTTPClient:       httpClient,
		Client:           client,
		MultipartSupport: multipartSupport,
	}
}

func (u *PreSignUploader) Upload(ctx context.Context, repoID string, branchID string, objPath string, content io.ReadSeeker,
	contentType string, metadata map[string]string,
) (*apigen.ObjectStats, error) {
	// calculate size and rewind content
	size, err := content.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if _, seekErr := content.Seek(0, io.SeekStart); seekErr != nil {
		return nil, seekErr
	}
	// check if content implements io.ReaderAt for multipart upload
	readerAt, _ := content.(io.ReaderAt)

	// create upload object - represents a single upload request
	upload := &presignUpload{
		uploader:    *u,
		repoID:      repoID,
		branchID:    branchID,
		objectPath:  objPath,
		metadata:    metadata,
		contentType: contentType,
		reader:      content,
		readerAt:    readerAt,
		size:        size,
	}
	return upload.Upload(ctx)
}

type presignPartReader struct {
	Reader *io.SectionReader
	URL    string
}

func (u *presignUpload) uploadMultipart(ctx context.Context) (*apigen.ObjectStats, error) {
	mpu, err := u.initMultipart(ctx)
	if err != nil {
		return nil, err
	}

	// prepare readers
	parts := make([]presignPartReader, u.numParts)
	rem := u.size
	off := int64(0)
	for i := range parts {
		size := min(u.partSize, rem)
		parts[i].Reader = io.NewSectionReader(u.readerAt, off, size) // use `readerAt`
		parts[i].URL = (*mpu.PresignedUrls)[i]
		rem -= size
		off += size
	}

	// upload parts in parallel, fill uploadParts array with results
	uploadParts := make([]apigen.UploadPart, u.numParts)
	g, grpCtx := errgroup.WithContext(context.Background())
	g.SetLimit(u.uploader.Concurrency)

	for i := 0; i < u.numParts; i++ {
		i := i // pinning
		g.Go(func() error {
			etag, err := u.uploadPart(grpCtx, parts[i].Reader, parts[i].URL)
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
		abortErr := u.abortMultipart(context.Background(), mpu)
		if abortErr != nil {
			err = errors.Join(err, abortErr)
		}
		return nil, err
	}

	return u.completeMultipart(ctx, mpu, uploadParts)
}

func (u *presignUpload) completeMultipart(ctx context.Context, mpu *apigen.PresignMultipartUpload, uploadParts []apigen.UploadPart) (*apigen.ObjectStats, error) {
	resp, err := u.uploader.Client.CompletePresignMultipartUploadWithResponse(ctx, u.repoID, u.branchID, mpu.UploadId,
		&apigen.CompletePresignMultipartUploadParams{
			Path: u.objectPath,
		},
		apigen.CompletePresignMultipartUploadJSONRequestBody{
			Parts:           uploadParts,
			PhysicalAddress: mpu.PhysicalAddress,
			UserMetadata: &apigen.CompletePresignMultipartUpload_UserMetadata{
				AdditionalProperties: u.metadata,
			},
			ContentType: apiutil.Ptr(u.contentType),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("complete presign multipart upload: %w", err)
	}
	if resp.JSON409 != nil {
		return nil, ErrConflict
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("complete presign multipart upload: %w", ResponseAsError(resp))
	}
	return resp.JSON200, nil
}

func (u *presignUpload) abortMultipart(ctx context.Context, mpu *apigen.PresignMultipartUpload) error {
	resp, err := u.uploader.Client.AbortPresignMultipartUploadWithResponse(ctx, u.repoID, u.branchID, mpu.UploadId,
		&apigen.AbortPresignMultipartUploadParams{
			Path: u.objectPath,
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

func (u *presignUpload) initMultipart(ctx context.Context) (*apigen.PresignMultipartUpload, error) {
	// adjust part size
	u.partSize = DefaultUploadPartSize
	if u.size/u.partSize >= int64(MaxUploadParts) {
		// Add one to the part size to account for remainders
		u.partSize = (u.size / int64(MaxUploadParts)) + 1
	}

	// calculate the number of parts
	u.numParts = int(u.size / u.partSize)
	if u.size%u.partSize != 0 {
		u.numParts++
	}

	// create presign multipart upload
	resp, err := u.uploader.Client.CreatePresignMultipartUploadWithResponse(ctx, u.repoID, u.branchID, &apigen.CreatePresignMultipartUploadParams{
		Path:  u.objectPath,
		Parts: swag.Int(u.numParts),
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
	if len(presignedUrls) != u.numParts {
		return nil, fmt.Errorf("create presign multipart upload: %w, expected %d presigned urls, got %d", ErrRequestFailed, u.numParts, len(presignedUrls))
	}
	return mpu, nil
}

func (u *presignUpload) uploadPart(ctx context.Context, partReader *io.SectionReader, partURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, partURL, partReader)
	if err != nil {
		return "", err
	}
	req.ContentLength = partReader.Size()
	if u.contentType != "" {
		req.Header.Set("Content-Type", u.contentType)
	}

	resp, err := u.uploader.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if !httputil.IsSuccessStatusCode(resp) {
		return "", fmt.Errorf("upload %s part(%s): %w", partURL, resp.Status, ErrRequestFailed)
	}

	etag := extractEtagFromResponseHeader(resp.Header)
	if etag == "" {
		return "", fmt.Errorf("upload etag is missing %s: %w", partURL, ErrRequestFailed)
	}
	return etag, nil
}

func (u *presignUpload) uploadObject(ctx context.Context) (*apigen.ObjectStats, error) {
	stagingLocation, err := getPhysicalAddress(ctx, u.uploader.Client, u.repoID, u.branchID, &apigen.GetPhysicalAddressParams{
		Path:    u.objectPath,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	preSignURL := swag.StringValue(stagingLocation.PresignedUrl)

	var body io.ReadSeeker
	if u.size > 0 {
		// Passing Reader with content length == 0 results in 501 Not Implemented
		body = u.reader
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, preSignURL, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = u.size
	if u.contentType != "" {
		req.Header.Set("Content-Type", u.contentType)
	}
	if isAzureBlobURL(req.URL) {
		req.Header.Set("x-ms-blob-type", "BlockBlob")
	}

	putResp, err := u.uploader.HTTPClient.Do(req)
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
		SizeBytes: u.size,
		Staging:   *stagingLocation,
		UserMetadata: &apigen.StagingMetadata_UserMetadata{
			AdditionalProperties: u.metadata,
		},
		ContentType: apiutil.Ptr(u.contentType),
	}
	linkResp, err := u.uploader.Client.LinkPhysicalAddressWithResponse(ctx, u.repoID, u.branchID,
		&apigen.LinkPhysicalAddressParams{
			Path: u.objectPath,
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

func (u *presignUpload) Upload(ctx context.Context) (*apigen.ObjectStats, error) {
	// use multipart upload if:
	// 1. Multipart upload is supported by the server.
	// 2. Reader supports ReaderAt.
	// 3. Content size is greater than MinUploadPartSize.
	if u.uploader.MultipartSupport && u.size >= MinUploadPartSize && u.readerAt != nil {
		return u.uploadMultipart(ctx)
	}
	return u.uploadObject(ctx)
}

func ClientUploadPreSign(ctx context.Context, client apigen.ClientWithResponsesInterface, httpClient *http.Client, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker, presignMultipartSupport bool) (*apigen.ObjectStats, error) {
	// upload loop, retry on conflict
	uploader := NewPreSignUploader(client, httpClient, presignMultipartSupport)
	for {
		stats, err := uploader.Upload(ctx, repoID, branchID, objPath, contents, contentType, metadata)
		if err == nil {
			return stats, nil
		}
		// break in case of error other than conflict, otherwise retry
		if !errors.Is(err, ErrConflict) {
			return nil, err
		}
	}
}

func isAzureBlobURL(u *url.URL) bool {
	_, _, err := azure.ParseURL(u)
	return err == nil
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
