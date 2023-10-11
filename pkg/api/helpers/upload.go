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
)

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

func ClientUploadPreSign(ctx context.Context, client apigen.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker) (*apigen.ObjectStats, error) {
	// calculate size using seek
	contentLength, err := contents.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// upload loop, retry on conflict
	for {
		stats, err := clientUploadPreSignHelper(ctx, client, repoID, branchID, objPath, metadata, contentType, contents, contentLength)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return stats, nil
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
		return nil, fmt.Errorf("etag is missing: %w", ErrRequestFailed)
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
		objStat := *linkResp.JSON200
		// this is a workaround for the fact that the API does not return the full physical address
		objStat.PhysicalAddress = preSignURL
		return &objStat, nil
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
