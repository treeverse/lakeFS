package esti

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	mathrand "math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
)

func TestCreatePresignMultipartUpload(t *testing.T) {
	skipPresignMultipart(t)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	tests := []struct {
		name       string
		repo       string
		branch     string
		objName    string
		parts      *int
		statusCode int
	}{
		{name: "empty_obj_name", repo: repo, branch: mainBranch, objName: "", parts: swag.Int(3), statusCode: http.StatusBadRequest},
		{name: "empty_branch", repo: repo, branch: "", objName: "empty_branch", parts: swag.Int(4), statusCode: http.StatusBadRequest},
		{name: "unknown_branch", repo: repo, branch: "unknown", objName: "unknown_branch", parts: swag.Int(4), statusCode: http.StatusNotFound},
		{name: "empty_repo", repo: "", branch: mainBranch, objName: "empty_repo", parts: swag.Int(5), statusCode: http.StatusBadRequest},
		{name: "no_parts", repo: repo, branch: mainBranch, objName: "no_parts", parts: nil, statusCode: http.StatusCreated},
		{name: "negative_parts", repo: repo, branch: mainBranch, objName: "negative_parts", parts: swag.Int(-1), statusCode: http.StatusBadRequest},
		{name: "valid", repo: repo, branch: mainBranch, objName: "valid", parts: swag.Int(6), statusCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objName := tt.objName
			if objName != "" {
				objName = "presign_multipart_upload/" + objName
			}
			resp, err := client.CreatePresignMultipartUploadWithResponse(ctx, tt.repo, tt.branch, &apigen.CreatePresignMultipartUploadParams{
				Path:  objName,
				Parts: tt.parts,
			})
			require.NoError(t, err, "CreatePresignMultipartUpload should succeed")
			require.Equalf(t, tt.statusCode, resp.StatusCode(), "CreatePresignMultipartUpload status code mismatch: %s - %s",
				resp.Status(), resp.Body)
			if tt.statusCode != http.StatusCreated {
				return
			}
			require.NotNil(t, resp.JSON201)
			require.NotEmpty(t, resp.JSON201.UploadId)
			require.NotEmpty(t, resp.JSON201.PhysicalAddress)
			if tt.parts != nil {
				require.NotNil(t, resp.JSON201.PresignedUrls)
				require.Len(t, *resp.JSON201.PresignedUrls, *tt.parts)
				for _, url := range *resp.JSON201.PresignedUrls {
					require.NotEmpty(t, url)
				}
			} else {
				require.Nil(t, resp.JSON201.PresignedUrls)
			}
		})
	}
}

func TestAbortPresignMultipartUpload(t *testing.T) {
	skipPresignMultipart(t)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	tests := []struct {
		name            string
		repo            string
		branch          string
		objName         string
		uploadID        string
		physicalAddress string
		statusCode      int
	}{
		{name: "empty_path", repo: repo, branch: mainBranch, objName: "", uploadID: "upload_id", physicalAddress: "addr", statusCode: http.StatusBadRequest},
		{name: "empty_repo", repo: "", branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "addr", statusCode: http.StatusBadRequest},
		{name: "unknown_repo", repo: "unknown", branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "addr", statusCode: http.StatusNotFound},
		{name: "empty_physical_address", repo: "", branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "", statusCode: http.StatusBadRequest},
		{name: "empty_physical_address", repo: repo, branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "", statusCode: http.StatusBadRequest},
		{name: "empty_upload_id", repo: repo, branch: mainBranch, objName: "obj", uploadID: "", physicalAddress: "addr", statusCode: http.StatusInternalServerError}, // produces invalid endpoint
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.AbortPresignMultipartUploadWithResponse(ctx, tt.repo, tt.branch, tt.uploadID, &apigen.AbortPresignMultipartUploadParams{
				Path: tt.objName,
			}, apigen.AbortPresignMultipartUploadJSONRequestBody{
				PhysicalAddress: tt.physicalAddress,
			})
			require.NoError(t, err, "AbortPresignMultipartUpload should succeed")
			require.Equalf(t, tt.statusCode, resp.StatusCode(), "AbortPresignMultipartUpload status code mismatch (expected %d): %s - %s", tt.statusCode, resp.Status(), resp.Body)
		})
	}

	t.Run("valid", func(t *testing.T) {
		const objPath = "presign_multipart_upload/abort"
		respCreate, err := client.CreatePresignMultipartUploadWithResponse(ctx, repo, mainBranch, &apigen.CreatePresignMultipartUploadParams{
			Path:  objPath,
			Parts: swag.Int(2),
		})
		require.NoError(t, err)
		require.NotNil(t, respCreate.JSON201)

		resp, err := client.AbortPresignMultipartUploadWithResponse(ctx, repo, mainBranch, respCreate.JSON201.UploadId, &apigen.AbortPresignMultipartUploadParams{
			Path: objPath,
		}, apigen.AbortPresignMultipartUploadJSONRequestBody{
			PhysicalAddress: respCreate.JSON201.PhysicalAddress,
		})
		require.NoError(t, err, "AbortPresignMultipartUpload should succeed")
		require.Equalf(t, http.StatusNoContent, resp.StatusCode(), "AbortPresignMultipartUpload status code mismatch: %s - %s", resp.Status(), resp.Body)
	})
}

func TestCompletePresignMultipartUpload(t *testing.T) {
	skipPresignMultipart(t)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// fake parts used for the above tests
	var fakeParts []apigen.UploadPart
	for i := 0; i < 2; i++ {
		fakeParts = append(fakeParts, apigen.UploadPart{
			Etag:       "etag" + strconv.Itoa(i),
			PartNumber: i + 1,
		})
	}

	tests := []struct {
		name            string
		repo            string
		branch          string
		objName         string
		uploadID        string
		physicalAddress string
		parts           []apigen.UploadPart
		statusCode      int
	}{
		{name: "empty_path", repo: repo, branch: mainBranch, objName: "", uploadID: "upload_id", physicalAddress: "addr", parts: fakeParts, statusCode: http.StatusBadRequest},
		{name: "empty_repo", repo: "", branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "addr", parts: fakeParts, statusCode: http.StatusBadRequest},
		{name: "unknown_repo", repo: "unknown", branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "addr", parts: fakeParts, statusCode: http.StatusNotFound},
		{name: "empty_physical_address", repo: "", branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "", parts: fakeParts, statusCode: http.StatusBadRequest},
		{name: "empty_physical_address", repo: repo, branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "", parts: fakeParts, statusCode: http.StatusBadRequest},
		{name: "empty_upload_id", repo: repo, branch: mainBranch, objName: "obj", uploadID: "", physicalAddress: "addr", parts: fakeParts, statusCode: http.StatusInternalServerError}, // produces invalid endpoint
		{name: "no_parts", repo: repo, branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "addr", parts: nil, statusCode: http.StatusBadRequest},              // produces invalid endpoint
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.CompletePresignMultipartUploadWithResponse(ctx, tt.repo, tt.branch, tt.uploadID, &apigen.CompletePresignMultipartUploadParams{
				Path: tt.objName,
			}, apigen.CompletePresignMultipartUploadJSONRequestBody{
				PhysicalAddress: tt.physicalAddress,
				Parts:           fakeParts,
			})
			require.NoError(t, err, "CompletePresignMultipartUpload should succeed")
			require.Equalf(t, tt.statusCode, resp.StatusCode(), "CompletePresignMultipartUpload status code mismatch (expected %d): %s - %s", tt.statusCode, resp.Status(), resp.Body)
		})
	}
}

func TestPresignMultipartUploadSeparateParts(t *testing.T) {
	skipPresignMultipart(t)
	// not a short test
	if testing.Short() {
		t.Skip()
	}

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// Create a large object.
	r := mathrand.New(mathrand.NewSource(17))
	data := make([]byte, largeDataContentLength)
	_, err := r.Read(data)
	require.NoError(t, err)
	const largeObjectPath = "data/large"
	_, err = helpers.ClientUpload(ctx, client, repo, mainBranch, largeObjectPath, nil, "", bytes.NewReader(data))
	require.NoError(t, err, "Failed to upload large file for multipart upload test")

	cases := []struct {
		Name              string
		PresignSeparately bool
		Copy              bool
	}{
		{"presign all parts", false, false},
		{"presign each part separately", true, false},
		{"presign each part separately, copy", true, true},
		// API can only copy a part if we presign each part separately.
	}

	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			const numberOfParts = 2
			objPath := fmt.Sprintf("presign_multipart_upload/%s/complete", tt.Name)
			numberOfPartsToRequest := 0
			if !tt.PresignSeparately {
				numberOfPartsToRequest = numberOfParts
			}

			respCreate, err := client.CreatePresignMultipartUploadWithResponse(ctx, repo, mainBranch, &apigen.CreatePresignMultipartUploadParams{
				Path:  objPath,
				Parts: &numberOfPartsToRequest,
			})
			require.NoError(t, err)
			require.NotNil(t, respCreate.JSON201)

			uploadID := respCreate.JSON201.UploadId
			physicalAddress := respCreate.JSON201.PhysicalAddress

			// upload parts
			httpClient := http.Client{
				Timeout: 30 * time.Second, // make sure we do not wait forever
			}
			var (
				totalSize int64 = 0
				parts     []apigen.UploadPart
			)
			for i := 0; i < numberOfParts; i++ {
				startTime := time.Now()
				// random data - all parts except the last one should be at least >= MinUploadPartSize
				var (
					data          []byte
					partSize      int64
					contentLength int64
				)
				if tt.Copy && i == 0 {
					// Will copy everything except first and last bytes.
					partSize = largeDataContentLength - 2
					contentLength = 0
				} else {
					n, err := rand.Int(rand.Reader, big.NewInt(1<<20))
					require.NoError(t, err)
					if i < numberOfParts-1 {
						partSize = manager.MinUploadPartSize + n.Int64() // 5mb + ~1mb
					} else {
						partSize = n.Int64() + 1 // ~1mb + 1
					}
					contentLength = partSize
					data = make([]byte, partSize)
					_, err = rand.Read(data)
					require.NoError(t, err)
				}

				// upload part using presigned url
				var partPresignedURL string
				body := apigen.UploadPartFromJSONRequestBody{
					PhysicalAddress: physicalAddress,
				}
				if tt.Copy && i == 0 {
					body.Type = "copy"
					body.CopySource = &apigen.CopyPartSource{
						Repository: repo,
						Ref:        mainBranch,
						Path:       largeObjectPath,
						// the Range header is _inclusive_.  So this drops the last byte.
						Range: swag.String(fmt.Sprintf("bytes=%d-%d", 1, largeDataContentLength-2)),
					}
				}
				var etag string
				if tt.PresignSeparately {
					respGetPresigned, err := client.UploadPartFromWithResponse(ctx, repo, mainBranch, uploadID, i+1, &apigen.UploadPartFromParams{Path: objPath}, body)
					require.NoError(t, err)
					require.NoError(t, helpers.ResponseAsError(respGetPresigned))
					if respGetPresigned.JSON200 != nil {
						partPresignedURL = respGetPresigned.JSON200.PresignedUrl
					} else {
						etag = respGetPresigned.HTTPResponse.Header.Get("ETag")
					}
				} else {
					partPresignedURL = (*respCreate.JSON201.PresignedUrls)[i]
				}
				if partPresignedURL != "" {
					req, err := http.NewRequest(http.MethodPut, partPresignedURL, bytes.NewReader(data))
					require.NoError(t, err)
					req.ContentLength = contentLength
					req.Header.Set("Content-Type", "application/octet-stream")
					resp, err := httpClient.Do(req)
					require.NoError(t, err)
					_ = resp.Body.Close()
					require.Equal(t, http.StatusOK, resp.StatusCode)
					etag = resp.Header.Get("ETag")
				}

				// extract etag from response
				parts = append(parts, apigen.UploadPart{
					Etag:       etag,
					PartNumber: i + 1,
				})
				t.Logf("Uploaded part %d/%d, %d bytes, in %s", i+1, numberOfParts, partSize, time.Since(startTime))
				totalSize += partSize
			}

			// complete multipart upload
			t.Logf("Parts: %v", parts)
			resp, err := client.CompletePresignMultipartUploadWithResponse(ctx, repo, mainBranch, uploadID, &apigen.CompletePresignMultipartUploadParams{
				Path: objPath,
			}, apigen.CompletePresignMultipartUploadJSONRequestBody{
				ContentType:     swag.String("application/octet-stream"),
				Parts:           parts,
				PhysicalAddress: physicalAddress,
				UserMetadata: &apigen.CompletePresignMultipartUpload_UserMetadata{
					AdditionalProperties: map[string]string{"foo": "bar"},
				},
			})
			require.NoError(t, err, "CompletePresignMultipartUpload should succeed")
			require.Equalf(t, http.StatusOK, resp.StatusCode(), "CompletePresignMultipartUpload status code mismatch: %s - %s", resp.Status(), resp.Body)

			// verify entry is found
			statResp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{
				Path: objPath,
			})
			require.NoError(t, err)
			require.NotNil(t, statResp.JSON200)
			require.Equal(t, resp.JSON200.Checksum, statResp.JSON200.Checksum)
			require.Equal(t, swag.Int64Value(statResp.JSON200.SizeBytes), totalSize)
		})
	}
}

func skipPresignMultipart(t *testing.T) {
	if viper.GetString(ViperBlockstoreType) != "s3" {
		t.Skip("Skipping test - s3 only")
	}
}
