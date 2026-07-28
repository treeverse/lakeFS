package esti

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	mathrand "math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/go-openapi/swag"
	"github.com/minio/crc64nvme"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
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
		{name: "empty_upload_id", repo: repo, branch: mainBranch, objName: "obj", uploadID: "", physicalAddress: "addr", statusCode: http.StatusNotFound}, // produces invalid endpoint
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
	for i := range 2 {
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
		{name: "empty_upload_id", repo: repo, branch: mainBranch, objName: "obj", uploadID: "", physicalAddress: "addr", parts: fakeParts, statusCode: http.StatusNotFound}, // produces invalid endpoint
		{name: "no_parts", repo: repo, branch: mainBranch, objName: "obj", uploadID: "upload_id", physicalAddress: "addr", parts: nil, statusCode: http.StatusBadRequest},
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
		{Name: "presign all parts", PresignSeparately: false, Copy: false},
		{Name: "presign each part separately", PresignSeparately: true, Copy: false},
		{Name: "presign each part separately, copy", PresignSeparately: true, Copy: true},
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
			for i := range numberOfParts {
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
				var etag string
				if tt.Copy && i == 0 {
					// Expand body to the wider type for Copy
					body := apigen.UploadPartCopyJSONRequestBody{
						UploadPartFrom: apigen.UploadPartFrom{
							PhysicalAddress: physicalAddress,
						},
						CopySource: apigen.CopyPartSource{
							Repository: repo,
							Ref:        mainBranch,
							Path:       largeObjectPath,
							// the Range header is _inclusive_.  So this drops the last byte.
							Range: swag.String(fmt.Sprintf("bytes=%d-%d", 1, largeDataContentLength-2)),
						},
					}
					respCopyPart, err := client.UploadPartCopyWithResponse(ctx, repo, mainBranch, uploadID, i+1, &apigen.UploadPartCopyParams{Path: objPath}, body)
					require.NoError(t, err)
					require.NoError(t, helpers.ResponseAsError(respCopyPart))
					require.Equal(t, http.StatusNoContent, respCopyPart.StatusCode())
					etag = respCopyPart.HTTPResponse.Header.Get("ETag")
				} else {
					var partPresignedURL string
					if tt.PresignSeparately {
						body := apigen.UploadPartJSONRequestBody{
							PhysicalAddress: physicalAddress,
						}
						respGetPresigned, err := client.UploadPartWithResponse(ctx, repo, mainBranch, uploadID, i+1, &apigen.UploadPartParams{Path: objPath}, body)
						require.NoError(t, err)
						require.NoError(t, helpers.ResponseAsError(respGetPresigned))
						require.Equal(t, http.StatusOK, respGetPresigned.StatusCode())
						partPresignedURL = respGetPresigned.JSON200.PresignedUrl
					} else {
						partPresignedURL = (*respCreate.JSON201.PresignedUrls)[i]
					}

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

func TestPresignMultipartUploadFullObjectChecksum(t *testing.T) {
	skipPresignMultipart(t)
	// not a short test
	if testing.Short() {
		t.Skip()
	}

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	configResp, err := client.GetStorageConfigWithResponse(ctx)
	require.NoError(t, err)
	require.NotNil(t, configResp.JSON200)
	if !swag.BoolValue(configResp.JSON200.PreSignMultipartUploadChecksum) {
		t.Skip("blockstore does not support presign multipart checksum validation")
	}

	// uploadChecksumParts creates a presign multipart upload requesting checksum
	// validation, uploads two parts and returns everything needed for completion.
	uploadChecksumParts := func(t *testing.T, objPath string, algorithm apigen.ChecksumAlgorithm, checksumType *apigen.ChecksumType) (uploadID, physicalAddress, checksum string, parts []apigen.UploadPart, totalSize int64) {
		t.Helper()
		const numberOfParts = 2
		respCreate, err := client.CreatePresignMultipartUploadWithResponse(ctx, repo, mainBranch, &apigen.CreatePresignMultipartUploadParams{
			Path:              objPath,
			Parts:             swag.Int(numberOfParts),
			ChecksumAlgorithm: &algorithm,
			ChecksumType:      checksumType,
		})
		require.NoError(t, err)
		require.Equalf(t, http.StatusCreated, respCreate.StatusCode(), "CreatePresignMultipartUpload status code mismatch: %s - %s", respCreate.Status(), respCreate.Body)
		require.NotNil(t, respCreate.JSON201)

		hasher := crc64nvme.New()
		httpClient := http.Client{Timeout: 30 * time.Second}
		for i := range numberOfParts {
			partSize := int64(manager.MinUploadPartSize + 100)
			if i == numberOfParts-1 {
				partSize = 1024
			}
			data := make([]byte, partSize)
			_, err := rand.Read(data)
			require.NoError(t, err)
			_, _ = hasher.Write(data)

			req, err := http.NewRequest(http.MethodPut, (*respCreate.JSON201.PresignedUrls)[i], bytes.NewReader(data))
			require.NoError(t, err)
			req.ContentLength = partSize
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.Equalf(t, http.StatusOK, resp.StatusCode, "part upload failed: %s - %s", resp.Status, body)
			parts = append(parts, apigen.UploadPart{
				Etag:       resp.Header.Get("ETag"),
				PartNumber: i + 1,
			})
			totalSize += partSize
		}
		checksum = base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		return respCreate.JSON201.UploadId, respCreate.JSON201.PhysicalAddress, checksum, parts, totalSize
	}

	// abortUpload cleans up an upload whose completion failed: 204 when the upload is
	// still open, 410 when the store already consumed it (validation runs after
	// assembly) — never a server error.
	abortUpload := func(t *testing.T, objPath, uploadID, physicalAddress string) {
		t.Helper()
		resp, err := client.AbortPresignMultipartUploadWithResponse(ctx, repo, mainBranch, uploadID, &apigen.AbortPresignMultipartUploadParams{
			Path: objPath,
		}, apigen.AbortPresignMultipartUploadJSONRequestBody{
			PhysicalAddress: physicalAddress,
		})
		require.NoError(t, err)
		require.Containsf(t, []int{http.StatusNoContent, http.StatusGone}, resp.StatusCode(),
			"abort after failed completion: %s - %s", resp.Status(), resp.Body)
	}

	for _, tt := range []struct {
		name         string
		checksumType *apigen.ChecksumType
	}{
		{name: "valid CRC64NVME", checksumType: apiutil.Ptr(apigen.ChecksumType_FULL_OBJECT)},
		{name: "valid CRC64NVME default type", checksumType: nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			algorithm := apigen.ChecksumAlgorithm_CRC64NVME
			objPath := fmt.Sprintf("presign_multipart_upload/checksum/%s", tt.name)
			uploadID, physicalAddress, checksum, parts, totalSize := uploadChecksumParts(t, objPath, algorithm, tt.checksumType)

			resp, err := client.CompletePresignMultipartUploadWithResponse(ctx, repo, mainBranch, uploadID, &apigen.CompletePresignMultipartUploadParams{
				Path: objPath,
			}, apigen.CompletePresignMultipartUploadJSONRequestBody{
				PhysicalAddress:   physicalAddress,
				Parts:             parts,
				ChecksumAlgorithm: &algorithm,
				ChecksumType:      tt.checksumType,
				Checksum:          swag.String(checksum),
				MpuObjectSize:     swag.Int64(totalSize),
			})
			require.NoError(t, err)
			require.Equalf(t, http.StatusOK, resp.StatusCode(), "CompletePresignMultipartUpload status code mismatch: %s - %s", resp.Status(), resp.Body)
			require.NotNil(t, resp.JSON200.Checksums)
			require.Equal(t, checksum, resp.JSON200.Checksums.AdditionalProperties[string(algorithm)])

			// the validated checksum is stored in the catalog and retrievable afterwards
			statResp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: objPath})
			require.NoError(t, err)
			require.NotNil(t, statResp.JSON200)
			require.Equal(t, totalSize, swag.Int64Value(statResp.JSON200.SizeBytes))
			require.NotNil(t, statResp.JSON200.Checksums)
			require.Equal(t, checksum, statResp.JSON200.Checksums.AdditionalProperties[string(algorithm)])
		})
	}

	t.Run("valid CRC64NVME with copied part", func(t *testing.T) {
		// the storage computes the full-object checksum over the assembled object,
		// so copied parts are covered the same as uploaded ones
		srcData := make([]byte, largeDataContentLength)
		_, err := rand.Read(srcData)
		require.NoError(t, err)
		const srcPath = "presign_multipart_upload/checksum/copy-source"
		_, err = helpers.ClientUpload(ctx, client, repo, mainBranch, srcPath, nil, "", bytes.NewReader(srcData))
		require.NoError(t, err)

		const objPath = "presign_multipart_upload/checksum/copy"
		algorithm := apigen.ChecksumAlgorithm_CRC64NVME
		respCreate, err := client.CreatePresignMultipartUploadWithResponse(ctx, repo, mainBranch, &apigen.CreatePresignMultipartUploadParams{
			Path:              objPath,
			Parts:             swag.Int(2),
			ChecksumAlgorithm: &algorithm,
		})
		require.NoError(t, err)
		require.Equalf(t, http.StatusCreated, respCreate.StatusCode(), "CreatePresignMultipartUpload status code mismatch: %s - %s", respCreate.Status(), respCreate.Body)
		uploadID, physicalAddress := respCreate.JSON201.UploadId, respCreate.JSON201.PhysicalAddress

		// part 1: server-side copy of the source prefix (the Range header is inclusive)
		copyLen := int64(manager.MinUploadPartSize + 100)
		respCopy, err := client.UploadPartCopyWithResponse(ctx, repo, mainBranch, uploadID, 1, &apigen.UploadPartCopyParams{Path: objPath},
			apigen.UploadPartCopyJSONRequestBody{
				UploadPartFrom: apigen.UploadPartFrom{PhysicalAddress: physicalAddress},
				CopySource: apigen.CopyPartSource{
					Repository: repo,
					Ref:        mainBranch,
					Path:       srcPath,
					Range:      swag.String(fmt.Sprintf("bytes=0-%d", copyLen-1)),
				},
			})
		require.NoError(t, err)
		require.Equalf(t, http.StatusNoContent, respCopy.StatusCode(), "UploadPartCopy status code mismatch: %s - %s", respCopy.Status(), respCopy.Body)
		parts := []apigen.UploadPart{{PartNumber: 1, Etag: respCopy.HTTPResponse.Header.Get("ETag")}}

		// part 2: regular presigned upload
		part2 := make([]byte, 1024)
		_, err = rand.Read(part2)
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPut, (*respCreate.JSON201.PresignedUrls)[1], bytes.NewReader(part2))
		require.NoError(t, err)
		req.ContentLength = int64(len(part2))
		httpClient := http.Client{Timeout: 30 * time.Second}
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		parts = append(parts, apigen.UploadPart{PartNumber: 2, Etag: resp.Header.Get("ETag")})

		hasher := crc64nvme.New()
		_, _ = hasher.Write(srcData[:copyLen])
		_, _ = hasher.Write(part2)
		checksum := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		totalSize := copyLen + int64(len(part2))

		respComplete, err := client.CompletePresignMultipartUploadWithResponse(ctx, repo, mainBranch, uploadID, &apigen.CompletePresignMultipartUploadParams{
			Path: objPath,
		}, apigen.CompletePresignMultipartUploadJSONRequestBody{
			PhysicalAddress:   physicalAddress,
			Parts:             parts,
			ChecksumAlgorithm: &algorithm,
			Checksum:          swag.String(checksum),
			MpuObjectSize:     swag.Int64(totalSize),
		})
		require.NoError(t, err)
		require.Equalf(t, http.StatusOK, respComplete.StatusCode(), "CompletePresignMultipartUpload status code mismatch: %s - %s", respComplete.Status(), respComplete.Body)

		statResp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: objPath})
		require.NoError(t, err)
		require.NotNil(t, statResp.JSON200)
		require.Equal(t, totalSize, swag.Int64Value(statResp.JSON200.SizeBytes))
		require.NotNil(t, statResp.JSON200.Checksums)
		require.Equal(t, checksum, statResp.JSON200.Checksums.AdditionalProperties[string(algorithm)])
	})

	t.Run("wrong checksum", func(t *testing.T) {
		const objPath = "presign_multipart_upload/checksum/wrong-value"
		algorithm := apigen.ChecksumAlgorithm_CRC64NVME
		uploadID, physicalAddress, checksum, parts, totalSize := uploadChecksumParts(t, objPath, algorithm, nil)
		defer abortUpload(t, objPath, uploadID, physicalAddress)

		// flip the checksum first byte to get a valid-looking but wrong value
		raw, err := base64.StdEncoding.DecodeString(checksum)
		require.NoError(t, err)
		raw[0] ^= 0xff
		resp, err := client.CompletePresignMultipartUploadWithResponse(ctx, repo, mainBranch, uploadID, &apigen.CompletePresignMultipartUploadParams{
			Path: objPath,
		}, apigen.CompletePresignMultipartUploadJSONRequestBody{
			PhysicalAddress:   physicalAddress,
			Parts:             parts,
			ChecksumAlgorithm: &algorithm,
			Checksum:          swag.String(base64.StdEncoding.EncodeToString(raw)),
			MpuObjectSize:     swag.Int64(totalSize),
		})
		require.NoError(t, err)
		require.Equalf(t, http.StatusBadRequest, resp.StatusCode(), "wrong checksum should fail completion: %s - %s", resp.Status(), resp.Body)
	})

	t.Run("wrong object size", func(t *testing.T) {
		const objPath = "presign_multipart_upload/checksum/wrong-size"
		algorithm := apigen.ChecksumAlgorithm_CRC64NVME
		uploadID, physicalAddress, checksum, parts, totalSize := uploadChecksumParts(t, objPath, algorithm, nil)
		defer abortUpload(t, objPath, uploadID, physicalAddress)

		resp, err := client.CompletePresignMultipartUploadWithResponse(ctx, repo, mainBranch, uploadID, &apigen.CompletePresignMultipartUploadParams{
			Path: objPath,
		}, apigen.CompletePresignMultipartUploadJSONRequestBody{
			PhysicalAddress:   physicalAddress,
			Parts:             parts,
			ChecksumAlgorithm: &algorithm,
			Checksum:          swag.String(checksum),
			MpuObjectSize:     swag.Int64(totalSize + 1),
		})
		require.NoError(t, err)
		require.Equalf(t, http.StatusBadRequest, resp.StatusCode(), "wrong object size should fail completion: %s - %s", resp.Status(), resp.Body)
	})

	t.Run("invalid checksum requests", func(t *testing.T) {
		// controller-level validation, no upload needed
		createResp, err := client.CreatePresignMultipartUploadWithResponse(ctx, repo, mainBranch, &apigen.CreatePresignMultipartUploadParams{
			Path:         "presign_multipart_upload/checksum/invalid",
			ChecksumType: apiutil.Ptr(apigen.ChecksumType_FULL_OBJECT),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, createResp.StatusCode(), "checksum_type without checksum_algorithm")

		completeBodies := map[string]apigen.CompletePresignMultipartUploadJSONRequestBody{
			"checksum without algorithm": {
				PhysicalAddress: "addr",
				Parts:           []apigen.UploadPart{{PartNumber: 1, Etag: "etag"}},
				Checksum:        swag.String("AAAAAAAAAAA="),
			},
			"algorithm without checksum": {
				PhysicalAddress:   "addr",
				Parts:             []apigen.UploadPart{{PartNumber: 1, Etag: "etag"}},
				ChecksumAlgorithm: apiutil.Ptr(apigen.ChecksumAlgorithm_CRC64NVME),
			},
			"invalid base64 checksum": {
				PhysicalAddress:   "addr",
				Parts:             []apigen.UploadPart{{PartNumber: 1, Etag: "etag"}},
				ChecksumAlgorithm: apiutil.Ptr(apigen.ChecksumAlgorithm_CRC64NVME),
				Checksum:          swag.String("not base64!"),
			},
			"wrong checksum length": {
				PhysicalAddress:   "addr",
				Parts:             []apigen.UploadPart{{PartNumber: 1, Etag: "etag"}},
				ChecksumAlgorithm: apiutil.Ptr(apigen.ChecksumAlgorithm_CRC64NVME),
				Checksum:          swag.String("AAAAAA=="), // 4 bytes, CRC64NVME takes 8
			},
			"negative object size": {
				PhysicalAddress: "addr",
				Parts:           []apigen.UploadPart{{PartNumber: 1, Etag: "etag"}},
				MpuObjectSize:   swag.Int64(-1),
			},
		}
		for name, body := range completeBodies {
			t.Run(name, func(t *testing.T) {
				resp, err := client.CompletePresignMultipartUploadWithResponse(ctx, repo, mainBranch, "upload-id", &apigen.CompletePresignMultipartUploadParams{
					Path: "presign_multipart_upload/checksum/invalid",
				}, body)
				require.NoError(t, err)
				require.Equalf(t, http.StatusBadRequest, resp.StatusCode(), "expected 400: %s - %s", resp.Status(), resp.Body)
			})
		}
	})
}

func skipPresignMultipart(t *testing.T) {
	if viper.GetString(ViperBlockstoreType) != "s3" {
		t.Skip("Skipping test - s3 only")
	}
}
