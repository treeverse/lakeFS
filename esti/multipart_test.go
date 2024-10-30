package esti

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	multipartNumberOfParts = 7
	multipartPartSize      = 5 * 1024 * 1024
)

func TestMultipartUpload(t *testing.T) {
	// timeResolution is a duration greater than the timestamp resolution of the backing
	// store.  Multipart object on S3 is the time of create-MPU, waiting before completion
	// ensures that lakeFS did not use the current time.  For other blockstores MPU
	// completion time is used, meaning it will be hard to detect if the underlying and
	// lakeFS objects share the same time.
	const timeResolution = time.Second

	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	file := "multipart_file"
	path := mainBranch + "/" + file
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(path),
	}

	resp, err := svc.CreateMultipartUpload(ctx, input)
	require.NoError(t, err, "failed to create multipart upload")
	logger.Info("Created multipart upload request")

	parts := make([][]byte, multipartNumberOfParts)
	var partsConcat []byte
	for i := 0; i < multipartNumberOfParts; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i)
		partsConcat = append(partsConcat, parts[i]...)
	}

	completedParts := uploadMultipartParts(t, ctx, logger, resp, parts, 0)

	if isBlockstoreType(block.BlockstoreTypeS3) == nil {
		// Object should have Last-Modified time at around time of MPU creation.  Ensure
		// lakeFS fails the test if it fakes it by using the current time.
		time.Sleep(2 * timeResolution)
	}

	completeResponse, err := uploadMultipartComplete(ctx, svc, resp, completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", aws.ToString(completeResponse.Key)).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &apigen.GetObjectParams{Path: file})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	if !bytes.Equal(partsConcat, getResp.Body) {
		t.Fatalf("uploaded object did not match")
	}

	statResp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: file, Presign: aws.Bool(true)})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode(), getResp.Status())

	// Get last-modified from the underlying store.

	presignedGetURL := statResp.JSON200.PhysicalAddress
	res, err := http.Get(presignedGetURL)
	require.NoError(t, err, "GET underlying")
	// The presigned URL is usable only for GET, but we don't actually care about its body.
	_ = res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode, "%s: %s", presignedGetURL, res.Status)
	lastModifiedString := res.Header.Get("Last-Modified")
	underlyingLastModified, err := time.Parse(time.RFC1123, lastModifiedString)
	require.NoError(t, err, "Last-Modified %s", lastModifiedString)
	// Last-Modified header includes a timezone, which is typically "GMT" on AWS.  Now GMT
	// _is equal to_ UTC!.  But Go is nothing if not cautious, and considers UTC and GMT to
	// be different timezones.  So cannot compare with "==" and must use time.Time.Equal.
	lakeFSMTime := time.Unix(statResp.JSON200.Mtime, 0)
	require.True(t, lakeFSMTime.Equal(underlyingLastModified),
		"lakeFS mtime %s should be same as on underlying object %s", lakeFSMTime, underlyingLastModified)
}

func TestMultipartUploadAbort(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	t.Run("exists", func(t *testing.T) {
		const objPath = mainBranch + "/multipart_file1"
		createInput := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(repo),
			Key:    aws.String(objPath),
		}
		createResp, err := svc.CreateMultipartUpload(ctx, createInput)
		require.NoError(t, err, "CreateMultipartUpload")

		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(repo),
			Key:      aws.String(objPath),
			UploadId: createResp.UploadId,
		}
		_, err = svc.AbortMultipartUpload(ctx, abortInput)
		require.NoError(t, err, "AbortMultipartUpload")
	})

	t.Run("unknown_upload_id", func(t *testing.T) {
		const objPath = mainBranch + "/multipart_file2"
		createInput := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(repo),
			Key:    aws.String(objPath),
		}
		createResp, err := svc.CreateMultipartUpload(ctx, createInput)
		require.NoError(t, err, "CreateMultipartUpload")

		uploadID := aws.ToString(createResp.UploadId)
		// reverse the upload id to get valid unknown upload id
		unknownUploadID := reverse(uploadID)

		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(repo),
			Key:      aws.String(objPath),
			UploadId: aws.String(unknownUploadID),
		}
		_, err = svc.AbortMultipartUpload(ctx, abortInput)
		require.Error(t, err, "AbortMultipartUpload should fail with unknown upload id")
	})

	t.Run("unknown_key", func(t *testing.T) {
		const objPath = mainBranch + "/multipart_file3"
		createInput := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(repo),
			Key:    aws.String(objPath),
		}
		createResp, err := svc.CreateMultipartUpload(ctx, createInput)
		require.NoError(t, err, "CreateMultipartUpload")

		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(repo),
			Key:      aws.String(mainBranch + "/unknown_file"),
			UploadId: createResp.UploadId,
		}
		_, err = svc.AbortMultipartUpload(ctx, abortInput)
		require.Error(t, err, "AbortMultipartUpload should fail with unknown key")
	})
}

func reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func uploadMultipartParts(t *testing.T, ctx context.Context, logger logging.Logger, resp *s3.CreateMultipartUploadOutput, parts [][]byte, firstIndex int) []types.CompletedPart {
	count := len(parts)
	completedParts := make([]types.CompletedPart, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			partNumber := firstIndex + i + 1
			completedParts[i], errs[i] = uploadMultipartPart(ctx, logger, svc, resp, parts[i], partNumber)
		}(i)
	}
	wg.Wait()

	// verify upload completed successfully
	for i, err := range errs {
		partNumber := aws.Int32(int32(firstIndex + i + 1))
		assert.NoErrorf(t, err, "error while upload part number %d", partNumber)
		// verify part number
		assert.Equal(t, partNumber, completedParts[i].PartNumber, "inconsistent part number")
	}
	return completedParts
}

func uploadMultipartComplete(ctx context.Context, svc *s3.Client, resp *s3.CreateMultipartUploadOutput, completedParts []types.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(ctx, completeInput)
}

func uploadMultipartPart(ctx context.Context, logger logging.Logger, svc *s3.Client, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (types.CompletedPart, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int32(int32(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	uploadResult, err := svc.UploadPart(ctx, partInput)
	if err != nil {
		return types.CompletedPart{}, err
	}

	logger.WithField("partNumber", partNumber).Info("Uploaded part successfully")

	return types.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int32(int32(partNumber)),
	}, nil
}
