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
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	multipartNumberOfParts = 7
	multipartPartSize      = 5 * 1024 * 1024
)

func TestMultipartUpload(t *testing.T) {
	// timeSlippage is a bound on the time difference between the local server and the S3
	// server.  It is used to verify that the "Last-Modified" time is actually close to the
	// CreateMultipartUpload time.  BUT S3 provides this time only at a 1-second resolution.
	// So a 1-second difference is possible.
	const timeSlippage = time.Second

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

	uploadTime := time.Now()

	parts := make([][]byte, multipartNumberOfParts)
	var partsConcat []byte
	for i := 0; i < multipartNumberOfParts; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i)
		partsConcat = append(partsConcat, parts[i]...)
	}

	completedParts := uploadMultipartParts(t, ctx, logger, resp, parts, 0)

	// Object should have Last-Modified time at around time of MPU creation.  Server times
	// after this Sleep will be more than timeSlippage away from uploadTime.
	time.Sleep(2 * timeSlippage)

	completeResponse, err := uploadMultipartComplete(ctx, svc, resp, completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", aws.ToString(completeResponse.Key)).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &apigen.GetObjectParams{Path: file})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	if !bytes.Equal(partsConcat, getResp.Body) {
		t.Fatalf("uploaded object did not match")
	}

	statResp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: file})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	lastModified := time.Unix(statResp.JSON200.Mtime, 0)
	require.Less(t, lastModified.Sub(uploadTime).Abs(), timeSlippage,
		"(remote) last modified time %s too far away from (local) upload time %s",
		lastModified, uploadTime)
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
