package esti

import (
	"bytes"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	multipartNumberOfParts = 7
	multipartPartSize      = 5 * 1024 * 1024
)

func TestMultipartUpload(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	file := "multipart_file"
	path := mainBranch + "/" + file
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(path),
	}

	resp, err := svc.CreateMultipartUpload(input)
	require.NoError(t, err, "failed to create multipart upload")
	logger.Info("Created multipart upload request")

	parts := make([][]byte, multipartNumberOfParts)
	var partsConcat []byte
	for i := 0; i < multipartNumberOfParts; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i)
		partsConcat = append(partsConcat, parts[i]...)
	}

	completedParts := uploadMultipartParts(t, logger, resp, parts, 0)

	completeResponse, err := uploadMultipartComplete(svc, resp, completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{Path: file})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.Equal(t, partsConcat, getResp.Body, "uploaded object did not match")
}

func uploadMultipartParts(t *testing.T, logger logging.Logger, resp *s3.CreateMultipartUploadOutput, parts [][]byte, firstIndex int) []*s3.CompletedPart {
	count := len(parts)
	completedParts := make([]*s3.CompletedPart, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			partNumber := firstIndex + i + 1
			completedParts[i], errs[i] = uploadMultipartPart(logger, svc, resp, parts[i], partNumber)
		}(i)
	}
	wg.Wait()

	// verify upload completed successfully
	for i, err := range errs {
		partNumber := int64(firstIndex + i + 1)
		assert.NoErrorf(t, err, "error while upload part number %d", partNumber)
		// verify part number
		assert.Equal(t, partNumber, *(completedParts[i].PartNumber), "inconsistent part number")
	}
	return completedParts
}

func uploadMultipartComplete(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadMultipartPart(logger logging.Logger, svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	uploadResult, err := svc.UploadPart(partInput)
	if err != nil {
		return nil, err
	}

	logger.WithField("partNumber", partNumber).Info("Uploaded part successfully")

	return &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil
}
