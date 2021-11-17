package nessie

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

	completedParts := uploadMultipartParts(t, ctx, logger, resp, parts)

	completeResponse, err := uploadMultipartComplete(ctx, svc, resp, completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{Path: file})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.Equal(t, getResp.Body, partsConcat, "uploaded object did not match")
}

func uploadMultipartParts(t *testing.T, ctx context.Context, logger logging.Logger, resp *s3.CreateMultipartUploadOutput, parts [][]byte) []types.CompletedPart {
	completedParts := make([]types.CompletedPart, multipartNumberOfParts)
	errs := make([]error, multipartNumberOfParts)
	var wg sync.WaitGroup
	wg.Add(multipartNumberOfParts)
	for i := 0; i < multipartNumberOfParts; i++ {
		go func(i int) {
			defer wg.Done()
			partNumber := i + 1
			completedParts[i], errs[i] = uploadMultipartPart(ctx, logger, svc, resp, parts[i], partNumber)
		}(i)
	}
	wg.Wait()

	// verify upload completed successfully
	for i, err := range errs {
		partNumber := int64(i + 1)
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
		PartNumber:    int32(partNumber),
		UploadId:      resp.UploadId,
		ContentLength: int64(len(fileBytes)),
	}

	uploadResult, err := svc.UploadPart(ctx, partInput)
	if err != nil {
		return types.CompletedPart{}, err
	}

	logger.WithField("partNumber", partNumber).Info("Uploaded part successfully")

	return types.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: int32(partNumber),
	}, nil
}
