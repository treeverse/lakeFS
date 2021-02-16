package nessie

import (
	"bytes"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/logging"
)

const (
	multipartNumberOfParts = 7
	multipartPartSize      = 5 * 1024 * 1024
)

func TestMultipartUpload(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	file := "multipart_file"
	path := masterBranch + "/" + file
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

	completedParts := uploadMultipartParts(t, logger, resp, parts)

	completeResponse, err := uploadMultipartComplete(svc, resp, completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipart request successfully")

	var b bytes.Buffer
	_, err = client.Objects.GetObject(
		objects.NewGetObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithRef(masterBranch).
			WithPath(file), nil, &b)
	require.NoError(t, err, "failed to get object")
	require.Equal(t, b.Bytes(), partsConcat, "uploaded object did not match")
}

func uploadMultipartParts(t *testing.T, logger logging.Logger, resp *s3.CreateMultipartUploadOutput, parts [][]byte) []*s3.CompletedPart {
	completedParts := make([]*s3.CompletedPart, multipartNumberOfParts)
	errs := make([]error, multipartNumberOfParts)
	var wg sync.WaitGroup
	wg.Add(multipartNumberOfParts)
	for i := 0; i < multipartNumberOfParts; i++ {
		go func(i int) {
			defer wg.Done()
			partNumber := i + 1
			completedParts[i], errs[i] = uploadMultipartPart(logger, svc, resp, parts[i], partNumber)
		}(i)
	}
	wg.Wait()

	// verify upload completed successfully
	for i, err := range errs {
		partNumber := int64(i + 1)
		assert.NoErrorf(t, err, "error while upload part number %d", partNumber)
		// verify part number
		assert.Equal(t, partNumber, swag.Int64Value(completedParts[i].PartNumber), "inconsistent part number")
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
