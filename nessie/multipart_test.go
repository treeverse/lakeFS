// +build systemtests

package nessie

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/logging"
)

const (
	numberOfParts = 5
	partSize      = 6 * 1024 * 1024
)

func TestMultiPart(t *testing.T) {
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

	completedParts := uploadRandomParts(t, logger, resp)

	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipart request successfully")

	var b bytes.Buffer
	_, err = client.GetObject(ctx, repo, masterBranch, file, &b)
	require.NoError(t, err, "failed to get object")
}

func uploadRandomParts(t *testing.T, logger logging.Logger, resp *s3.CreateMultipartUploadOutput) []*s3.CompletedPart {
	t.Helper()
	ch := make(chan *s3.CompletedPart, numberOfParts)
	errs := make(chan error, numberOfParts)
	var wg sync.WaitGroup

	wg.Add(numberOfParts)
	for partNumber := 1; partNumber <= numberOfParts; partNumber++ {
		go func(partNumber int) {
			completedPart, err := uploadPart(logger, svc, resp, randstr.Bytes(partSize), partNumber)

			if err != nil {
				// keep just the first error
				errs <- fmt.Errorf("failed to upload part %d: %w", partNumber, err)
			} else if int(*completedPart.PartNumber) != partNumber {
				errs <- fmt.Errorf("inconsistent part number. Expected %d, got %d", partNumber, *completedPart.PartNumber)
			}
			ch <- completedPart
			wg.Done()
		}(partNumber)
	}

	wg.Wait()
	close(ch)
	close(errs)

	errNum := 0
	for err := range errs {
		errNum++
		assert.NoError(t, err)
	}
	require.Equal(t, 0, errNum, "errors in parts upload")

	// completedParts must be ordered by the part number
	completedParts := make([]*s3.CompletedPart, numberOfParts)
	for completedPart := range ch {
		completedParts[*completedPart.PartNumber-1] = completedPart
	}
	return completedParts
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
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

func uploadPart(logger logging.Logger, svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
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
