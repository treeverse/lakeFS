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
	"github.com/treeverse/lakefs/pkg/api/apigen"
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

	logger.WithField("key", aws.StringValue(completeResponse.Key)).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &apigen.GetObjectParams{Path: file})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	if !bytes.Equal(partsConcat, getResp.Body) {
		t.Fatalf("uploaded object did not match")
	}
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
		createResp, err := svc.CreateMultipartUploadWithContext(ctx, createInput)
		require.NoError(t, err, "CreateMultipartUpload")

		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(repo),
			Key:      aws.String(objPath),
			UploadId: createResp.UploadId,
		}
		_, err = svc.AbortMultipartUploadWithContext(ctx, abortInput)
		require.NoError(t, err, "AbortMultipartUploadWithContext")
	})

	t.Run("unknown_upload_id", func(t *testing.T) {
		const objPath = mainBranch + "/multipart_file2"
		createInput := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(repo),
			Key:    aws.String(objPath),
		}
		createResp, err := svc.CreateMultipartUploadWithContext(ctx, createInput)
		require.NoError(t, err, "CreateMultipartUpload")

		uploadID := aws.StringValue(createResp.UploadId)
		// reverse the upload id to get valid unknown upload id
		unknownUploadID := reverse(uploadID)

		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(repo),
			Key:      aws.String(objPath),
			UploadId: aws.String(unknownUploadID),
		}
		_, err = svc.AbortMultipartUploadWithContext(ctx, abortInput)
		require.Error(t, err, "AbortMultipartUploadWithContext should fail with unknown upload id")
	})

	t.Run("unknown_key", func(t *testing.T) {
		const objPath = mainBranch + "/multipart_file3"
		createInput := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(repo),
			Key:    aws.String(objPath),
		}
		createResp, err := svc.CreateMultipartUploadWithContext(ctx, createInput)
		require.NoError(t, err, "CreateMultipartUpload")

		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(repo),
			Key:      aws.String(mainBranch + "/unknown_file"),
			UploadId: createResp.UploadId,
		}
		_, err = svc.AbortMultipartUploadWithContext(ctx, abortInput)
		require.Error(t, err, "AbortMultipartUploadWithContext should fail with unknown key")
	})
}

func reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
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
