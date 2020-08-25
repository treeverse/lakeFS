package nessie

import (
	"bytes"
	"testing"

	"github.com/thanhpk/randstr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
)

const (
	numberOfParts = 2
	partSize      = 6* 1024 * 1024
)

func TestMultiPart(t *testing.T) {
	ctx, logger, repo := setupTest(t)

	path := masterBranch+"/multipart_file"
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(path),
	}

	resp, err := svc.CreateMultipartUpload(input)
	require.NoError(t, err, "failed to create multipart upload")
	logger.Info("Created multipart upload request")
	var completedParts []*s3.CompletedPart
	for partNumber := 1; partNumber <= numberOfParts; partNumber++ {
		completedPart, err := uploadPart(svc, resp, path, randstr.Bytes(partSize), partNumber)
		require.NoError(t, err, "failed to upload part %d", partNumber)

		completedParts = append(completedParts, completedPart)
	}

	completeResponse, err := completeMultipartUpload(svc, resp,path,completedParts)
	require.NoError(t, err, "failed to complete multipart upload")

	var b bytes.Buffer
	_, err = client.GetObject(ctx, repo, masterBranch, *completeResponse.Key, &b)
	require.NoError(t, err, "failed to get object")
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, path string, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      aws.String(path),
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput,path string, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           aws.String(path),
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	logger.WithField("partInput", *partInput).WithField("CreateMultipartUploadOutput", *resp).Info("HELPPPPPP")

	uploadResult, err := svc.UploadPart(partInput)
	if err != nil {
		return nil, err
	}

	return &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil
}
