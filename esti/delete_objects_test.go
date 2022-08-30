package esti

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

func TestDeleteObjects(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	const numOfObjects = 10

	identifiers := make([]*s3.ObjectIdentifier, 0, numOfObjects)

	for i := 1; i <= numOfObjects; i++ {
		file := strconv.Itoa(i) + ".txt"
		identifiers = append(identifiers, &s3.ObjectIdentifier{
			Key: aws.String(mainBranch + "/" + file),
		})
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, file, false)
	}

	listOut, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})

	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, numOfObjects)

	deleteOut, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(repo),
		Delete: &s3.Delete{
			Objects: identifiers,
		},
	})

	assert.NoError(t, err)
	assert.Len(t, deleteOut.Deleted, numOfObjects)

	listOut, err = svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})

	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, 0)
}
