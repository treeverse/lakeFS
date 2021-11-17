package nessie

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

func TestDeleteObjects(t *testing.T) {
	ctx, _, repo := setupTest(t)
	const numOfObjects = 10

	identifiers := make([]types.ObjectIdentifier, 0, numOfObjects)

	for i := 1; i <= numOfObjects; i++ {
		file := strconv.Itoa(i) + ".txt"
		identifiers = append(identifiers, types.ObjectIdentifier{
			Key: aws.String(mainBranch + "/" + file),
		})
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, file, false)
	}

	listOut, err := svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})

	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, numOfObjects)

	deleteOut, err := svc.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(repo),
		Delete: &types.Delete{
			Objects: identifiers,
		},
	})

	assert.NoError(t, err)
	assert.Len(t, deleteOut.Deleted, numOfObjects)

	listOut, err = svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})

	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, 0)
}
