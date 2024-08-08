package esti

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestDeleteObjects(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	const numOfObjects = 10

	identifiers := make([]types.ObjectIdentifier, 0, numOfObjects)

	for i := 1; i <= numOfObjects; i++ {
		file := strconv.Itoa(i) + ".txt"
		identifiers = append(identifiers, types.ObjectIdentifier{
			Key: aws.String(mainBranch + "/" + file),
		})
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, file)
	}

	listOut, err := svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})

	require.NoError(t, err)
	require.Len(t, listOut.Contents, numOfObjects)

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

// TestDeleteObjects_Viewer verify we can't delete with read only user
func TestDeleteObjects_Viewer(t *testing.T) {
	t.SkipNow()
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// setup data
	const filename = "delete-me"
	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, filename)

	// setup user with only view rights - create user, add to group, generate credentials
	uid := "del-viewer"
	resCreateUser, err := client.CreateUserWithResponse(ctx, apigen.CreateUserJSONRequestBody{
		Id: uid,
	})
	require.NoError(t, err, "Admin failed while creating user")
	require.Equal(t, http.StatusCreated, resCreateUser.StatusCode(), "Admin unexpectedly failed to create user")

	resAssociateUser, err := client.AddGroupMembershipWithResponse(ctx, "Viewers", "del-viewer")
	require.NoError(t, err, "Failed to add user to Viewers group")
	require.Equal(t, http.StatusCreated, resAssociateUser.StatusCode(), "AddGroupMembershipWithResponse unexpectedly status code")

	resCreateCreds, err := client.CreateCredentialsWithResponse(ctx, "del-viewer")
	require.NoError(t, err, "Failed to create credentials")
	require.NotNil(t, resCreateCreds.JSON201, "CreateCredentials unexpectedly empty response")

	// client with viewer user credentials
	key := resCreateCreds.JSON201.AccessKeyId
	secret := resCreateCreds.JSON201.SecretAccessKey
	s3Endpoint := viper.GetString("s3_endpoint")
	forcePathStyle := viper.GetBool("force_path_style")
	s3Client, err := testutil.SetupTestS3Client(s3Endpoint, key, secret, forcePathStyle)
	require.NoError(t, err)

	// delete objects using viewer
	deleteOut, err := s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(repo),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String(mainBranch + "/" + filename)},
			},
		},
	})
	// make sure we got an error we fail to delete the file
	assert.NoError(t, err)
	assert.Len(t, deleteOut.Errors, 1, "error we fail to delete")
	assert.Len(t, deleteOut.Deleted, 0, "no file should be deleted")

	// verify that viewer can't delete the file
	listOut, err := svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})
	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, 1, "list should find 'delete-me' file")
	assert.Equal(t, aws.ToString(listOut.Contents[0].Key), mainBranch+"/"+filename)
}
