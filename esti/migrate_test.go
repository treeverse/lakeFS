package esti

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
)

// state to be used
var state migrateTestState

type migrateTestState struct {
	Multipart multipartState
}

type multipartState struct {
	Repo           string                         `json:"repo"`
	Info           s3.CreateMultipartUploadOutput `json:"info"`
	CompletedParts []*s3.CompletedPart            `json:"completed_parts"`
	Content        string                         `json:"state"`
}

const (
	migrateMultipartFile     = "multipart_file"
	migrateMultipartFilepath = mainBranch + "/" + migrateMultipartFile
	migrateStateRepoName     = "migrate"
	migrateStateObjectPath   = "state.json"
	migratePrePartsCount     = 3
	migratePostPartsCount    = 2
)

func TestMigrate(t *testing.T) {
	postMigrate := viper.GetViper().GetBool("post_migrate")

	if postMigrate {
		postMigrateTests(t)
	} else {
		preMigrateTests(t)
	}
}

func preMigrateTests(t *testing.T) {
	// all pre tests execution
	t.Run("TestPreMigrateMultipart", testPreMigrateMultipart)

	saveStateInLakeFS(t)
}

func postMigrateTests(t *testing.T) {
	readStateFromLakeFS(t)

	// all post tests execution
	t.Run("TestPostMigrateMultipart", testPostMigrateMultipart)
}

func saveStateInLakeFS(t *testing.T) {
	// write the state file
	stateBytes, err := json.Marshal(&state)
	require.NoError(t, err, "marshal state")

	ctx := context.Background()
	_ = createRepositoryByName(context.Background(), t, migrateStateRepoName)
	resp, err := uploadContent(ctx, migrateStateRepoName, "main", migrateStateObjectPath, string(stateBytes))
	require.NoError(t, err, "writing state file")
	require.Equal(t, http.StatusCreated, resp.StatusCode())
}

func readStateFromLakeFS(t *testing.T) {
	// read the state file
	ctx := context.Background()
	resp, err := client.GetObjectWithResponse(ctx, migrateStateRepoName, "main", &api.GetObjectParams{Path: migrateStateObjectPath})
	require.NoError(t, err, "reading state file")
	require.Equal(t, http.StatusOK, resp.StatusCode())

	err = json.Unmarshal(resp.Body, &state)
	require.NoError(t, err, "unmarshal state from response")
}

func testPreMigrateMultipart(t *testing.T) {
	_, logger, repo := setupTest(t)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(migrateMultipartFilepath),
	}

	resp, err := svc.CreateMultipartUpload(input)
	require.NoError(t, err, "failed to create multipart upload")
	logger.Info("Created multipart upload request")

	partsConcat, completedParts := createAndUploadParts(t, logger, resp, migratePrePartsCount, 0)

	state.Multipart.Repo = repo
	state.Multipart.Info = *resp
	state.Multipart.CompletedParts = completedParts
	state.Multipart.Content = base64.StdEncoding.EncodeToString(partsConcat)
}

func createAndUploadParts(t *testing.T, logger logging.Logger, resp *s3.CreateMultipartUploadOutput, count, firstIndex int) ([]byte, []*s3.CompletedPart) {
	parts := make([][]byte, count)
	var partsConcat []byte
	for i := 0; i < count; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i + firstIndex)
		partsConcat = append(partsConcat, parts[i]...)
	}

	completedParts := uploadMultipartParts(t, logger, resp, parts, firstIndex)
	return partsConcat, completedParts
}

func testPostMigrateMultipart(t *testing.T) {
	ctx := context.Background()

	partsConcat, completedParts := createAndUploadParts(t, logger, &state.Multipart.Info, migratePostPartsCount, migratePrePartsCount)

	completeResponse, err := uploadMultipartComplete(svc, &state.Multipart.Info, append(state.Multipart.CompletedParts, completedParts...))
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, state.Multipart.Repo, mainBranch, &api.GetObjectParams{Path: migrateMultipartFile})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())

	preContentBytes, err := base64.StdEncoding.DecodeString(state.Multipart.Content)
	require.NoError(t, err, "failed to decode error")

	fullContent := append(preContentBytes, partsConcat...)
	require.Equal(t, fullContent, getResp.Body)
}
