package esti

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api"
)

func TestMigrate(t *testing.T) {
	postMigrate := viper.GetViper().GetBool("post_migrate")
	stateFile := viper.GetViper().GetString("migrate_state_file")

	if postMigrate {
		postMigrateTests(t, stateFile)
	} else {
		preMigrateTests(t, stateFile)
	}
}

func preMigrateTests(t *testing.T, stateFile string) {
	// all pre tests execution
	t.Run("TestPreMigrateMultipart", testPreMigrateMultipart)

	// write the state file
	bytes, err := json.Marshal(&state)
	require.NoError(t, err, "marshal state")

	err = ioutil.WriteFile(stateFile, bytes, 0644)
	require.NoError(t, err, "writing state file")
}

func postMigrateTests(t *testing.T, stateFile string) {
	// read the state file
	bytes, err := ioutil.ReadFile(stateFile)
	require.NoError(t, err, "reading state file")

	err = json.Unmarshal(bytes, &state)
	require.NoError(t, err, "unmarshal state")

	// all post tests execution
	t.Run("TestPostMigrateMultipart", testPostMigrateMultipart)
}

// state to be used
var state migrateTestState

type migrateTestState struct {
	Multipart multipartState
}

type multipartState struct {
	Repo           string                         `json:"repo"`
	Info           s3.CreateMultipartUploadOutput `json:"info"`
	CompletedParts []*s3.CompletedPart            `json:"completed_parts"`
}

const (
	migrateMultipartFile     = "multipart_file"
	migrateMultipartFilepath = mainBranch + "/" + migrateMultipartFile
)

func testPreMigrateMultipart(t *testing.T) {
	_, logger, repo := setupTest(t)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(migrateMultipartFilepath),
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

	state.Multipart.Repo = repo
	state.Multipart.Info = *resp
	state.Multipart.CompletedParts = completedParts
}

func testPostMigrateMultipart(t *testing.T) {
	ctx := context.Background()

	completeResponse, err := uploadMultipartComplete(svc, &state.Multipart.Info, state.Multipart.CompletedParts)
	require.NoError(t, err, "failed to complete multipartState upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipartState request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, state.Multipart.Repo, mainBranch, &api.GetObjectParams{Path: migrateMultipartFile})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.Equal(t, multipartNumberOfParts*multipartPartSize, len(getResp.Body))
}
