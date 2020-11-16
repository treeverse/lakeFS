package nessie

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/export"
	"github.com/treeverse/lakefs/api/gen/models"
)

func NewExternalS3Service() *s3.S3 {
	awsSession := session.Must(session.NewSession())
	return s3.New(awsSession,
		aws.NewConfig().
			WithRegion("us-east-1").
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     viper.GetString("aws_access_key_id"),
						SecretAccessKey: viper.GetString("aws_secret_access_key"),
					}})))

}

func parsePath(t testing.TB, path string) (string, string) {
	t.Helper()
	u, err := url.Parse(path)
	require.NoError(t, err, "failed to parse path")
	bucket := u.Host
	keyPath := strings.TrimLeft(u.Path, "/")
	return bucket, keyPath
}

func TestExport(t *testing.T) {
	if viper.GetString("aws_access_key_id") == "" {
		t.Skip("test export only on s3")
	}
	ctx, _, repo := setupTest(t)

	// set export configurations
	exportPath, statusPath := setExportPathForTest(ctx, t, masterBranch)

	// upload and commit
	objPath := "1.txt"
	_, objContent := uploadFileRandomData(ctx, t, repo, masterBranch, objPath)
	commitRes, err := client.Commits.Commit(
		commits.NewCommitParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(masterBranch).
			WithCommit(&models.CommitCreation{
				Message: swag.String("nessie:firstCommitForExport"),
			}), nil)
	require.NoError(t, err, "failed to commit changes")
	commit := commitRes.GetPayload()

	// run single export
	_, err = client.Export.Run(export.NewRunParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch), nil)
	require.NoError(t, err, "failed to export changes")

	// check exported file exist
	bucket, keyPath := parsePath(t, exportPath)
	key := keyPath + "/" + objPath
	s3Svc := NewExternalS3Service()
	objectOutput, err := s3GetObjectRetry(s3Svc, bucket, key)
	require.NoError(t, err, "failed to get exported file bucket:%s key:%s", bucket, key)
	body, err := ioutil.ReadAll(objectOutput.Body)
	require.NoError(t, err, "failed to read exported file")
	require.Equal(t, objContent, string(body), "unexpected content at %s", objPath)

	// check exported status file exists
	bucket, keyPath = parsePath(t, statusPath)
	statusFilename := fmt.Sprintf("%s-%s-%s", repo, masterBranch, commit.ID)
	key = keyPath + "/" + statusFilename
	_, err = s3GetObjectRetry(s3Svc, bucket, key)

	require.NoError(t, err, "failed to get exported status file bucket:%s key:%s", bucket, key)
}

func s3GetObjectRetry(s3Svc *s3.S3, bucket string, key string) (*s3.GetObjectOutput, error) {
	var err error
	var objectOutput *s3.GetObjectOutput
	count := 0
	for range time.Tick(5 * time.Second) {
		count++
		objectOutput, err = s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil || count > 5 {
			break
		}
	}
	return objectOutput, err
}
