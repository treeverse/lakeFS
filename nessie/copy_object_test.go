package nessie

import (
	"bytes"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestS3CopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	destRepo := createRepositoryByName(ctx, t, "tests3copyobjectdest")
	rand := rand.New(rand.NewSource(17))

	Content := testutil.RandomString(rand, randomDataContentLength)
	SourcePath := prefix + "source-file"
	DestPath := prefix + "dest-file"
	SourcePathWithRepo := repo + "/" + prefix + "source-file"

	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(repo),
		Key:    aws.String(SourcePath),
		Body:   bytes.NewReader([]byte(Content)),
	})

	if err != nil {
		t.Errorf("s3.Client.PutObject(%s): %s", SourcePath, err)
	}

	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(repo),
		CopySource: aws.String(SourcePathWithRepo),
		Key:        aws.String(DestPath)})

	if err != nil {
		t.Errorf("s3.Client.CopyObjectFrom(%s)To(%s): %s", SourcePath, DestPath, err)
	}

	download, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(repo),
		Key:    aws.String(DestPath),
	})

	if err != nil {
		t.Fatalf("s3.Client.GetObject(%s): %s", DestPath, err)
	}
	// compere files content
	contents := bytes.NewBuffer(nil)
	_, err = io.Copy(contents, download.Body)
	if err != nil {
		t.Errorf("download %s: %s", DestPath, err)
	}
	if strings.Compare(contents.String(), Content) != 0 {
		t.Errorf(
			"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), Content)
	}

	resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", SourcePath, err)
	}
	require.Equal(t, http.StatusOK, resp.StatusCode())
	sourceObjectStats := resp.JSON200

	resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", DestPath, err)
	}
	require.Equal(t, http.StatusOK, resp.StatusCode())
	destObjectStats := resp.JSON200

	// assert that the physical addresses of the objects are the same
	if strings.Compare(sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress) != 0 {
		t.Errorf(
			"Source object address: %s Source destination address: %s", sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	}

	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(destRepo),
		CopySource: aws.String(SourcePathWithRepo),
		Key:        aws.String(DestPath)})

	if err != nil {
		t.Errorf("s3.Client.CopyObjectFrom(%s)To(%s): %s", SourcePath, DestPath, err)
	}

	download, err = svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(destRepo),
		Key:    aws.String(DestPath),
	})
	if err != nil {
		t.Errorf("s3.Client.GetObject(%s): %s", DestPath, err)
	}
	contents = bytes.NewBuffer(nil)
	_, err = io.Copy(contents, download.Body)
	if err != nil {
		t.Fatalf("download %s: %s", DestPath, err)
	}
	// compere files content
	if strings.Compare(contents.String(), Content) != 0 {
		t.Errorf(
			"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), Content)
	}

	resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", SourcePath, err)
	}
	require.Equal(t, http.StatusOK, resp.StatusCode())
	sourceObjectStats = resp.JSON200

	resp, err = client.StatObjectWithResponse(ctx, destRepo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", DestPath, err)
	}
	require.Equal(t, http.StatusOK, resp.StatusCode())
	destObjectStats = resp.JSON200

	// assert that the physical addresses of the objects are not the same
	if strings.Compare(sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress) == 0 {
		t.Errorf(
			"Source object address: %s Source destination address: %s", sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	}
}
