package simulator

import (
	"context"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/treeverse/lakefs/pkg/logging"
)

const EtagExtension = "etag"

type externalRecordDownloader struct {
	client     *s3.Client
	downloader *s3manager.Downloader
}

func NewExternalRecordDownloader(region string) *externalRecordDownloader {
	client := s3.New(s3.Options{Region: region})

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(client)

	return &externalRecordDownloader{client, downloader}
}

func getEtagFileName(path string) string {
	return path + "." + EtagExtension
}

func getLocalEtag(path string) (string, error) {
	// if etag exists return
	etagFileName := getEtagFileName(path)
	etag, err := os.ReadFile(etagFileName)
	if err == nil {
		return string(etag), nil
	}
	if os.IsNotExist(err) {
		return "", nil
	}
	return "", err
}

func (d *externalRecordDownloader) DownloadRecording(bucket, key, destination string) error {
	etag, err := getLocalEtag(destination)
	if err != nil {
		return err
	}

	headObject, err := d.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	s3Etag := ""
	if headObject.ETag != nil {
		s3Etag = *headObject.ETag
	}
	if s3Etag == etag {
		return nil
	}

	logging.Default().WithFields(logging.Fields{"bucket": bucket, "key": key, "destination": destination}).Info("download Recording")
	// make sure target folder exists
	dir := filepath.Dir(destination)
	_ = os.MkdirAll(dir, os.ModePerm)
	// create file
	f, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	// Write the contents of S3 Object to the file
	n, err := d.downloader.Download(context.Background(), f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	logging.Default().WithFields(logging.Fields{"file": destination, "bytes": n}).Info("file downloaded")

	// write the etag file
	etagFileName := getEtagFileName(destination)
	err = os.WriteFile(etagFileName, []byte(s3Etag), 0644) //nolint:gosec
	return err
}
