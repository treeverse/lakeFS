package simulator

import (
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/treeverse/lakefs/logging"
)

const EtagExtension = "etag"

type externalRecordDownloader struct {
	downloader *s3manager.Downloader
}

func NewExternalRecordDownloader(region string) *externalRecordDownloader {
	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.AnonymousCredentials,
	}))

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	return &externalRecordDownloader{downloader}
}

func getEtagFileName(path string) string {
	return path + "." + EtagExtension
}

func getLocalEtag(path string) (string, error) {
	//if etag exists return
	etagFileName := getEtagFileName(path)
	etag, err := ioutil.ReadFile(etagFileName)
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

	headObject, err := d.downloader.S3.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	s3Etag := aws.StringValue(headObject.ETag)
	if s3Etag == etag {
		return nil
	}

	logging.Default().WithFields(logging.Fields{"bucket": bucket, "key": key, "destination": destination}).Info("download Recording")
	f, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	// Write the contents of S3 Object to the file
	n, err := d.downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	logging.Default().WithFields(logging.Fields{"file": destination, "bytes": n}).Info("file downloaded")

	//write the etag file
	etagFileName := getEtagFileName(destination)
	err = ioutil.WriteFile(etagFileName, []byte(s3Etag), 0644) //nolint:gosec
	return err
}
