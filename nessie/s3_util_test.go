package nessie

import (
	"bytes"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func putS3Object(client *s3.S3, filePath, bucket, key string) error {
	upFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer upFile.Close()

	upFileInfo, _ := upFile.Stat()
	var fileSize int64 = upFileInfo.Size()
	fileBuffer := make([]byte, fileSize)
	upFile.Read(fileBuffer)

	// Put the file object to s3 with the file name
	_, err = client.PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(fileBuffer),
		ContentLength:        aws.Int64(fileSize),
		ContentType:          aws.String(http.DetectContentType(fileBuffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})

	return err
}

func cleanS3Bucket(client *s3.S3, bucket string) error {
	listObjectsInput := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	for {
		listObjectsOutput, err := client.ListObjectsV2(listObjectsInput)
		if err != nil {
			return err
		}

		for _, record := range listObjectsOutput.Contents {
			_, err := client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    record.Key,
			})
			if err != nil {
				return err
			}
		}

		if !aws.BoolValue(listObjectsOutput.IsTruncated) {
			break
		}
		listObjectsInput.ContinuationToken = listObjectsOutput.NextContinuationToken
	}
	_, err := client.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucket)})

	return err
}
