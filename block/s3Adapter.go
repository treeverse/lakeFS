package block

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	_ "github.com/aws/aws-sdk-go/service/s3/s3iface"

	"io"
)

type S3Adapter struct {
	s3 s3iface.S3API
}

func NewS3Adapter(s3 s3iface.S3API) (Adapter, error) {
	return &S3Adapter{s3: s3}, nil
}

func (s S3Adapter) Put(repo string, identifier string, reader io.ReadSeeker) error {
	putObject := s3.PutObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier), Body: reader}
	_, err := s.s3.PutObject(&putObject)
	if err != nil {
		return err
	}
	return nil
}

func (s S3Adapter) Get(repo string, identifier string) (io.ReadCloser, error) {
	getObjectInput := s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s S3Adapter) GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	getObjectInput := s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier), Range: aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition))}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		return nil, err
	}
	return objectOutput.Body, nil
}
