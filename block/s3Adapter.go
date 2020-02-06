package block

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
)

type S3Adapter struct {
	s3 *s3.S3
}

func NewS3Adapter(s3 *s3.S3) (Adapter, error) {
	return &S3Adapter{s3: s3}, nil
}

func (s S3Adapter) Put(repo string, identifier string, reader io.ReadSeeker) error {

	_, err := s.s3.PutObject(&s3.PutObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier), Body: reader})
	if err != nil {
		return err
	}
	return nil
}

func (s S3Adapter) Get(repo string, identifier string) (io.ReadCloser, error) {
	//return NewS3File(s.s3, repo, identifier), nil
	objectOutput, err := s.s3.GetObject(&s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)})
	if err != nil {
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s S3Adapter) GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	objectOutput, err := s.s3.GetObject(&s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier), Range: aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition))})
	if err != nil {
		return nil, err
	}
	return objectOutput.Body, nil
}
