package block

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/logging"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
)

type S3Adapter struct {
	s3       s3iface.S3API
	uploader *s3manager.Uploader
	ctx      context.Context
}

func NewS3Adapter(s3 s3iface.S3API) (Adapter, error) {
	return &S3Adapter{s3: s3,
		uploader: s3manager.NewUploaderWithClient(s3),
		ctx:      context.Background()}, nil
}

func (s S3Adapter) WithContext(ctx context.Context) Adapter {
	return &S3Adapter{
		s3:       s.s3,
		uploader: s3manager.NewUploaderWithClient(s.s3),
		ctx:      ctx,
	}
}

func (s S3Adapter) log() logging.Logger {
	return logging.FromContext(s.ctx)
}

//todo: if "aws-chunked" is not be implemented before the POC. We can significantly improve memory consumption by using "content-length" field
func (s S3Adapter) Put(repo string, identifier string, reader io.Reader) error {
	input := &s3manager.UploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(identifier),
		Body:   reader,
	}
	_, err := s.uploader.Upload(input)
	if err != nil {
		s.log().WithError(err).Error("failed to upload S3 object")
	}

	return err
}

func (s S3Adapter) Remove(repo string, identifier string) error {
	deleteObjectParams := &s3.DeleteObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)}
	_, err := s.s3.DeleteObject(deleteObjectParams)
	if err != nil {
		s.log().WithError(err).Error("failed to delete S3 object")
		return err
	}
	err = s.s3.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(repo),
		Key:    aws.String(identifier),
	})
	return err
}

func (s S3Adapter) Get(repo string, identifier string) (io.ReadCloser, error) {
	getObjectInput := s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		s.log().WithError(err).Error("failed to get S3 object")
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s S3Adapter) GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	getObjectInput := s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier), Range: aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition))}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		s.log().WithError(err).WithFields(logging.Fields{
			"start_position": startPosition,
			"end_position":   endPosition,
		}).Error("failed to get S3 object range")
		return nil, err
	}
	return objectOutput.Body, nil
}
