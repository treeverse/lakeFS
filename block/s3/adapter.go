package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/request"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/block"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"

	"github.com/treeverse/lakefs/logging"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	_ "github.com/aws/aws-sdk-go/service/s3/s3iface"

	"io"
)

const (
	StreamingDefaultChunkSize = 2 << 19 // 1MiB by default per chunk
)

type Adapter struct {
	s3         s3iface.S3API
	httpClient *http.Client
	ctx        context.Context
}

type AdapterInterface interface {
	block.Adapter
	CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error)
	UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (*http.Response, error)
}

func WithHTTPClient(c *http.Client) func(a *Adapter) {
	return func(a *Adapter) {
		a.httpClient = c
	}
}

func WithContext(ctx context.Context) func(a *Adapter) {
	return func(a *Adapter) {
		a.ctx = ctx
	}
}

func NewAdapter(s3 s3iface.S3API, opts ...func(a *Adapter)) block.Adapter {
	a := &Adapter{
		s3:         s3,
		httpClient: http.DefaultClient,
		ctx:        context.Background(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (s *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		s3:         s.s3,
		httpClient: s.httpClient,
		ctx:        ctx,
	}
}

func (s *Adapter) log() logging.Logger {
	return logging.FromContext(s.ctx)
}
func (s *Adapter) Put(repo string, identifier string, sizeBytes int64, reader io.Reader) error {

	putObject := s3.PutObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)}
	sdkRequest, _ := s.s3.PutObjectRequest(&putObject)
	_, err := s.streamToS3(sdkRequest, sizeBytes, reader)
	return err
}

func (s *Adapter) UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (*http.Response, error) {
	uploadPartObject := s3.UploadPartInput{Bucket: aws.String(repo), Key: aws.String(identifier), PartNumber: aws.Int64(partNumber), UploadId: aws.String(uploadId)}
	sdkRequest, _ := s.s3.UploadPartRequest(&uploadPartObject)
	resp, err := s.streamToS3(sdkRequest, sizeBytes, reader)
	return resp, err
}

func (s *Adapter) streamToS3(sdkRequest *request.Request, sizeBytes int64, reader io.Reader) (*http.Response, error) {
	sigTime := time.Now()
	log := s.log().WithField("operation", "PutObject")

	_ = sdkRequest.Build()

	req, _ := http.NewRequest(sdkRequest.HTTPRequest.Method, sdkRequest.HTTPRequest.URL.String(), nil)
	req.Header.Set("Content-Encoding", StreamingContentEncoding)
	req.Header.Set("x-amz-content-sha256", StreamingSha256)
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", sizeBytes))
	req.Header.Set("Expect", "100-Continue")
	req.ContentLength = int64(CalculateStreamSizeForPayload(sizeBytes, StreamingDefaultChunkSize))

	baseSigner := v4.NewSigner(sdkRequest.Config.Credentials)

	_, err := baseSigner.Sign(req, nil, s3.ServiceName, aws.StringValue(sdkRequest.Config.Region), sigTime)
	if err != nil {
		log.WithError(err).Error("failed to sign request")
		return nil, err
	}

	sigSeed, err := v4.GetSignedRequestSignature(req)
	if err != nil {
		log.WithError(err).Error("failed to get seed signature")
		return nil, err
	}

	req.Body = ioutil.NopCloser(&StreamingReader{
		Reader: reader,
		Size:   int(sizeBytes),
		Time:   sigTime,
		StreamSigner: v4.NewStreamSigner(
			aws.StringValue(sdkRequest.Config.Region),
			s3.ServiceName,
			sigSeed,
			sdkRequest.Config.Credentials,
		),
		ChunkSize: StreamingDefaultChunkSize,
	})

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.WithError(err).
			WithField("url", sdkRequest.HTTPRequest.URL.String()).
			Error("error making request request")
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			body = []byte(fmt.Sprintf("s3 error: %d %s (unknown)", resp.StatusCode, resp.Status))
		}
		err = fmt.Errorf("s3 error: %s", body)
		log.WithError(err).
			WithField("url", sdkRequest.HTTPRequest.URL.String()).
			WithField("status_code", resp.StatusCode).
			Error("bad S3 PutObject response")
		return nil, err
	}
	return resp, nil
}

func (s *Adapter) Get(repo string, identifier string) (io.ReadCloser, error) {
	log := s.log().WithField("operation", "GetObject")
	getObjectInput := s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		log.WithError(err).Error("failed to get S3 object")
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s *Adapter) GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	log := s.log().WithField("operation", "GetObjectRange")
	getObjectInput := s3.GetObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier), Range: aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition))}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		log.WithError(err).WithFields(logging.Fields{
			"start_position": startPosition,
			"end_position":   endPosition,
		}).Error("failed to get S3 object range")
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s *Adapter) Remove(repo string, identifier string) error {
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

func (s *Adapter) GetAdapterType() string {
	return "s3"
}

func (s *Adapter) CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(repo),
		Key:         aws.String(identifier),
		ContentType: aws.String(""),
	}
	resp, err := s.s3.CreateMultipartUpload(input)
	return *resp.UploadId, err
}
