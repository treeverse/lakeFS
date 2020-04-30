package s3

import (
	"context"
	"fmt"
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

func (s *Adapter) Put(repo string, identifier string, sizeBytes int, reader io.Reader) error {
	sigTime := time.Now()

	log := s.log().WithField("operation", "PutObject")

	putObject := s3.PutObjectInput{Bucket: aws.String(repo), Key: aws.String(identifier)}
	sdkRequest, _ := s.s3.PutObjectRequest(&putObject)
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
		return err
	}

	sigSeed, err := v4.GetSignedRequestSignature(req)
	if err != nil {
		log.WithError(err).Error("failed to get seed signature")
		return err
	}

	req.Body = ioutil.NopCloser(&StreamingReader{
		Reader: reader,
		Size:   sizeBytes,
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
		return err
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
		return err
	}
	return nil
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
