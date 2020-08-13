package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

const (
	BlockstoreType = "gcs"

	DefaultStreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultStreamingChunkTimeout = time.Second * 1 // if we haven't read DefaultStreamingChunkSize by this duration, write whatever we have as a chunk
)

var (
	ErrGCS                     = errors.New("gcs error")
	ErrMissingETag             = fmt.Errorf("%w: missing ETag", ErrGCS)
	ErrInventoryNotImplemented = errors.New("inventory feature not implemented for gcs storage adapter")
)

func resolveNamespace(obj block.ObjectPointer) (block.QualifiedKey, error) {
	qualifiedKey, err := block.ResolveNamespace(obj.StorageNamespace, obj.Identifier)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.StorageType != block.StorageTypeS3 {
		return qualifiedKey, block.ErrInvalidNamespace
	}
	return qualifiedKey, nil
}

type Adapter struct {
	s3                    s3iface.S3API
	httpClient            *http.Client
	ctx                   context.Context
	uploadIDTranslator    block.UploadIDTranslator
	streamingChunkSize    int
	streamingChunkTimeout time.Duration
}

func WithHTTPClient(c *http.Client) func(a *Adapter) {
	return func(a *Adapter) {
		a.httpClient = c
	}
}

func WithStreamingChunkSize(sz int) func(a *Adapter) {
	return func(a *Adapter) {
		a.streamingChunkSize = sz
	}
}

func WithStreamingChunkTimeout(d time.Duration) func(a *Adapter) {
	return func(a *Adapter) {
		a.streamingChunkTimeout = d
	}
}

func WithContext(ctx context.Context) func(a *Adapter) {
	return func(a *Adapter) {
		a.ctx = ctx
	}
}

func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}

func NewAdapter(s3 s3iface.S3API, opts ...func(a *Adapter)) block.Adapter {
	a := &Adapter{
		s3:                    s3,
		httpClient:            http.DefaultClient,
		ctx:                   context.Background(),
		uploadIDTranslator:    &block.NoOpTranslator{},
		streamingChunkSize:    DefaultStreamingChunkSize,
		streamingChunkTimeout: DefaultStreamingChunkTimeout,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (s *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		s3:                    s.s3,
		httpClient:            s.httpClient,
		ctx:                   ctx,
		uploadIDTranslator:    s.uploadIDTranslator,
		streamingChunkSize:    s.streamingChunkSize,
		streamingChunkTimeout: s.streamingChunkTimeout,
	}
}

func (s *Adapter) log() logging.Logger {
	return logging.FromContext(s.ctx)
}

// work around, because put failed with trying to create symlinks
func (s *Adapter) PutWithoutStream(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	putObject := s3.PutObjectInput{
		Body:         aws.ReadSeekCloser(reader),
		Bucket:       aws.String(qualifiedKey.StorageNamespace),
		Key:          aws.String(qualifiedKey.Key),
		StorageClass: opts.StorageClass,
	}
	_, err = s.s3.PutObject(&putObject)
	return err
}

func (s *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	putObject := s3.PutObjectInput{
		Bucket:       aws.String(qualifiedKey.StorageNamespace),
		Key:          aws.String(qualifiedKey.Key),
		StorageClass: opts.StorageClass,
	}
	sdkRequest, _ := s.s3.PutObjectRequest(&putObject)
	_, err = s.streamRequestData(sdkRequest, sizeBytes, reader)
	return err
}

func (s *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	uploadID = s.uploadIDTranslator.TranslateUploadID(uploadID)
	uploadPartObject := s3.UploadPartInput{
		Bucket:     aws.String(qualifiedKey.StorageNamespace),
		Key:        aws.String(qualifiedKey.Key),
		PartNumber: aws.Int64(partNumber),
		UploadId:   aws.String(uploadID),
	}
	sdkRequest, _ := s.s3.UploadPartRequest(&uploadPartObject)
	etag, err := s.streamRequestData(sdkRequest, sizeBytes, reader)
	if err != nil {
		return "", err
	}
	if etag == "" {
		return "", ErrMissingETag
	}
	return etag, nil
}

func (s *Adapter) streamRequestData(sdkRequest *request.Request, sizeBytes int64, reader io.Reader) (string, error) {
	sigTime := time.Now()
	log := s.log().WithField("operation", "PutObject")

	if err := sdkRequest.Build(); err != nil {
		return "", err
	}

	req, err := http.NewRequest(sdkRequest.HTTPRequest.Method, sdkRequest.HTTPRequest.URL.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Encoding", StreamingContentEncoding)
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("x-amz-content-sha256", StreamingSha256)
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", sizeBytes))
	req.Header.Set("Expect", "100-Continue")

	baseSigner := v4.NewSigner(sdkRequest.Config.Credentials)

	_, err = baseSigner.Sign(req, nil, s3.ServiceName, aws.StringValue(sdkRequest.Config.Region), sigTime)
	if err != nil {
		log.WithError(err).Error("failed to sign request")
		return "", err
	}

	sigSeed, err := v4.GetSignedRequestSignature(req)
	if err != nil {
		log.WithError(err).Error("failed to get seed signature")
		return "", err
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
		ChunkSize:    s.streamingChunkSize,
		ChunkTimeout: s.streamingChunkTimeout,
	})
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.WithError(err).
			WithField("url", sdkRequest.HTTPRequest.URL.String()).
			Error("error making request request")
		return "", err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("%w: %d %s (unknown)", ErrGCS, resp.StatusCode, resp.Status)
		} else {
			err = fmt.Errorf("%w: %s", ErrGCS, body)
		}
		log.WithError(err).
			WithField("url", sdkRequest.HTTPRequest.URL.String()).
			WithField("status_code", resp.StatusCode).
			Error("bad GCS PutObject response")
		return "", err
	}
	etag := resp.Header.Get("Etag")
	// error in case etag is missing - note that empty header value will cause the same error
	if len(etag) == 0 {
		return "", ErrMissingETag
	}
	return etag, nil
}

func (s *Adapter) Get(obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	var sizeBytes int64
	defer reportMetrics("Get", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	log := s.log().WithField("operation", "GetObject")
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		log.WithError(err).Error("failed to get GCS object")
		return nil, err
	}
	sizeBytes = *objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (s *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	var sizeBytes int64
	defer reportMetrics("GetRange", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	log := s.log().WithField("operation", "GetObjectRange")
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition)),
	}
	objectOutput, err := s.s3.GetObject(&getObjectInput)
	if err != nil {
		log.WithError(err).WithFields(logging.Fields{
			"start_position": startPosition,
			"end_position":   endPosition,
		}).Error("failed to get GCS object range")
		return nil, err
	}
	sizeBytes = *objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (s *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return block.Properties{}, err
	}
	headObjectParams := &s3.HeadObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}
	s3Props, err := s.s3.HeadObject(headObjectParams)
	if err != nil {
		return block.Properties{}, err
	}
	return block.Properties{StorageClass: s3Props.StorageClass}, nil
}

func (s *Adapter) Remove(obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	deleteObjectParams := &s3.DeleteObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}
	_, err = s.s3.DeleteObject(deleteObjectParams)
	if err != nil {
		s.log().WithError(err).Error("failed to delete GCS object")
		return err
	}
	err = s.s3.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	})
	return err
}

func (s *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(qualifiedKey.StorageNamespace),
		Key:          aws.String(qualifiedKey.Key),
		ContentType:  aws.String(""),
		StorageClass: opts.StorageClass,
	}
	resp, err := s.s3.CreateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	uploadID := *resp.UploadId
	uploadID = s.uploadIDTranslator.SetUploadID(uploadID)
	s.log().WithFields(logging.Fields{
		"upload_id":            *resp.UploadId,
		"translated_upload_id": uploadID,
		"qualified_ns":         qualifiedKey.StorageNamespace,
		"qualified_key":        qualifiedKey.Key,
		"key":                  obj.Identifier,
	}).Debug("created multipart upload")
	return uploadID, err
}

func (s *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	uploadID = s.uploadIDTranslator.TranslateUploadID(uploadID)
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(qualifiedKey.StorageNamespace),
		Key:      aws.String(qualifiedKey.Key),
		UploadId: aws.String(uploadID),
	}
	_, err = s.s3.AbortMultipartUpload(input)
	s.uploadIDTranslator.RemoveUploadID(uploadID)
	s.log().WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	}).Debug("aborted multipart upload")
	return err
}

func (s *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, 0, err
	}
	cmpu := &s3.CompletedMultipartUpload{Parts: multipartList.Part}
	translatedUploadID := s.uploadIDTranslator.TranslateUploadID(uploadID)
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(qualifiedKey.StorageNamespace),
		Key:             aws.String(qualifiedKey.Key),
		UploadId:        aws.String(translatedUploadID),
		MultipartUpload: cmpu,
	}
	lg := s.log().WithFields(logging.Fields{
		"upload_id":            uploadID,
		"translated_upload_id": translatedUploadID,
		"qualified_ns":         qualifiedKey.StorageNamespace,
		"qualified_key":        qualifiedKey.Key,
		"key":                  obj.Identifier,
	})
	resp, err := s.s3.CompleteMultipartUpload(input)

	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, -1, err
	}
	lg.Debug("completed multipart upload")
	s.uploadIDTranslator.RemoveUploadID(translatedUploadID)
	headInput := &s3.HeadObjectInput{Bucket: &qualifiedKey.StorageNamespace, Key: &qualifiedKey.Key}
	headResp, err := s.s3.HeadObject(headInput)
	if err != nil {
		return nil, -1, err
	} else {
		return resp.ETag, *headResp.ContentLength, err
	}
}

func (s *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string) (block.Inventory, error) {
	return nil, ErrInventoryNotImplemented
}
