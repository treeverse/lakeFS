package s3

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"

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
	ExpireObjectS3Tag         = "lakefs_expire_object"
)

func getMapKeys(m map[string]interface{}) []string {
	ret := make([]string, len(m))
	i := 0
	for k := range m {
		ret[i] = k
		i += 1
	}
	return ret
}

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
	s3                 s3iface.S3API
	httpClient         *http.Client
	ctx                context.Context
	uploadIdTranslator block.UploadIdTranslator
	streamingChunkSize int
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

func WithContext(ctx context.Context) func(a *Adapter) {
	return func(a *Adapter) {
		a.ctx = ctx
	}
}

func WithTranslator(t block.UploadIdTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIdTranslator = t
	}
}

func NewAdapter(s3 s3iface.S3API, opts ...func(a *Adapter)) block.Adapter {
	a := &Adapter{
		s3:                 s3,
		httpClient:         http.DefaultClient,
		ctx:                context.Background(),
		uploadIdTranslator: &block.NoOpTranslator{},
		streamingChunkSize: StreamingDefaultChunkSize,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (s *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		s3:                 s.s3,
		httpClient:         s.httpClient,
		ctx:                ctx,
		uploadIdTranslator: s.uploadIdTranslator,
		streamingChunkSize: s.streamingChunkSize,
	}
}

func (s *Adapter) log() logging.Logger {
	return logging.FromContext(s.ctx)
}

func GetScheme(key string) string {
	parsed, err := url.Parse(key)
	if err != nil {
		return ""
	}
	return parsed.Scheme
}

func (s *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
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
	_, err = s.streamToS3(sdkRequest, sizeBytes, reader)
	return err
}

func (s *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error) {
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	uploadId = s.uploadIdTranslator.TranslateUploadId(uploadId)
	uploadPartObject := s3.UploadPartInput{
		Bucket:     aws.String(qualifiedKey.StorageNamespace),
		Key:        aws.String(qualifiedKey.Key),
		PartNumber: aws.Int64(partNumber),
		UploadId:   aws.String(uploadId),
	}
	sdkRequest, _ := s.s3.UploadPartRequest(&uploadPartObject)
	resp, err := s.streamToS3(sdkRequest, sizeBytes, reader)
	if err == nil && resp != nil {
		etagList, ok := resp.Header["Etag"]
		if ok && len(etagList) > 0 {
			return etagList[0], nil
		}
	}
	if err == nil {
		err = fmt.Errorf("Etag not returned")
	}
	return "", err
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
	req.ContentLength = int64(CalculateStreamSizeForPayload(sizeBytes, s.streamingChunkSize))

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
		ChunkSize: s.streamingChunkSize,
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

func (s *Adapter) Get(obj block.ObjectPointer) (io.ReadCloser, error) {
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
		log.WithError(err).Error("failed to get S3 object")
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
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
		}).Error("failed to get S3 object range")
		return nil, err
	}
	return objectOutput.Body, nil
}

func (s *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
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
		s.log().WithError(err).Error("failed to delete S3 object")
		return err
	}
	err = s.s3.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(obj.StorageNamespace),
		Key:    aws.String(obj.Identifier),
	})
	return err
}

func (s *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(obj.StorageNamespace),
		Key:          aws.String(obj.Identifier),
		ContentType:  aws.String(""),
		StorageClass: opts.StorageClass,
	}
	resp, err := s.s3.CreateMultipartUpload(input)
	if err == nil {
		uploadId := *resp.UploadId
		uploadId = s.uploadIdTranslator.SetUploadId(uploadId)
		return uploadId, err
	} else {
		return "", err
	}
}
func (s *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadId string) error {
	uploadId = s.uploadIdTranslator.TranslateUploadId(uploadId)
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(obj.StorageNamespace),
		Key:      aws.String(obj.Identifier),
		UploadId: aws.String(uploadId),
	}
	_, err := s.s3.AbortMultipartUpload(input)
	s.uploadIdTranslator.RemoveUploadId(uploadId)
	return err
}

func (s *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadId string, MultipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	cmpu := &s3.CompletedMultipartUpload{Parts: MultipartList.Part}
	uploadId = s.uploadIdTranslator.TranslateUploadId(uploadId)
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(obj.StorageNamespace),
		Key:             aws.String(obj.Identifier),
		UploadId:        aws.String(uploadId),
		MultipartUpload: cmpu,
	}
	resp, err := s.s3.CompleteMultipartUpload(input)
	if err == nil {
		s.uploadIdTranslator.RemoveUploadId(uploadId)
		headInput := &s3.HeadObjectInput{Bucket: &obj.StorageNamespace, Key: &obj.Identifier}
		headResp, err := s.s3.HeadObject(headInput)
		if err != nil {
			return nil, -1, err
		} else {
			return resp.ETag, *headResp.ContentLength, err
		}
	} else {
		return nil, -1, err
	}
}

func contains(tags []*s3.Tag, pred func(string, string) bool) bool {
	for _, tag := range tags {
		if pred(*tag.Key, *tag.Value) {
			return true
		}
	}
	return false
}

func isExpirationRule(rule s3.LifecycleRule) bool {
	return rule.Expiration != nil && // Check for *any* expiration -- not its details.
		rule.Status != nil && *rule.Status == "Enabled" &&
		rule.Filter != nil &&
		rule.Filter.Tag != nil &&
		*rule.Filter.Tag.Key == ExpireObjectS3Tag &&
		*rule.Filter.Tag.Value == "1" ||
		rule.Filter.And != nil &&
			contains(rule.Filter.And.Tags,
				func(key string, value string) bool {
					return key == ExpireObjectS3Tag && value == "1"
				},
			)
}

func anyRule(array []*s3.LifecycleRule, pred func(rule s3.LifecycleRule) bool) bool {
	for _, a := range array {
		if pred(*a) {
			return true
		}
	}
	return false
}

// ValidateConfiguration on an S3 adapter checks for a usable bucket
// lifecycle policy: the storageNamespace bucket should expire objects
// marked with ExpireObjectS3Tag (with _some_ duration, even if
// nonzero).
func (s *Adapter) ValidateConfiguration(storageNamespace string) error {
	getLifecycleConfigInput := &s3.GetBucketLifecycleConfigurationInput{Bucket: &storageNamespace}
	config, err := s.s3.GetBucketLifecycleConfiguration(getLifecycleConfigInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NoSuchLifecycleConfiguration" {
			return fmt.Errorf("Bucket %s has no lifecycle configuration", storageNamespace)			
		}
		return err
	}
	// TODO(oz): Is this too chatty for a command?
	s.log().WithFields(logging.Fields{
		"Bucket":          storageNamespace,
		"LifecyclePolicy": config.GoString(),
	}).Info("S3 bucket lifecycle policy")

	if !anyRule(config.Rules, isExpirationRule) {
		// TODO(oz): Add a "to fix, ..." message?
		return fmt.Errorf("Bucket %s lifecycle rules not configured to expire objects tagged \"%s\"", storageNamespace, ExpireObjectS3Tag)
	}
	return nil
}
