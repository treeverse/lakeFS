package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

var (
	ErrS3          = errors.New("s3 error")
	ErrMissingETag = fmt.Errorf("%w: missing ETag", ErrS3)
)

type Adapter struct {
	clients                      *ClientCache
	respServer                   string
	respServerLock               sync.Mutex
	ServerSideEncryption         string
	ServerSideEncryptionKmsKeyID string
	preSignedExpiry              time.Duration
	preSignedRefreshWindow       time.Duration
	disablePreSigned             bool
	disablePreSignedUI           bool
}

func WithStatsCollector(s stats.Collector) func(a *Adapter) {
	return func(a *Adapter) {
		a.clients.SetStatsCollector(s)
	}
}

func WithDiscoverBucketRegion(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		a.clients.DiscoverBucketRegion(b)
	}
}

func WithPreSignedRefreshWindow(v time.Duration) func(a *Adapter) {
	return func(a *Adapter) {
		a.preSignedRefreshWindow = v
	}
}

func WithPreSignedExpiry(v time.Duration) func(a *Adapter) {
	return func(a *Adapter) {
		a.preSignedExpiry = v
	}
}

func WithDisablePreSigned(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		if b {
			a.disablePreSigned = true
		}
	}
}

func WithDisablePreSignedUI(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		if b {
			a.disablePreSignedUI = true
		}
	}
}

func WithServerSideEncryption(s string) func(a *Adapter) {
	return func(a *Adapter) {
		a.ServerSideEncryption = s
	}
}

func WithServerSideEncryptionKmsKeyID(s string) func(a *Adapter) {
	return func(a *Adapter) {
		a.ServerSideEncryptionKmsKeyID = s
	}
}

type AdapterOption func(a *Adapter)

func NewAdapterFromParams(ctx context.Context, params params.S3, opts ...AdapterOption) (*Adapter, error) {
	cfg, err := LoadConfig(ctx, params)
	if err != nil {
		return nil, err
	}
	a := &Adapter{
		clients:         NewClientCache(cfg, params),
		preSignedExpiry: block.DefaultPreSignExpiryDuration,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

func NewAdapter(awsConfig aws.Config, params params.S3, opts ...AdapterOption) *Adapter {
	a := &Adapter{
		clients:         NewClientCache(awsConfig, params),
		preSignedExpiry: block.DefaultPreSignExpiryDuration,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func LoadConfig(ctx context.Context, params params.S3) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	opts = append(opts, config.WithLogger(&logging.AWSAdapter{
		Logger: logging.ContextUnavailable().WithField("sdk", "aws"),
	}))
	var logMode aws.ClientLogMode
	if params.ClientLogRetries {
		logMode |= aws.LogRetries
	}
	if params.ClientLogRequest {
		logMode |= aws.LogRequest
	}
	if logMode != 0 {
		opts = append(opts, config.WithClientLogMode(logMode))
	}
	if params.Region != "" {
		opts = append(opts, config.WithRegion(params.Region))
	}
	if params.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(params.Profile))
	}
	if params.CredentialsFile != "" {
		opts = append(opts, config.WithSharedCredentialsFiles([]string{params.CredentialsFile}))
	}
	if params.Credentials.AccessKeyID != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				params.Credentials.AccessKeyID,
				params.Credentials.SecretAccessKey,
				params.Credentials.SessionToken,
			),
		))
	}
	if params.MaxRetries > 0 {
		opts = append(opts, config.WithRetryMaxAttempts(params.MaxRetries))
	}
	if params.SkipVerifyCertificateTestOnly {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		}
		opts = append(opts, config.WithHTTPClient(&http.Client{Transport: tr}))
	}
	if params.WebIdentity != nil {
		wi := *params.WebIdentity // Copy WebIdentity: it will be used asynchronously.
		if wi.SessionDuration > 0 {
			opts = append(opts, config.WithWebIdentityRoleCredentialOptions(
				func(options *stscreds.WebIdentityRoleOptions) {
					options.Duration = wi.SessionDuration
				}),
			)
		}
		if wi.SessionExpiryWindow > 0 {
			opts = append(opts, config.WithCredentialsCacheOptions(
				func(options *aws.CredentialsCacheOptions) {
					options.ExpiryWindow = wi.SessionExpiryWindow
				}),
			)
		}
	}
	return config.LoadDefaultConfig(ctx, opts...)
}

func WithClientParams(params params.S3) func(options *s3.Options) {
	return func(options *s3.Options) {
		if params.Endpoint != "" {
			options.BaseEndpoint = aws.String(params.Endpoint)
		}
		if params.ForcePathStyle {
			options.UsePathStyle = true
		}
	}
}

func (a *Adapter) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx)
}

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)

	// for unknown size, we assume we like to stream content, will use s3manager to perform the request.
	// we assume the caller may not have 1:1 request to s3 put object in this case as it may perform multipart upload
	if sizeBytes == -1 {
		return a.managerUpload(ctx, obj, reader, opts)
	}

	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}

	putObject := s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          reader,
		ContentLength: sizeBytes,
	}
	if opts.StorageClass != nil {
		putObject.StorageClass = types.StorageClass(*opts.StorageClass)
	}
	if a.ServerSideEncryption != "" {
		putObject.ServerSideEncryption = types.ServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		putObject.SSEKMSKeyId = aws.String(a.ServerSideEncryptionKmsKeyID)
	}

	client := a.clients.Get(ctx, bucket)
	resp, err := client.PutObject(ctx, &putObject,
		s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
	if err != nil {
		return err
	}
	etag := aws.ToString(resp.ETag)
	if etag == "" {
		return ErrMissingETag
	}
	return nil
}

func (a *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}

	uploadPartInput := &s3.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		PartNumber:    int32(partNumber),
		UploadId:      aws.String(uploadID),
		Body:          reader,
		ContentLength: sizeBytes,
	}
	if a.ServerSideEncryption != "" {
		uploadPartInput.SSECustomerAlgorithm = &a.ServerSideEncryption
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		uploadPartInput.SSECustomerKey = &a.ServerSideEncryptionKmsKeyID
	}

	client := a.clients.Get(ctx, bucket)
	resp, err := client.UploadPart(ctx, uploadPartInput)
	if err != nil {
		return nil, err
	}
	etag := aws.ToString(resp.ETag)
	if etag == "" {
		return nil, ErrMissingETag
	}
	return &block.UploadPartResponse{
		ETag:             strings.Trim(etag, `"`),
		ServerSideHeader: extractSSHeaderUploadPart(resp),
	}, nil
}

func isErrNotFound(err error) bool {
	var (
		errNoSuchKey *types.NoSuchKey
		errNotFound  *types.NotFound
	)
	return errors.As(err, &errNoSuchKey) || errors.As(err, &errNotFound)
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	var sizeBytes int64
	defer reportMetrics("Get", time.Now(), &sizeBytes, &err)
	log := a.log(ctx).WithField("operation", "GetObject")
	bucket, key, qualifiedKey, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}

	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	client := a.clients.Get(ctx, bucket)
	objectOutput, err := client.GetObject(ctx, &getObjectInput)
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		log.WithError(err).Errorf("failed to get S3 object bucket %s key %s", qualifiedKey.GetStorageNamespace(), qualifiedKey.GetKey())
		return nil, err
	}
	sizeBytes = objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (a *Adapter) GetWalker(uri *url.URL) (block.Walker, error) {
	if err := block.ValidateStorageType(uri, block.StorageTypeS3); err != nil {
		return nil, err
	}
	return NewS3Walker(a.clients.GetDefault()), nil
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, time.Time, error) {
	if a.disablePreSigned {
		return "", time.Time{}, block.ErrOperationNotSupported
	}

	// TODO(barak): handle expiry window of the client credentials when pre-signed
	// support enabled
	expiry := time.Now().Add(a.preSignedExpiry)

	log := a.log(ctx).WithFields(logging.Fields{
		"operation":  "GetPreSignedURL",
		"namespace":  obj.StorageNamespace,
		"identifier": obj.Identifier,
		"ttl":        time.Until(expiry),
	})
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		log.WithError(err).Error("could not resolve namespace")
		return "", time.Time{}, err
	}

	client := a.clients.Get(ctx, bucket)
	presigner := s3.NewPresignClient(client, func(options *s3.PresignOptions) {
		options.Expires = a.preSignedExpiry
	})

	var req *v4.PresignedHTTPRequest
	if mode == block.PreSignModeWrite {
		putObjectInput := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		req, err = presigner.PresignPutObject(ctx, putObjectInput)
	} else {
		getObjectInput := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		req, err = presigner.PresignGetObject(ctx, getObjectInput)
	}
	if err != nil {
		log.WithError(err).Error("could not pre-sign request")
		return "", time.Time{}, err
	}
	return req.URL, expiry, nil
}

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)
	log := a.log(ctx).WithField("operation", "HeadObject")
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return false, err
	}

	input := s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	client := a.clients.Get(ctx, bucket)
	_, err = client.HeadObject(ctx, &input)
	if isErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		log.WithError(err).Errorf("failed to stat S3 object")
		return false, err
	}
	return true, nil
}

func (a *Adapter) GetRange(ctx context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	var sizeBytes int64
	defer reportMetrics("GetRange", time.Now(), &sizeBytes, &err)
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	log := a.log(ctx).WithField("operation", "GetObjectRange")
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition)),
	}
	client := a.clients.Get(ctx, bucket)
	objectOutput, err := client.GetObject(ctx, &getObjectInput)
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		log.WithError(err).WithFields(logging.Fields{
			"start_position": startPosition,
			"end_position":   endPosition,
		}).Error("failed to get S3 object range")
		return nil, err
	}
	sizeBytes = objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (a *Adapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return block.Properties{}, err
	}

	headObjectParams := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	client := a.clients.Get(ctx, bucket)
	s3Props, err := client.HeadObject(ctx, headObjectParams)
	if err != nil {
		return block.Properties{}, err
	}
	return block.Properties{
		StorageClass: aws.String(string(s3Props.StorageClass)),
	}, nil
}

func (a *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	client := a.clients.Get(ctx, bucket)
	_, err = client.DeleteObject(ctx, deleteInput)
	if err != nil {
		a.log(ctx).WithError(err).Error("failed to delete S3 object")
		return err
	}

	// TODO(barak): check if wait for delete is needed here
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	const maxWaitDur = 100 * time.Second
	waiter := s3.NewObjectNotExistsWaiter(client)
	return waiter.Wait(ctx, headInput, maxWaitDur)
}

func (a *Adapter) copyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, byteRange *string) (*block.UploadPartResponse, error) {
	srcKey, err := resolveNamespace(sourceObj)
	if err != nil {
		return nil, err
	}

	bucket, key, _, err := a.extractParamsFromObj(destinationObj)
	if err != nil {
		return nil, err
	}

	uploadPartCopyObject := s3.UploadPartCopyInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		PartNumber: int32(partNumber),
		UploadId:   aws.String(uploadID),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcKey.GetStorageNamespace(), srcKey.GetKey())),
	}
	if byteRange != nil {
		uploadPartCopyObject.CopySourceRange = byteRange
	}
	client := a.clients.Get(ctx, bucket)
	resp, err := client.UploadPartCopy(ctx, &uploadPartCopyObject)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.CopyPartResult == nil || resp.CopyPartResult.ETag == nil {
		return nil, ErrMissingETag
	}

	etag := strings.Trim(*resp.CopyPartResult.ETag, `"`)
	return &block.UploadPartResponse{
		ETag:             etag,
		ServerSideHeader: extractSSHeaderUploadPartCopy(resp),
	}, nil
}

// extractAmzServerSideHeaders extracts the x-amz-server-side-* headers from the response
// TODO(barak): verify that this is the correct way to extract the headers
func extractSSHeaderUploadPartCopy(resp *s3.UploadPartCopyOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSECustomerAlgorithm != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", *resp.SSECustomerAlgorithm)
	}
	if resp.SSECustomerKeyMD5 != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", *resp.SSECustomerKeyMD5)
	}
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	return headers
}

func extractSSHeaderCreateMultipartUpload(resp *s3.CreateMultipartUploadOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSECustomerAlgorithm != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", *resp.SSECustomerAlgorithm)
	}
	if resp.SSECustomerKeyMD5 != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", *resp.SSECustomerKeyMD5)
	}
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	if resp.SSEKMSEncryptionContext != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Context", *resp.SSEKMSEncryptionContext)
	}
	return headers
}

func extractSSHeaderCompleteMultipartUpload(resp *s3.CompleteMultipartUploadOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	return headers
}

func extractSSHeaderUploadPart(resp *s3.UploadPartOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSECustomerAlgorithm != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", *resp.SSECustomerAlgorithm)
	}
	if resp.SSECustomerKeyMD5 != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", *resp.SSECustomerKeyMD5)
	}
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	return headers
}

func (a *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadCopyPart", time.Now(), nil, &err)
	return a.copyPart(ctx, sourceObj, destinationObj, uploadID, partNumber, nil)
}

func (a *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadCopyPartRange", time.Now(), nil, &err)
	return a.copyPart(ctx,
		sourceObj, destinationObj, uploadID, partNumber,
		aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition)))
}

func (a *Adapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Copy", time.Now(), nil, &err)
	qualifiedSourceKey, err := resolveNamespace(sourceObj)
	if err != nil {
		return err
	}

	destBucket, destKey, _, err := a.extractParamsFromObj(destinationObj)
	if err != nil {
		return err
	}

	copyObjectInput := &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(qualifiedSourceKey.GetStorageNamespace() + "/" + qualifiedSourceKey.GetKey()),
	}
	if a.ServerSideEncryption != "" {
		copyObjectInput.ServerSideEncryption = types.ServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		copyObjectInput.SSEKMSKeyId = aws.String(a.ServerSideEncryptionKmsKeyID)
	}
	_, err = a.clients.Get(ctx, destBucket).CopyObject(ctx, copyObjectInput)
	if err != nil {
		a.log(ctx).WithError(err).Error("failed to copy S3 object")
	}
	return err
}

func (a *Adapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, _ *http.Request, opts block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	bucket, key, qualifiedKey, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(""),
	}
	if opts.StorageClass != nil {
		input.StorageClass = types.StorageClass(*opts.StorageClass)
	}
	if a.ServerSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		input.SSEKMSKeyId = &a.ServerSideEncryptionKmsKeyID
	}
	client := a.clients.Get(ctx, bucket)
	resp, err := client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, err
	}
	uploadID := aws.ToString(resp.UploadId)
	a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.GetStorageNamespace(),
		"qualified_key": qualifiedKey.GetKey(),
		"key":           obj.Identifier,
	}).Debug("created multipart upload")
	return &block.CreateMultiPartUploadResponse{
		UploadID:         uploadID,
		ServerSideHeader: extractSSHeaderCreateMultipartUpload(resp),
	}, err
}

func (a *Adapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)
	bucket, key, qualifiedKey, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}

	client := a.clients.Get(ctx, bucket)
	_, err = client.AbortMultipartUpload(ctx, input)
	lg := a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.GetStorageNamespace(),
		"qualified_key": qualifiedKey.GetKey(),
		"key":           obj.Identifier,
	})
	if err != nil {
		lg.Error("Failed to abort multipart upload")
		return err
	}
	lg.Debug("aborted multipart upload")
	return nil
}

func convertFromBlockMultipartUploadCompletion(multipartList *block.MultipartUploadCompletion) *types.CompletedMultipartUpload {
	parts := make([]types.CompletedPart, 0, len(multipartList.Part))
	for _, p := range multipartList.Part {
		parts = append(parts, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: int32(p.PartNumber),
		})
	}
	return &types.CompletedMultipartUpload{Parts: parts}
}

func (a *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	bucket, key, qualifiedKey, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: convertFromBlockMultipartUploadCompletion(multipartList),
	}
	lg := a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.GetStorageNamespace(),
		"qualified_key": qualifiedKey.GetKey(),
		"key":           obj.Identifier,
	})
	client := a.clients.Get(ctx, bucket)
	resp, err := client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, err
	}
	lg.Debug("completed multipart upload")
	headInput := &s3.HeadObjectInput{Bucket: &bucket, Key: &key}
	headResp, err := client.HeadObject(ctx, headInput)
	if err != nil {
		return nil, err
	}

	etag := strings.Trim(aws.ToString(resp.ETag), `"`)
	return &block.CompleteMultiPartUploadResponse{
		ETag:             etag,
		ContentLength:    headResp.ContentLength,
		ServerSideHeader: extractSSHeaderCompleteMultipartUpload(resp),
	}, nil
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeS3
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeS3)
	if a.disablePreSigned {
		info.PreSignSupport = false
	}
	if !(a.disablePreSignedUI || a.disablePreSigned) {
		info.PreSignSupportUI = true
	}
	return info
}

func resolveNamespace(obj block.ObjectPointer) (block.CommonQualifiedKey, error) {
	qualifiedKey, err := block.DefaultResolveNamespace(obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.GetStorageType() != block.StorageTypeS3 {
		return qualifiedKey, fmt.Errorf("expected storage type s3: %w", block.ErrInvalidAddress)
	}
	return qualifiedKey, nil
}

func (a *Adapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return block.DefaultResolveNamespace(storageNamespace, key, identifierType)
}

func (a *Adapter) RuntimeStats() map[string]string {
	a.respServerLock.Lock()
	defer a.respServerLock.Unlock()
	if a.respServer == "" {
		return nil
	}
	return map[string]string{
		"resp_server": a.respServer,
	}
}

// extractS3Server extracts the responding server from the response.
// TODO(barak): check how to extract server name from response using sdk v2
/*
func (a *Adapter) extractS3Server(resp *http.Response) {
	if resp == nil || resp.Header == nil {
		return
	}

	// Extract the responding server from the response.
	// Expected values: "S3" from AWS, "MinIO" for MinIO. Others unknown.
	server := resp.Header.Get("Server")
	if server == "" {
		return
	}

	a.respServerLock.Lock()
	defer a.respServerLock.Unlock()
	a.respServer = server
}
*/

func (a *Adapter) managerUpload(ctx context.Context, obj block.ObjectPointer, reader io.Reader, opts block.PutOpts) error {
	bucket, key, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}

	client := a.clients.Get(ctx, bucket)
	uploader := manager.NewUploader(client)
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   reader,
	}
	if opts.StorageClass != nil {
		input.StorageClass = types.StorageClass(*opts.StorageClass)
	}
	if a.ServerSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		input.SSEKMSKeyId = aws.String(a.ServerSideEncryptionKmsKeyID)
	}

	output, err := uploader.Upload(ctx, input)
	if err != nil {
		return err
	}
	if aws.ToString(output.ETag) == "" {
		return ErrMissingETag
	}
	return nil
}

func (a *Adapter) extractParamsFromObj(obj block.ObjectPointer) (string, string, block.QualifiedKey, error) {
	qk, err := a.ResolveNamespace(obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
	if err != nil {
		return "", "", nil, err
	}
	bucket, key := ExtractParamsFromQK(qk)
	return bucket, key, qk, nil
}

func ExtractParamsFromQK(qk block.QualifiedKey) (string, string) {
	bucket, prefix, _ := strings.Cut(qk.GetStorageNamespace(), "/")
	key := qk.GetKey()
	if len(prefix) > 0 { // Avoid situations where prefix is empty or "/"
		key = prefix + "/" + key
	}
	return bucket, key
}
