package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

const (
	DefaultStreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultStreamingChunkTimeout = time.Second * 1 // if we haven't read DefaultStreamingChunkSize by this duration, write whatever we have as a chunk

	// Chunks smaller than that are only allowed for the last chunk upload
	minChunkSize = 8 * 1024
)

var (
	ErrS3          = errors.New("s3 error")
	ErrMissingETag = fmt.Errorf("%w: missing ETag", ErrS3)
)

func resolveNamespace(obj block.ObjectPointer) (block.QualifiedKey, error) {
	qualifiedKey, err := block.ResolveNamespace(obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.StorageType != block.StorageTypeS3 {
		return qualifiedKey, fmt.Errorf("expected storage type s3: %w", block.ErrInvalidNamespace)
	}
	return qualifiedKey, nil
}

func resolveNamespacePrefix(opts block.WalkOpts) (block.QualifiedPrefix, error) {
	qualifiedPrefix, err := block.ResolveNamespacePrefix(opts.StorageNamespace, opts.Prefix)
	if err != nil {
		return qualifiedPrefix, err
	}
	if qualifiedPrefix.StorageType != block.StorageTypeS3 {
		return qualifiedPrefix, block.ErrInvalidNamespace
	}
	return qualifiedPrefix, nil
}

type Adapter struct {
	clients                      *ClientCache
	httpClient                   *http.Client
	streamingChunkSize           int
	streamingChunkTimeout        time.Duration
	respServer                   string
	respServerLock               sync.Mutex
	ServerSideEncryption         string
	ServerSideEncryptionKmsKeyID string
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

func NewAdapter(awsSession *session.Session, opts ...AdapterOption) *Adapter {
	a := &Adapter{
		clients:               NewClientCache(awsSession),
		httpClient:            awsSession.Config.HTTPClient,
		streamingChunkSize:    DefaultStreamingChunkSize,
		streamingChunkTimeout: DefaultStreamingChunkTimeout,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Adapter) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx)
}

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}

	// for unknown size we assume we like to stream content, will use s3manager to perform the request.
	// we assume the caller may not have 1:1 request to s3 put object in this case as it may perform multipart upload
	if sizeBytes == -1 {
		return a.managerUpload(ctx, qualifiedKey, reader, opts)
	}

	putObject := s3.PutObjectInput{
		Bucket:       aws.String(qualifiedKey.StorageNamespace),
		Key:          aws.String(qualifiedKey.Key),
		StorageClass: opts.StorageClass,
	}
	if a.ServerSideEncryption != "" {
		putObject.SetServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		putObject.SetSSEKMSKeyId(a.ServerSideEncryptionKmsKeyID)
	}

	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	sdkRequest, _ := client.PutObjectRequest(&putObject)
	headers, err := a.streamToS3(ctx, sdkRequest, sizeBytes, reader)
	if err != nil {
		return err
	}
	etag := headers.Get("ETag")
	if etag == "" {
		return ErrMissingETag
	}
	return err
}

func (a *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	uploadPartObject := s3.UploadPartInput{
		Bucket:     aws.String(qualifiedKey.StorageNamespace),
		Key:        aws.String(qualifiedKey.Key),
		PartNumber: aws.Int64(int64(partNumber)),
		UploadId:   aws.String(uploadID),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	sdkRequest, _ := client.UploadPartRequest(&uploadPartObject)
	headers, err := a.streamToS3(ctx, sdkRequest, sizeBytes, reader)
	if err != nil {
		return nil, err
	}
	etag := headers.Get("ETag")
	if etag == "" {
		return nil, ErrMissingETag
	}
	return &block.UploadPartResponse{
		ETag:             strings.Trim(etag, `"`),
		ServerSideHeader: extractAmzServerSideHeader(headers),
	}, nil
}

func (a *Adapter) streamToS3(ctx context.Context, sdkRequest *request.Request, sizeBytes int64, reader io.Reader) (http.Header, error) {
	sigTime := time.Now()
	log := a.log(ctx).WithField("operation", "PutObject")

	if err := sdkRequest.Build(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest(sdkRequest.HTTPRequest.Method, sdkRequest.HTTPRequest.URL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Encoding", StreamingContentEncoding)
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("x-amz-content-sha256", StreamingSha256)
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", sizeBytes))
	if a.ServerSideEncryption != "" {
		req.Header.Set("x-amz-server-side-encryption", a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		req.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", a.ServerSideEncryptionKmsKeyID)
	}
	req = req.WithContext(ctx)

	baseSigner := v4.NewSigner(sdkRequest.Config.Credentials)
	baseSigner.DisableURIPathEscaping = true
	_, err = baseSigner.Sign(req, nil, s3.ServiceName, aws.StringValue(sdkRequest.Config.Region), sigTime)
	if err != nil {
		log.WithError(err).Error("failed to sign request")
		return nil, err
	}
	req.Header.Set("Expect", "100-Continue")

	sigSeed, err := v4.GetSignedRequestSignature(req)
	if err != nil {
		log.WithError(err).Error("failed to get seed signature")
		return nil, err
	}

	req.Body = io.NopCloser(&StreamingReader{
		Reader: reader,
		Size:   sizeBytes,
		Time:   sigTime,
		StreamSigner: v4.NewStreamSigner(
			aws.StringValue(sdkRequest.Config.Region),
			s3.ServiceName,
			sigSeed,
			sdkRequest.Config.Credentials,
		),
		ChunkSize:    a.streamingChunkSize,
		ChunkTimeout: a.streamingChunkTimeout,
	})
	resp, err := a.httpClient.Do(req)
	if err != nil {
		log.WithError(err).
			WithField("url", sdkRequest.HTTPRequest.URL.String()).
			Error("error making request")
		return nil, err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("%w: %d %s (unknown)", ErrS3, resp.StatusCode, resp.Status)
		} else {
			err = fmt.Errorf("%w: %s", ErrS3, body)
		}
		log.WithError(err).
			WithField("url", sdkRequest.HTTPRequest.URL.String()).
			WithField("status_code", resp.StatusCode).
			Error("bad S3 PutObject response")
		return nil, err
	}

	a.extractS3Server(resp)
	return resp.Header, nil
}

func isErrNotFound(err error) bool {
	var reqErr awserr.RequestFailure
	return errors.As(err, &reqErr) && reqErr.StatusCode() == http.StatusNotFound
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	var sizeBytes int64
	defer reportMetrics("Get", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	log := a.log(ctx).WithField("operation", "GetObject")
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}

	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	objectOutput, err := client.GetObjectWithContext(ctx, &getObjectInput)
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		log.WithError(err).Errorf("failed to get S3 object bucket %s key %s", qualifiedKey.StorageNamespace, qualifiedKey.Key)
		return nil, err
	}
	sizeBytes = *objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, error) {
	log := a.log(ctx).WithField("operation", "GetPresignedURL")
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		log.WithField("namespace", obj.StorageNamespace).
			WithField("identifier", obj.Identifier).
			WithError(err).Error("could not resolve namespace")
		return "", err
	}
	var preSignedURL string
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	if mode == block.PreSignModeWrite {
		putObjectInput := &s3.PutObjectInput{
			Bucket: aws.String(qualifiedKey.StorageNamespace),
			Key:    aws.String(qualifiedKey.Key),
		}
		req, _ := client.PutObjectRequest(putObjectInput)
		preSignedURL, err = req.Presign(block.DefaultPreSignExpiryDuration)
	} else {
		getObjectInput := &s3.GetObjectInput{
			Bucket: aws.String(qualifiedKey.StorageNamespace),
			Key:    aws.String(qualifiedKey.Key),
		}
		req, _ := client.GetObjectRequest(getObjectInput)
		preSignedURL, err = req.Presign(block.DefaultPreSignExpiryDuration)
	}
	if err != nil {
		log.WithField("namespace", obj.StorageNamespace).
			WithField("identifier", obj.Identifier).
			WithError(err).Error("could not pre-sign request")
	}
	return preSignedURL, err
}

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return false, err
	}
	log := a.log(ctx).WithField("operation", "HeadObject")
	input := s3.HeadObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	_, err = client.HeadObjectWithContext(ctx, &input)
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
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	log := a.log(ctx).WithField("operation", "GetObjectRange")
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition)),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	objectOutput, err := client.GetObjectWithContext(ctx, &getObjectInput)
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
	sizeBytes = *objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (a *Adapter) Walk(ctx context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	log := a.log(ctx).WithField("operation", "Walk")
	var err error
	var lenRes int64
	defer reportMetrics("Walk", time.Now(), &lenRes, &err)

	qualifiedPrefix, err := resolveNamespacePrefix(walkOpt)
	if err != nil {
		return err
	}

	listObjectInput := s3.ListObjectsInput{
		Bucket: aws.String(qualifiedPrefix.StorageNamespace),
		Prefix: aws.String(qualifiedPrefix.Prefix),
	}

	for {
		listOutput, err := a.clients.Get(ctx, qualifiedPrefix.StorageNamespace).ListObjectsWithContext(ctx, &listObjectInput)
		if err != nil {
			log.WithError(err).WithFields(logging.Fields{
				"bucket": qualifiedPrefix.StorageNamespace,
				"prefix": qualifiedPrefix.Prefix,
			}).Error("failed to list S3 objects")
			return err
		}

		for _, obj := range listOutput.Contents {
			if err := walkFn(*obj.Key); err != nil {
				return err
			}
		}

		if listOutput.IsTruncated == nil || !*listOutput.IsTruncated {
			break
		}

		// start with the next marker
		listObjectInput.Marker = listOutput.NextMarker
	}

	return nil
}

func (a *Adapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
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
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	s3Props, err := client.HeadObjectWithContext(ctx, headObjectParams)
	if err != nil {
		return block.Properties{}, err
	}
	return block.Properties{
		StorageClass: s3Props.StorageClass,
		Checksum:     s3Props.ETag,
		SizeBytes:    *s3Props.ContentLength,
	}, nil
}

func (a *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
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
	svc := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	_, err = svc.DeleteObjectWithContext(ctx, deleteObjectParams)
	if err != nil {
		a.log(ctx).WithError(err).Error("failed to delete S3 object")
		return err
	}
	err = svc.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	})
	return err
}

func (a *Adapter) copyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, byteRange *string) (*block.UploadPartResponse, error) {
	qualifiedKey, err := resolveNamespace(destinationObj)
	if err != nil {
		return nil, err
	}
	srcKey, err := resolveNamespace(sourceObj)
	if err != nil {
		return nil, err
	}

	uploadPartCopyObject := s3.UploadPartCopyInput{
		Bucket:     aws.String(qualifiedKey.StorageNamespace),
		Key:        aws.String(qualifiedKey.Key),
		PartNumber: aws.Int64(int64(partNumber)),
		UploadId:   aws.String(uploadID),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcKey.StorageNamespace, srcKey.Key)),
	}
	if byteRange != nil {
		uploadPartCopyObject.CopySourceRange = byteRange
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	req, resp := client.UploadPartCopyRequest(&uploadPartCopyObject)
	req.SetContext(ctx)
	err = req.Send()
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.CopyPartResult == nil || resp.CopyPartResult.ETag == nil {
		return nil, ErrMissingETag
	}
	etag := strings.Trim(*resp.CopyPartResult.ETag, `"`)
	// x-amz-server-side-* headers
	headers := make(http.Header)
	for k, v := range req.HTTPResponse.Header {
		if strings.HasPrefix(k, "X-Amz-Server-Side-") {
			headers[k] = v
		}
	}
	return &block.UploadPartResponse{
		ETag:             etag,
		ServerSideHeader: headers,
	}, nil
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

	qualifiedDestinationKey, err := resolveNamespace(destinationObj)
	if err != nil {
		return err
	}
	qualifiedSourceKey, err := resolveNamespace(sourceObj)
	if err != nil {
		return err
	}
	copyObjectParams := &s3.CopyObjectInput{
		Bucket:     aws.String(qualifiedDestinationKey.StorageNamespace),
		Key:        aws.String(qualifiedDestinationKey.Key),
		CopySource: aws.String(qualifiedSourceKey.StorageNamespace + "/" + qualifiedSourceKey.Key),
	}
	if a.ServerSideEncryption != "" {
		copyObjectParams.SetServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		copyObjectParams.SetSSEKMSKeyId(a.ServerSideEncryptionKmsKeyID)
	}
	_, err = a.clients.Get(ctx, qualifiedDestinationKey.StorageNamespace).CopyObjectWithContext(ctx, copyObjectParams)
	if err != nil {
		a.log(ctx).WithError(err).Error("failed to copy S3 object")
	}
	return err
}

func (a *Adapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, _ *http.Request, opts block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(qualifiedKey.StorageNamespace),
		Key:          aws.String(qualifiedKey.Key),
		ContentType:  aws.String(""),
		StorageClass: opts.StorageClass,
	}
	if a.ServerSideEncryption != "" {
		input.SetServerSideEncryption(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		input.SetSSEKMSKeyId(a.ServerSideEncryptionKmsKeyID)
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	req, resp := client.CreateMultipartUploadRequest(input)
	req.SetContext(ctx)
	err = req.Send()
	if err != nil {
		return nil, err
	}
	uploadID := *resp.UploadId
	a.log(ctx).WithFields(logging.Fields{
		"upload_id":     *resp.UploadId,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	}).Debug("created multipart upload")
	return &block.CreateMultiPartUploadResponse{
		UploadID:         uploadID,
		ServerSideHeader: extractAmzServerSideHeader(req.HTTPResponse.Header),
	}, err
}

func (a *Adapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(qualifiedKey.StorageNamespace),
		Key:      aws.String(qualifiedKey.Key),
		UploadId: aws.String(uploadID),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	_, err = client.AbortMultipartUploadWithContext(ctx, input)
	a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	}).Debug("aborted multipart upload")
	return err
}

func convertFromBlockMultipartUploadCompletion(multipartList *block.MultipartUploadCompletion) *s3.CompletedMultipartUpload {
	parts := make([]*s3.CompletedPart, len(multipartList.Part))
	for i, p := range multipartList.Part {
		parts[i] = &s3.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int64(int64(p.PartNumber)),
		}
	}
	return &s3.CompletedMultipartUpload{Parts: parts}
}

func (a *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(qualifiedKey.StorageNamespace),
		Key:             aws.String(qualifiedKey.Key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: convertFromBlockMultipartUploadCompletion(multipartList),
	}
	lg := a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	})
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	req, resp := client.CompleteMultipartUploadRequest(input)
	req.SetContext(ctx)
	err = req.Send()
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, err
	}
	lg.Debug("completed multipart upload")
	headInput := &s3.HeadObjectInput{Bucket: &qualifiedKey.StorageNamespace, Key: &qualifiedKey.Key}
	headResp, err := client.HeadObjectWithContext(ctx, headInput)
	if err != nil {
		return nil, err
	}

	etag := strings.Trim(aws.StringValue(resp.ETag), `"`)
	contentLength := aws.Int64Value(headResp.ContentLength)
	return &block.CompleteMultiPartUploadResponse{
		ETag:             etag,
		ContentLength:    contentLength,
		ServerSideHeader: extractAmzServerSideHeader(req.HTTPResponse.Header),
	}, nil
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeS3
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	return block.DefaultStorageNamespaceInfo(block.BlockstoreTypeS3)
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

func (a *Adapter) managerUpload(ctx context.Context, qualifiedKey block.QualifiedKey, reader io.Reader, opts block.PutOpts) error {
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	uploader := s3manager.NewUploaderWithClient(client)

	input := &s3manager.UploadInput{
		Bucket:       aws.String(qualifiedKey.StorageNamespace),
		Key:          aws.String(qualifiedKey.Key),
		Body:         reader,
		StorageClass: opts.StorageClass,
	}
	if a.ServerSideEncryption != "" {
		input.ServerSideEncryption = aws.String(a.ServerSideEncryption)
	}
	if a.ServerSideEncryptionKmsKeyID != "" {
		input.SSEKMSKeyId = aws.String(a.ServerSideEncryptionKmsKeyID)
	}

	output, err := uploader.UploadWithContext(ctx, input)
	if err != nil {
		return err
	}
	if aws.StringValue(output.ETag) == "" {
		return ErrMissingETag
	}
	return nil
}

func extractAmzServerSideHeader(header http.Header) http.Header {
	// return additional headers: x-amz-server-side-*
	h := make(http.Header)
	for k, v := range header {
		if strings.HasPrefix(k, "X-Amz-Server-Side-") {
			h[k] = v
		}
	}
	return h
}
