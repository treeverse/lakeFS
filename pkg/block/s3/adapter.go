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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

const (
	DefaultStreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultStreamingChunkTimeout = time.Second * 1 // if we haven't read DefaultStreamingChunkSize by this duration, write whatever we have as a chunk

	// Time to Wait() for S3 operations
	S3WaitDur = 1 * time.Minute

	ExpireObjectS3Tag      = "lakefs_expire_object"
	ServerSideHeaderPrefix = "X-Amz-Server-Side-"
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
	clients               *ClientCache
	httpClient            *http.Client
	uploadIDTranslator    block.UploadIDTranslator
	streamingChunkSize    int
	streamingChunkTimeout time.Duration
	respServer            string
	respServerLock        sync.Mutex
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

func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
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

func NewAdapter(params *params.AWSParams, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		clients:               NewClientCache(params),
		httpClient:            http.DefaultClient,
		uploadIDTranslator:    &block.NoOpTranslator{},
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

func qkFields(qualifiedKey block.QualifiedKey) logging.Fields {
	return logging.Fields{
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
	}
}

func qpFields(qualifiedPrefix block.QualifiedPrefix) logging.Fields {
	return logging.Fields{
		"qualified_ns":     qualifiedPrefix.StorageNamespace,
		"qualified_prefix": qualifiedPrefix.Prefix,
	}
}

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	putIn := s3.PutObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
		// PutObject will put this on the Content-Length header,
		// which disables length calculation by the
		// ComputeContentLength middleware.
		ContentLength: sizeBytes,
		Body:          reader,

		// TODO(ariels): Verify SigV4 signed correctly for these streams.

		// TODO(ariels): If we have an MD5 from the user, pass it to S3 to verify.
	}
	if opts.StorageClass != nil {
		putIn.StorageClass = types.StorageClass(*opts.StorageClass)
	}

	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	// TODO(ariels): Could just set region from a bucket->region cache as an extra Opt arg.
	putOut, err := client.PutObject(ctx, &putIn)
	if err != nil {
		return err
	}
	etag := putOut.ETag
	if etag == nil || *etag == "" {
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
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	uploadPartIn := s3.UploadPartInput{
		Bucket:     aws.String(qualifiedKey.StorageNamespace),
		Key:        aws.String(qualifiedKey.Key),
		PartNumber: int32(partNumber),
		UploadId:   aws.String(uploadID),
		Body:       reader,
	}

	// TODO(ariels): Could just set region from a bucket->region cache as an extra Opt arg.
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)

	httpClient := recordHeadersClient{Prefix: ServerSideHeaderPrefix}
	uploadPartOut, err := client.UploadPart(ctx, &uploadPartIn, httpClient.WrapClient())
	if err != nil {
		return nil, err
	}
	etag := uploadPartOut.ETag
	if etag == nil || *etag == "" {
		return nil, ErrMissingETag
	}

	return &block.UploadPartResponse{
		ETag:             strings.Trim(*etag, `"`),
		ServerSideHeader: httpClient.Metadata,
	}, nil
}

func isErrNoSuchKey(err error) bool {
	var nsk *types.NoSuchKey
	return errors.As(err, &nsk)
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	var sizeBytes int64
	defer reportMetrics("Get", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	objectOutput, err := client.GetObject(ctx, &getObjectInput)
	if isErrNoSuchKey(err) {
		return nil, adapter.ErrDataNotFound
	}
	if err != nil {
		log := a.log(ctx).WithField("operation", "GetObject")
		log.WithError(err).WithFields(qkFields(qualifiedKey)).Error("failed to get S3 object")
		return nil, err
	}
	sizeBytes = objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return false, err
	}
	input := s3.HeadObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	_, err = client.HeadObject(ctx, &input)
	if isErrNoSuchKey(err) {
		return false, nil
	}
	if err != nil {
		log := a.log(ctx).WithField("operation", "HeadObject")
		log.WithError(err).WithFields(qkFields(qualifiedKey)).Error("failed to stat S3 object")
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
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startPosition, endPosition)),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	objectOutput, err := client.GetObject(ctx, &getObjectInput)
	if isErrNoSuchKey(err) {
		return nil, adapter.ErrDataNotFound
	}
	if err != nil {
		log := a.log(ctx).WithField("operation", "GetRange")
		log.WithError(err).WithFields(qkFields(qualifiedKey)).WithFields(logging.Fields{
			"start_position": startPosition,
			"end_position":   endPosition,
		}).Error("failed to get S3 object range")
		return nil, err
	}
	sizeBytes = objectOutput.ContentLength
	return objectOutput.Body, nil
}

func (a *Adapter) Walk(ctx context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	var err error
	var lenRes int64
	defer reportMetrics("Walk", time.Now(), &lenRes, &err)

	qualifiedPrefix, err := resolveNamespacePrefix(walkOpt)
	if err != nil {
		return err
	}

	log := a.log(ctx).WithField("operation", "Walk").WithFields(qpFields(qualifiedPrefix))

	listObjectInput := s3.ListObjectsInput{
		Bucket: aws.String(qualifiedPrefix.StorageNamespace),
		Prefix: aws.String(qualifiedPrefix.Prefix),
	}

	for {
		listOutput, err := a.clients.
			Get(ctx, qualifiedPrefix.StorageNamespace).
			ListObjects(ctx, &listObjectInput)
		if err != nil {
			log.WithError(err).Error("failed to list S3 objects")
			return err
		}

		for _, obj := range listOutput.Contents {
			if err := walkFn(*obj.Key); err != nil {
				return err
			}
		}

		if !listOutput.IsTruncated {
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
	s3Props, err := client.HeadObject(ctx, headObjectParams)
	if err != nil {
		return block.Properties{}, err
	}
	return block.Properties{StorageClass: aws.String(string(s3Props.StorageClass))}, nil
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
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	_, err = client.DeleteObject(ctx, deleteObjectParams)
	if err != nil {
		a.log(ctx).WithError(err).WithFields(qkFields(qualifiedKey)).Error("failed to delete S3 object")
		return err
	}
	waiter := s3.NewObjectNotExistsWaiter(client)
	return waiter.Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(qualifiedKey.StorageNamespace),
		Key:    aws.String(qualifiedKey.Key),
	}, S3WaitDur)
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

	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	uploadPartCopyIn := s3.UploadPartCopyInput{
		Bucket:     aws.String(qualifiedKey.StorageNamespace),
		Key:        aws.String(qualifiedKey.Key),
		PartNumber: int32(partNumber),
		UploadId:   aws.String(uploadID),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcKey.StorageNamespace, srcKey.Key)),
	}
	uploadPartCopyIn.CopySourceRange = byteRange

	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)

	httpClient := recordHeadersClient{Prefix: "X-Amz-Server-Side-"}
	uploadPartCopyOut, err := client.UploadPartCopy(ctx, &uploadPartCopyIn, httpClient.WrapClient())
	if err != nil {
		return nil, err
	}
	etag := uploadPartCopyOut.CopyPartResult.ETag
	if etag == nil || *etag == "" {
		return nil, ErrMissingETag
	}
	trimmedETag := strings.Trim(*etag, `"`)
	return &block.UploadPartResponse{
		ETag:             trimmedETag,
		ServerSideHeader: httpClient.Metadata,
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
	_, err = a.clients.Get(ctx, qualifiedDestinationKey.StorageNamespace).CopyObject(ctx, copyObjectParams)
	if err != nil {
		a.log(ctx).WithError(err).WithFields(logging.Fields{
			"qualified_ns_source":       qualifiedSourceKey.StorageNamespace,
			"qualified_key_source":      qualifiedSourceKey.Key,
			"qualified_ns_destination":  qualifiedDestinationKey.StorageNamespace,
			"qualified_key_destination": qualifiedDestinationKey.Key,
		}).Error("failed to copy S3 object")
	}
	return err
}

func (a *Adapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(qualifiedKey.StorageNamespace),
		Key:         aws.String(qualifiedKey.Key),
		ContentType: aws.String(""),
	}
	if opts.StorageClass != nil {
		input.StorageClass = types.StorageClass(*opts.StorageClass)
	}

	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)

	httpClient := recordHeadersClient{Prefix: ServerSideHeaderPrefix}
	upload, err := client.CreateMultipartUpload(ctx, input, httpClient.WrapClient())
	uploadID := *upload.UploadId
	uploadID = a.uploadIDTranslator.SetUploadID(uploadID)
	a.log(ctx).WithFields(qkFields(qualifiedKey)).WithFields(logging.Fields{
		"upload_id":            *upload.UploadId,
		"translated_upload_id": uploadID,
		"key":                  obj.Identifier,
	}).Debug("created multipart upload")
	return &block.CreateMultiPartUploadResponse{
		UploadID:         uploadID,
		ServerSideHeader: httpClient.Metadata,
	}, err
}

func (a *Adapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(qualifiedKey.StorageNamespace),
		Key:      aws.String(qualifiedKey.Key),
		UploadId: aws.String(uploadID),
	}
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)
	_, err = client.AbortMultipartUpload(ctx, input)
	a.uploadIDTranslator.RemoveUploadID(uploadID)
	a.log(ctx).WithFields(qkFields(qualifiedKey)).WithFields(logging.Fields{
		"upload_id": uploadID,
		"key":       obj.Identifier,
	}).Debug("aborted multipart upload")
	return err
}

func convertFromBlockMultipartUploadCompletion(multipartList *block.MultipartUploadCompletion) *types.CompletedMultipartUpload {
	parts := make([]types.CompletedPart, len(multipartList.Part))
	for i, p := range multipartList.Part {
		parts[i] = types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: int32(p.PartNumber),
		}
	}
	return &types.CompletedMultipartUpload{Parts: parts}
}

func (a *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	translatedUploadID := a.uploadIDTranslator.TranslateUploadID(uploadID)
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(qualifiedKey.StorageNamespace),
		Key:             aws.String(qualifiedKey.Key),
		UploadId:        aws.String(translatedUploadID),
		MultipartUpload: convertFromBlockMultipartUploadCompletion(multipartList),
	}
	lg := a.log(ctx).WithFields(qkFields(qualifiedKey)).WithFields(logging.Fields{
		"upload_id":            uploadID,
		"translated_upload_id": translatedUploadID,
		"key":                  obj.Identifier,
	})
	client := a.clients.Get(ctx, qualifiedKey.StorageNamespace)

	httpClient := recordHeadersClient{Prefix: ServerSideHeaderPrefix}
	uploadPartOut, err := client.CompleteMultipartUpload(ctx, input, httpClient.WrapClient())
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, err
	}
	lg.Debug("completed multipart upload")
	a.uploadIDTranslator.RemoveUploadID(translatedUploadID)
	headInput := &s3.HeadObjectInput{Bucket: &qualifiedKey.StorageNamespace, Key: &qualifiedKey.Key}
	headResp, err := client.HeadObject(ctx, headInput)
	if err != nil {
		return nil, err
	}

	etag := uploadPartOut.ETag
	if etag == nil || *etag == "" {
		return nil, ErrMissingETag
	}
	trimmedETag := strings.Trim(*etag, `"`)
	return &block.CompleteMultiPartUploadResponse{
		ETag:             trimmedETag,
		ContentLength:    headResp.ContentLength,
		ServerSideHeader: httpClient.Metadata,
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

// BUG(ariels): Extract this another way.  E.g. recordHeadersClient could do
//     this, or a similar one, maybe run it just when unknown :-/
//
// Needed to report stats of actual S3 server identification.
func (a *Adapter) ExtractS3Server(resp *http.Response) {
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

type httpClient interface {
	Do(r *http.Request) (*http.Response, error)
}

// recordHeadersClient wraps an HTTP client to record response headers that
// start with Prefix.  It is based on recordLocationClient in
// github.com/aws/aws-sdk-go-v2/feature/s3/manager.
type recordHeadersClient struct {
	httpClient
	Prefix   string
	Metadata http.Header
}

func (c *recordHeadersClient) WrapClient() func(o *s3.Options) {
	return func(o *s3.Options) {
		o.HTTPClient, c.httpClient = c, o.HTTPClient
	}
}

func (c *recordHeadersClient) Do(req *http.Request) (resp *http.Response, err error) {
	resp, err = c.httpClient.Do(req)
	if err != nil {
		return
	}

	if resp.Header != nil {
		c.Metadata = make(http.Header)
		for k, v := range resp.Header {
			if strings.HasPrefix(k, c.Prefix) {
				c.Metadata[k] = v
			}
		}
	}
	return resp, err
}
