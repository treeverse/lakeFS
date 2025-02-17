package block

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/treeverse/lakefs/pkg/httputil"
)

type MetricsAdapter struct {
	adapter Adapter
}

func NewMetricsAdapter(adapter Adapter) Adapter {
	return &MetricsAdapter{adapter: adapter}
}

func (m *MetricsAdapter) InnerAdapter() Adapter {
	return m.adapter
}

func (m *MetricsAdapter) Put(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) (*PutResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Put(ctx, obj, sizeBytes, reader, opts)
}

func (m *MetricsAdapter) Get(ctx context.Context, obj ObjectPointer) (io.ReadCloser, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Get(ctx, obj)
}

func (m *MetricsAdapter) GetWalker(storageID string, opts WalkerOptions) (Walker, error) {
	return m.adapter.GetWalker(storageID, opts)
}

func (m *MetricsAdapter) GetPreSignedURL(ctx context.Context, obj ObjectPointer, mode PreSignMode) (string, time.Time, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetPreSignedURL(ctx, obj, mode)
}

func (m *MetricsAdapter) GetPresignUploadPartURL(ctx context.Context, obj ObjectPointer, uploadID string, partNumber int) (string, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetPresignUploadPartURL(ctx, obj, uploadID, partNumber)
}

func (m *MetricsAdapter) Exists(ctx context.Context, obj ObjectPointer) (bool, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Exists(ctx, obj)
}

func (m *MetricsAdapter) GetRange(ctx context.Context, obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetRange(ctx, obj, startPosition, endPosition)
}

func (m *MetricsAdapter) GetProperties(ctx context.Context, obj ObjectPointer) (Properties, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetProperties(ctx, obj)
}

func (m *MetricsAdapter) Remove(ctx context.Context, obj ObjectPointer) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Remove(ctx, obj)
}

func (m *MetricsAdapter) Copy(ctx context.Context, sourceObj, destinationObj ObjectPointer) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Copy(ctx, sourceObj, destinationObj)
}

func (m *MetricsAdapter) CreateMultiPartUpload(ctx context.Context, obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (*CreateMultiPartUploadResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.CreateMultiPartUpload(ctx, obj, r, opts)
}

func (m *MetricsAdapter) UploadPart(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*UploadPartResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.UploadPart(ctx, obj, sizeBytes, reader, uploadID, partNumber)
}

func (m *MetricsAdapter) ListParts(ctx context.Context, obj ObjectPointer, uploadID string, opts ListPartsOpts) (*ListPartsResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.ListParts(ctx, obj, uploadID, opts)
}
func (m *MetricsAdapter) ListMultipartUploads(ctx context.Context, obj ObjectPointer, opts ListMultipartUploadsOpts) (*ListMultipartUploadsResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.ListMultipartUploads(ctx, obj, opts)
}

func (m *MetricsAdapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int) (*UploadPartResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.UploadCopyPart(ctx, sourceObj, destinationObj, uploadID, partNumber)
}

func (m *MetricsAdapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*UploadPartResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.UploadCopyPartRange(ctx, sourceObj, destinationObj, uploadID, partNumber, startPosition, endPosition)
}

func (m *MetricsAdapter) AbortMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.AbortMultiPartUpload(ctx, obj, uploadID)
}

func (m *MetricsAdapter) CompleteMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string, multipartList *MultipartUploadCompletion) (*CompleteMultiPartUploadResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.CompleteMultiPartUpload(ctx, obj, uploadID, multipartList)
}

func (m *MetricsAdapter) BlockstoreType() string {
	return m.adapter.BlockstoreType()
}

func (m *MetricsAdapter) BlockstoreMetadata(ctx context.Context) (*BlockstoreMetadata, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.BlockstoreMetadata(ctx)
}

func (m *MetricsAdapter) GetStorageNamespaceInfo(storageID string) *StorageNamespaceInfo {
	return m.adapter.GetStorageNamespaceInfo(storageID)
}

func (m *MetricsAdapter) ResolveNamespace(storageID, storageNamespace, key string, identifierType IdentifierType) (QualifiedKey, error) {
	return m.adapter.ResolveNamespace(storageID, storageNamespace, key, identifierType)
}

func (m *MetricsAdapter) GetRegion(ctx context.Context, storageID, storageNamespace string) (string, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetRegion(ctx, storageID, storageNamespace)
}

func (m *MetricsAdapter) RuntimeStats() map[string]string {
	return m.adapter.RuntimeStats()
}

type Histograms struct {
	durationHistograms    *prometheus.HistogramVec
	requestSizeHistograms *prometheus.HistogramVec
	tag                   *string
}

func BuildHistogramsInstance(name string, tag *string) Histograms {
	labelNames := []string{"operation", "error"}
	if tag != nil {
		labelNames = append(labelNames, "tag")
	}
	var durationHistograms = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: name + "_operation_duration_seconds",
			Help: "durations of outgoing " + name + " operations",
		},
		labelNames,
	)
	var requestSizeHistograms = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    name + "_operation_size_bytes",
			Help:    "handled sizes of outgoing " + name + " operations",
			Buckets: prometheus.ExponentialBuckets(1, 10, 10), //nolint: mnd
		},
		labelNames,
	)

	return Histograms{
		durationHistograms:    durationHistograms,
		requestSizeHistograms: requestSizeHistograms,
		tag:                   tag,
	}
}

func (s Histograms) ReportMetrics(operation string, start time.Time, sizeBytes *int64, err *error) {
	isErrStr := strconv.FormatBool(*err != nil)
	labels := []string{operation, isErrStr}
	if s.tag != nil {
		labels = append(labels, *s.tag)
	}
	s.durationHistograms.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	if sizeBytes != nil {
		s.requestSizeHistograms.WithLabelValues(labels...).Observe(float64(*sizeBytes))
	}
}
