package s3

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/scritchley/orc"
	"github.com/treeverse/lakefs/logging"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
)

const (
	OrcFormatName     = "ORC"
	ParquetFormatName = "Parquet"
)

var (
	ErrUnsupportedInventoryFormat = errors.New("unsupported inventory type. supported types: parquet, orc")
)

type IReader interface {
	GetFileReader(format string, bucket string, key string) (FileReader, error)
	GetMetadataReader(format string, bucket string, key string) (MetadataReader, error)
}

type InventoryObject struct {
	Bucket         string  `parquet:"name=bucket, type=UTF8"`
	Key            string  `parquet:"name=key, type=UTF8"`
	IsLatest       *bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker *bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
	Size           *int64  `parquet:"name=size, type=INT_64"`
	LastModified   *int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	Checksum       *string `parquet:"name=e_tag, type=UTF8"`
}

func (o *InventoryObject) GetPhysicalAddress() string {
	return "s3://" + o.Bucket + "/" + o.Key
}

type Reader struct {
	ctx    context.Context
	svc    s3iface.S3API
	logger logging.Logger
}

type MetadataReader interface {
	GetNumRows() int64
	Close() error
	FirstObjectKey() string
	LastObjectKey() string
}

type FileReader interface {
	MetadataReader
	Read(dstInterface interface{}) error
}

func NewReader(ctx context.Context, svc s3iface.S3API, logger logging.Logger) IReader {
	return &Reader{ctx: ctx, svc: svc, logger: logger}
}

func (o *Reader) GetFileReader(format string, bucket string, key string) (FileReader, error) {
	switch format {
	case OrcFormatName:
		return o.getOrcReader(bucket, key, false)
	case ParquetFormatName:
		return o.getParquetReader(bucket, key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
}

func (o *Reader) GetMetadataReader(format string, bucket string, key string) (MetadataReader, error) {
	switch format {
	case OrcFormatName:
		return o.getOrcReader(bucket, key, true)
	default:
		return o.GetFileReader(format, bucket, key)
	}
}

func (o *Reader) getParquetReader(bucket string, key string) (FileReader, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(o.ctx, o.svc, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	var rawObject InventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	return &ParquetInventoryFileReader{ParquetReader: *pr}, nil
}

func (o *Reader) getOrcReader(bucket string, key string, tailOnly bool) (FileReader, error) {
	orcFile, err := DownloadOrc(o.ctx, o.svc, o.logger, bucket, key, tailOnly)
	if err != nil {
		return nil, err
	}
	orcReader, err := orc.NewReader(orcFile)
	if err != nil {
		return nil, err
	}
	orcSelect := getOrcSelect(orcReader.Schema())
	return &OrcInventoryFileReader{
		ctx:       o.ctx,
		reader:    orcReader,
		orcFile:   orcFile,
		orcSelect: orcSelect,
		cursor:    orcReader.Select(orcSelect.SelectFields...),
	}, nil
}
