package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-openapi/swag"
	"github.com/scritchley/orc"
	"github.com/treeverse/lakefs/logging"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"modernc.org/mathutil"
)

const (
	OrcFormatName     = "ORC"
	ParquetFormatName = "Parquet"
)

var (
	ErrUnsupportedInventoryFormat = errors.New("unsupported inventory type. supported types: parquet, orc")
	ErrNoMoreRowsToSkip           = errors.New("no more rows to skip")
)

type IReader interface {
	GetInventoryFileReader(format string, bucket string, key string) (InventoryFileReader, error)
	GetInventoryMetadataReader(format string, bucket string, key string) (InventoryMetadataReader, error)
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

type InventoryReader struct {
	ctx           context.Context
	svc           s3iface.S3API
	orcFilesByKey map[string]*orcFile
	logger        logging.Logger
}

type InventoryMetadataReader interface {
	GetNumRows() int64
	SkipRows(int64) error
	Close() error
	MinValue() string
	MaxValue() string
}

type InventoryFileReader interface {
	InventoryMetadataReader
	Read(dstInterface interface{}) error
}

type OrcInventoryFileReader struct {
	reader                   *orc.Reader
	c                        *orc.Cursor
	ctx                      context.Context
	selectIndexByField       map[string]int
	actualColumnIndexByField map[string]int
	mgr                      *InventoryReader
	key                      string
}

type ParquetInventoryFileReader struct {
	reader.ParquetReader
}

type orcFile struct {
	key           string
	localFilename string
	ready         bool
}

func NewReader(ctx context.Context, svc s3iface.S3API, logger logging.Logger) IReader {
	return &InventoryReader{ctx: ctx, svc: svc, logger: logger, orcFilesByKey: make(map[string]*orcFile)}
}

func (o *InventoryReader) cleanOrcFile(key string) {
	localFilename := o.orcFilesByKey[key].localFilename
	delete(o.orcFilesByKey, key)
	defer func() {
		_ = os.Remove(localFilename)
	}()
}

func (o *InventoryReader) GetInventoryFileReader(format string, bucket string, key string) (InventoryFileReader, error) {
	switch format {
	case OrcFormatName:
		return o.getOrcReader(bucket, key, false)
	case ParquetFormatName:
		return o.getParquetReader(bucket, key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
}

func (o *InventoryReader) GetInventoryMetadataReader(format string, bucket string, key string) (InventoryMetadataReader, error) {
	switch format {
	case OrcFormatName:
		return o.getOrcReader(bucket, key, true)
	case ParquetFormatName:
		return o.getParquetReader(bucket, key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
}

func (o *InventoryReader) getParquetReader(bucket string, key string) (InventoryFileReader, error) {
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

func (o *InventoryReader) getOrcReader(bucket string, key string, footerOnly bool) (InventoryFileReader, error) {
	file, ok := o.orcFilesByKey[key]
	if !ok {
		file = &orcFile{key: key}
		o.orcFilesByKey[key] = file
	}
	if !file.ready {
		localFilename, err := DownloadOrc(o.ctx, o.svc, o.logger, bucket, key, footerOnly)
		if err != nil {
			return nil, err
		}
		file.ready = true
		file.localFilename = localFilename
	}
	orcReader, err := orc.Open(file.localFilename)
	if err != nil {
		return nil, err
	}
	res := &OrcInventoryFileReader{ctx: o.ctx, reader: orcReader, mgr: o, key: key}
	selectFields, selectIndexByField, actualColumnIndexByField := getSelectFields(res.reader.Schema())
	res.selectIndexByField = selectIndexByField
	res.actualColumnIndexByField = actualColumnIndexByField
	res.c = res.reader.Select(selectFields...)
	return res, nil
}

func (p *ParquetInventoryFileReader) Close() error {
	p.ReadStop()
	return p.PFile.Close()
}

func (p *ParquetInventoryFileReader) MinValue() string {
	return string(p.Footer.RowGroups[0].Columns[0].GetMetaData().GetStatistics().GetMinValue())
}

func (p *ParquetInventoryFileReader) MaxValue() string {
	return string(p.Footer.RowGroups[0].Columns[0].GetMetaData().GetStatistics().GetMaxValue())
}

func getSelectFields(typeDescription *orc.TypeDescription) ([]string, map[string]int, map[string]int) {
	relevantFields := []string{"bucket", "key", "size", "last_modified_date", "e_tag", "is_delete_marker", "is_latest"}
	actualColumnIndexByField := make(map[string]int)
	selectFields := make([]string, 0, len(typeDescription.Columns()))
	selectIndexByField := make(map[string]int)
	for i, field := range typeDescription.Columns() {
		actualColumnIndexByField[field] = i
	}
	j := 0
	for _, field := range relevantFields {
		if _, ok := actualColumnIndexByField[field]; ok {
			selectFields = append(selectFields, field)
			selectIndexByField[field] = j
			j++
		}
	}
	return selectFields, selectIndexByField, actualColumnIndexByField
}

func (r *OrcInventoryFileReader) inventoryObjectFromRow(rowData []interface{}) InventoryObject {
	var size *int64
	if sizeIdx, ok := r.selectIndexByField["size"]; ok && rowData[sizeIdx] != nil {
		size = swag.Int64(rowData[sizeIdx].(int64))
	}
	var lastModified *int64
	if lastModifiedIdx, ok := r.selectIndexByField["last_modified_date"]; ok && rowData[lastModifiedIdx] != nil {
		lastModified = swag.Int64(rowData[lastModifiedIdx].(time.Time).Unix())
	}
	var eTag *string
	if eTagIdx, ok := r.selectIndexByField["e_tag"]; ok && rowData[eTagIdx] != nil {
		eTag = swag.String(rowData[eTagIdx].(string))
	}
	var isLatest *bool
	if isLatestIdx, ok := r.selectIndexByField["is_latest"]; ok && rowData[isLatestIdx] != nil {
		isLatest = swag.Bool(rowData[isLatestIdx].(bool))
	}
	var isDeleteMarker *bool
	if isDeleteMarkerIdx, ok := r.selectIndexByField["is_delete_marker"]; ok && rowData[isDeleteMarkerIdx] != nil {
		isDeleteMarker = swag.Bool(rowData[isDeleteMarkerIdx].(bool))
	}
	return InventoryObject{
		Bucket:         rowData[r.selectIndexByField["bucket"]].(string),
		Key:            rowData[r.selectIndexByField["key"]].(string),
		Size:           size,
		LastModified:   lastModified,
		Checksum:       eTag,
		IsLatest:       isLatest,
		IsDeleteMarker: isDeleteMarker,
	}
}

func (r *OrcInventoryFileReader) Read(dstInterface interface{}) error {
	num := reflect.ValueOf(dstInterface).Elem().Len()
	res := make([]InventoryObject, 0, num)
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}
		if !r.c.Next() {
			r.mgr.logger.Debugf("start new stripe in file %s", r.key)
			if !r.c.Stripes() {
				break
			} else if !r.c.Next() {
				break
			}
		}
		res = append(res, r.inventoryObjectFromRow(r.c.Row()))
		if len(res) == num {
			break
		}
	}

	reflect.ValueOf(dstInterface).Elem().Set(reflect.ValueOf(res))
	return nil
}

func (r *OrcInventoryFileReader) GetNumRows() int64 {
	if os.Getenv("LAKEFS_IMPORT_LIMIT_PER_FILE") != "" {
		limit, err := strconv.ParseInt(os.Getenv("LAKEFS_IMPORT_LIMIT_PER_FILE"), 10, 64)
		if err == nil {
			return mathutil.MinInt64(limit, int64(r.reader.NumRows()))
		}
	}
	return int64(r.reader.NumRows())
}

func (r *OrcInventoryFileReader) SkipRows(i int64) error {
	if i == 0 {
		return nil
	}
	skipped := int64(0)
	for r.c.Stripes() {
		for r.c.Next() {
			r.c.Row()
			skipped += 1
			if skipped == i {
				return nil
			}
		}
	}
	return ErrNoMoreRowsToSkip
}

func (r *OrcInventoryFileReader) Close() error {
	_ = r.c.Close()
	_ = r.reader.Close()
	r.mgr.cleanOrcFile(r.key)
	return nil
}

func (r *OrcInventoryFileReader) MinValue() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.actualColumnIndexByField["key"]+1].StringStatistics.Minimum
}

func (r *OrcInventoryFileReader) MaxValue() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.actualColumnIndexByField["key"]+1].StringStatistics.Maximum
}
