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

var ErrNoMoreRowsToSkip = errors.New("no more rows to skip")

type IInventoryReader interface {
	GetInventoryFileReader(fileInManifest string) (InventoryFileReader, error)
	GetInventoryMetadataReader(fileInManifest string) (InventoryMetadataReader, error)
}

type InventoryReader struct {
	manifest      *Manifest
	ctx           context.Context
	svc           s3iface.S3API
	orcFilesByKey map[string]*orcFile
	logger        logging.Logger
}

type OrcInventoryFileReader struct {
	reader                   *orc.Reader
	c                        *orc.Cursor
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
	idx           int
	localFilename string
	ready         bool
}

func NewInventoryReader(ctx context.Context, svc s3iface.S3API, manifest *Manifest, logger logging.Logger) IInventoryReader {
	return &InventoryReader{ctx: ctx, svc: svc, manifest: manifest, logger: logger, orcFilesByKey: make(map[string]*orcFile)}
}

func (o *InventoryReader) cleanOrcFile(key string) {
	localFilename := o.orcFilesByKey[key].localFilename
	delete(o.orcFilesByKey, key)
	defer func() {
		_ = os.Remove(localFilename)
	}()
}

func (o *InventoryReader) GetInventoryFileReader(key string) (InventoryFileReader, error) {
	switch o.manifest.Format {
	case OrcFormatName:
		return o.getOrcReader(key, false)
	case ParquetFormatName:
		return o.getParquetReader(key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
}

func (o *InventoryReader) GetInventoryMetadataReader(key string) (InventoryMetadataReader, error) {
	switch o.manifest.Format {
	case OrcFormatName:
		return o.getOrcReader(key, true)
	case ParquetFormatName:
		return o.getParquetReader(key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
}

func (o *InventoryReader) getParquetReader(key string) (InventoryFileReader, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(o.ctx, o.svc, o.manifest.inventoryBucket, key)
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

func (o *InventoryReader) getOrcReader(key string, footerOnly bool) (InventoryFileReader, error) {
	file, ok := o.orcFilesByKey[key]
	if !ok {
		file = &orcFile{key: key}
		o.orcFilesByKey[key] = file
	}
	for idx, f := range o.manifest.Files {
		if f.Key == key {
			file.idx = idx
			break
		}
	}
	if !file.ready {
		localFilename, err := DownloadOrcFile(o.ctx, o.svc, o.logger, o.manifest.inventoryBucket, key, footerOnly)
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
	res := &OrcInventoryFileReader{reader: orcReader, mgr: o, key: key}
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
		if !r.c.Next() {
			r.mgr.logger.Debugf("start new stripe in file %s", r.key)
			if !r.c.Stripes() {
				return nil
			} else if !r.c.Next() {
				return nil
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
