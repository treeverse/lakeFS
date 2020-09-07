package s3

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/go-openapi/swag"
	"github.com/hashicorp/go-multierror"
	"github.com/scritchley/orc"
	"modernc.org/mathutil"
)

type OrcInventoryFileReader struct {
	reader                   *orc.Reader
	c                        *orc.Cursor
	ctx                      context.Context
	selectIndexByField       map[string]int
	actualColumnIndexByField map[string]int
	mgr                      *Reader
	key                      string
}

type orcFile struct {
	key           string
	localFilename string
	ready         bool
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
	var combinedErr error
	if err := r.c.Close(); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	if err := r.reader.Close(); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	r.mgr.cleanOrcFile(r.key)
	return combinedErr
}

func (r *OrcInventoryFileReader) MinValue() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.actualColumnIndexByField["key"]+1].StringStatistics.Minimum
}

func (r *OrcInventoryFileReader) MaxValue() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.actualColumnIndexByField["key"]+1].StringStatistics.Maximum
}
