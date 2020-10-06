package s3inventory

import (
	"context"
	"reflect"
	"time"

	"github.com/go-openapi/swag"
	"github.com/hashicorp/go-multierror"
	"github.com/scritchley/orc"
)

type OrcInventoryFileReader struct {
	reader    *orc.Reader
	cursor    *orc.Cursor
	ctx       context.Context
	orcSelect *OrcSelect
	orcFile   *OrcFile
}

type OrcField struct {
	IndexInFile   int
	IndexInSelect int
}

type OrcSelect struct {
	SelectFields  []string       // the list of fields to select from the file
	IndexInSelect map[string]int // for each field, its index in the select query
	IndexInFile   map[string]int // for each field, its index in the original file
}

func getOrcSelect(typeDescription *orc.TypeDescription) *OrcSelect {
	relevantFields := []string{"bucket", "key", "size", "last_modified_date", "e_tag", "is_delete_marker", "is_latest"}
	res := &OrcSelect{
		SelectFields:  nil,
		IndexInFile:   make(map[string]int),
		IndexInSelect: make(map[string]int),
	}
	for i, field := range typeDescription.Columns() {
		res.IndexInFile[field] = i
	}
	j := 0
	for _, field := range relevantFields {
		if _, ok := res.IndexInFile[field]; ok {
			res.SelectFields = append(res.SelectFields, field)
			res.IndexInSelect[field] = j
			j++
		}
	}
	return res
}

func (r *OrcInventoryFileReader) inventoryObjectFromRow(rowData []interface{}) InventoryObject {
	var size *int64
	if sizeIdx, ok := r.orcSelect.IndexInSelect["size"]; ok && rowData[sizeIdx] != nil {
		size = swag.Int64(rowData[sizeIdx].(int64))
	}
	var lastModifiedMillis *int64
	if lastModifiedIdx, ok := r.orcSelect.IndexInSelect["last_modified_date"]; ok && rowData[lastModifiedIdx] != nil {
		lastModifiedMillis = swag.Int64(rowData[lastModifiedIdx].(time.Time).UnixNano() / int64(time.Millisecond))
	}
	var eTag *string
	if eTagIdx, ok := r.orcSelect.IndexInSelect["e_tag"]; ok && rowData[eTagIdx] != nil {
		eTag = swag.String(rowData[eTagIdx].(string))
	}
	var isLatest *bool
	if isLatestIdx, ok := r.orcSelect.IndexInSelect["is_latest"]; ok && rowData[isLatestIdx] != nil {
		isLatest = swag.Bool(rowData[isLatestIdx].(bool))
	}
	var isDeleteMarker *bool
	if isDeleteMarkerIdx, ok := r.orcSelect.IndexInSelect["is_delete_marker"]; ok && rowData[isDeleteMarkerIdx] != nil {
		isDeleteMarker = swag.Bool(rowData[isDeleteMarkerIdx].(bool))
	}
	return InventoryObject{
		Bucket:             rowData[r.orcSelect.IndexInSelect["bucket"]].(string),
		Key:                rowData[r.orcSelect.IndexInSelect["key"]].(string),
		Size:               size,
		LastModifiedMillis: lastModifiedMillis,
		Checksum:           eTag,
		IsLatest:           isLatest,
		IsDeleteMarker:     isDeleteMarker,
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
		if !r.cursor.Next() {
			if !r.cursor.Stripes() {
				break
			}
			if !r.cursor.Next() {
				break
			}
		}
		res = append(res, r.inventoryObjectFromRow(r.cursor.Row()))
		if len(res) == num {
			break
		}
	}

	reflect.ValueOf(dstInterface).Elem().Set(reflect.ValueOf(res))
	return nil
}

func (r *OrcInventoryFileReader) GetNumRows() int64 {
	return int64(r.reader.NumRows())
}

func (r *OrcInventoryFileReader) Close() error {
	var combinedErr error
	if err := r.cursor.Close(); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	if err := r.reader.Close(); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	if err := r.orcFile.Close(); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	return combinedErr
}

func (r *OrcInventoryFileReader) FirstObjectKey() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.orcSelect.IndexInFile["key"]+1].StringStatistics.Minimum
}

func (r *OrcInventoryFileReader) LastObjectKey() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.orcSelect.IndexInFile["key"]+1].StringStatistics.Maximum
}
