package s3inventory

import (
	"context"
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
	res := &OrcSelect{
		SelectFields:  nil,
		IndexInFile:   make(map[string]int),
		IndexInSelect: make(map[string]int),
	}
	for i, field := range typeDescription.Columns() {
		res.IndexInFile[field] = i
	}
	j := 0
	for _, field := range inventoryFields {
		if _, ok := res.IndexInFile[field]; ok {
			res.SelectFields = append(res.SelectFields, field)
			res.IndexInSelect[field] = j
			j++
		}
	}
	return res
}

func (r *OrcInventoryFileReader) inventoryObjectFromRow(rowData []interface{}) *InventoryObject {
	obj := NewInventoryObject()
	obj.Bucket = rowData[r.orcSelect.IndexInSelect[bucketFieldName]].(string)
	obj.Key = rowData[r.orcSelect.IndexInSelect[keyFieldName]].(string)
	if sizeIdx, ok := r.orcSelect.IndexInSelect[sizeFieldName]; ok && rowData[sizeIdx] != nil {
		obj.Size = rowData[sizeIdx].(int64)
	}
	if lastModifiedIdx, ok := r.orcSelect.IndexInSelect[lastModifiedDateFieldName]; ok && rowData[lastModifiedIdx] != nil {
		obj.LastModified = swag.Time(rowData[lastModifiedIdx].(time.Time))
	}
	if eTagIdx, ok := r.orcSelect.IndexInSelect[eTagFieldName]; ok && rowData[eTagIdx] != nil {
		obj.Checksum = rowData[eTagIdx].(string)
	}
	if isLatestIdx, ok := r.orcSelect.IndexInSelect[isLatestFieldName]; ok && rowData[isLatestIdx] != nil {
		obj.IsLatest = rowData[isLatestIdx].(bool)
	}
	if isDeleteMarkerIdx, ok := r.orcSelect.IndexInSelect[isDeleteMarkerFieldName]; ok && rowData[isDeleteMarkerIdx] != nil {
		obj.IsDeleteMarker = rowData[isDeleteMarkerIdx].(bool)
	}
	return obj
}

func (r *OrcInventoryFileReader) Read(n int) ([]*InventoryObject, error) {
	res := make([]*InventoryObject, 0, n)
	for {
		select {
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
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
		obj := r.inventoryObjectFromRow(r.cursor.Row())
		res = append(res, obj)
		if len(res) == n {
			break
		}
	}
	return res, nil
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
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.orcSelect.IndexInFile[keyFieldName]+1].StringStatistics.Minimum
}

func (r *OrcInventoryFileReader) LastObjectKey() string {
	return *r.reader.Metadata().StripeStats[0].GetColStats()[r.orcSelect.IndexInFile[keyFieldName]+1].StringStatistics.Maximum
}
