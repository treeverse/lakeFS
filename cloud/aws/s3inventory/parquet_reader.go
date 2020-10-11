package s3inventory

import (
	"fmt"
	"sync"

	"github.com/cznic/mathutil"

	"github.com/go-openapi/swag"
	"github.com/spf13/cast"
	"github.com/xitongsys/parquet-go/reader"
)

type ParquetInventoryFileReader struct {
	*reader.ParquetReader
	nextRow      int64
	actualFields map[string]string
}

func NewParquetInventoryFileReader(parquetReader *reader.ParquetReader) (*ParquetInventoryFileReader, error) {
	res := &ParquetInventoryFileReader{
		ParquetReader: parquetReader,
	}
	res.actualFields = res.getActualFields()
	for _, required := range requiredFields {
		if _, ok := res.actualFields[required]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrRequiredFieldNotFound, required)
		}
	}
	return res, nil
}

func (p *ParquetInventoryFileReader) Close() error {
	p.ReadStop()
	return p.PFile.Close()
}

func (p *ParquetInventoryFileReader) FirstObjectKey() string {
	for i, c := range p.Footer.RowGroups[0].Columns {
		if c.MetaData.PathInSchema[len(c.GetMetaData().GetPathInSchema())-1] == "Key" {
			return string(p.Footer.RowGroups[0].Columns[i].GetMetaData().GetStatistics().GetMin())
		}
	}
	return string(p.Footer.RowGroups[0].Columns[1].GetMetaData().GetStatistics().GetMin())
}

func (p *ParquetInventoryFileReader) LastObjectKey() string {
	for i, c := range p.Footer.RowGroups[0].Columns {
		if c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1] == "Key" {
			return string(p.Footer.RowGroups[0].Columns[i].GetMetaData().GetStatistics().GetMax())
		}
	}
	return string(p.Footer.RowGroups[0].Columns[1].GetMetaData().GetStatistics().GetMax())
}

func (p *ParquetInventoryFileReader) Read(n int) ([]*InventoryObject, error) {
	var wg sync.WaitGroup
	num := mathutil.MinInt64(int64(n), p.GetNumRows()-p.nextRow)
	p.nextRow += num
	res := make([]*InventoryObject, num)
	for fieldName, path := range p.actualFields {
		wg.Add(1)
		path := path
		fieldName := fieldName

		columnRes, _, dls, err := p.ReadColumnByPath(path, num)
		if err != nil {
			return nil, fmt.Errorf("failed to read parquet column %s: %w", fieldName, err)
		}
		for i, v := range columnRes {
			if !isRequired(fieldName) && dls[i] == 0 {
				// got no value for non-required field, move on
				continue
			}
			if res[i] == nil {
				res[i] = new(InventoryObject)
			}
			err := set(res[i], fieldName, v)
			if err != nil {
				return nil, fmt.Errorf("failed to read parquet column %s: %w", fieldName, err)
			}
		}
	}
	return res, nil
}

func set(o *InventoryObject, f string, v interface{}) error {
	var err error
	switch f {
	case bucketFieldName:
		var bucket string
		bucket, err = cast.ToStringE(v)
		o.Bucket = bucket
	case keyFieldName:
		var key string
		key, err = cast.ToStringE(v)
		o.Key = key
	case isLatestFieldName:
		var isLatest bool
		isLatest, err = cast.ToBoolE(v)
		o.IsLatest = swag.Bool(isLatest)
	case isDeleteMarkerFieldName:
		var isDeleteMarker bool
		isDeleteMarker, err = cast.ToBoolE(v)
		o.IsDeleteMarker = swag.Bool(isDeleteMarker)
	case sizeFieldName:
		var size int64
		size, err = cast.ToInt64E(v)
		o.Size = swag.Int64(size)
	case lastModifiedDateFieldName:
		var lastModifiedMillis int64
		lastModifiedMillis, err = cast.ToInt64E(v)
		o.LastModifiedMillis = swag.Int64(lastModifiedMillis)
	case eTagFieldName:
		var checksum string
		checksum, err = cast.ToStringE(v)
		o.Checksum = swag.String(checksum)
	}
	return err
}

// getActualFields returns parquet schema fields as a mapping from their base column name to their path in ParquetReader
// only known inventory fields are returned
func (p *ParquetInventoryFileReader) getActualFields() map[string]string {
	res := make(map[string]string)
	for i, fieldInfo := range p.SchemaHandler.Infos {
		for _, field := range inventoryFields {
			if fieldInfo.ExName == field {
				res[field] = p.SchemaHandler.IndexMap[int32(i)]
			}
		}
	}
	return res
}
