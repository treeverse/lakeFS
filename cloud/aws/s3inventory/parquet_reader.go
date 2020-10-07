package s3inventory

import (
	"fmt"
	"sync"

	"github.com/go-openapi/swag"
	"github.com/spf13/cast"
	"github.com/xitongsys/parquet-go/reader"
)

var inventoryFields = []string{"bucket", "key", "size", "last_modified_date", "e_tag", "is_delete_marker", "is_latest"}

type ParquetInventoryFileReader struct {
	reader.ParquetReader
}

func (p *ParquetInventoryFileReader) Close() error {
	p.ReadStop()
	return p.PFile.Close()
}

func (p *ParquetInventoryFileReader) FirstObjectKey() string {
	return string(p.Footer.RowGroups[0].Columns[0].GetMetaData().GetStatistics().GetMinValue())
}

func (p *ParquetInventoryFileReader) LastObjectKey() string {
	return string(p.Footer.RowGroups[0].Columns[0].GetMetaData().GetStatistics().GetMaxValue())
}

func (p *ParquetInventoryFileReader) Read(n int) ([]InventoryObject, error) {
	res := make([]InventoryObject, n)
	actualFields := p.getActualFields()
	var wg sync.WaitGroup
	errChan := make(chan error, len(actualFields))
	for fieldName, path := range actualFields {
		wg.Add(1)
		path := path
		fieldName := fieldName
		go func() {
			defer wg.Done()
			columnRes, _, _, err := p.ReadColumnByPath(path, int64(n))
			if err != nil {
				errChan <- fmt.Errorf("failed to read parquet column %s: %w", fieldName, err)
				return
			}
			for i, v := range columnRes {
				err := set(&res[i], fieldName, v)
				if err != nil {
					errChan <- fmt.Errorf("failed to read parquet column %s: %w", fieldName, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errChan:
		return nil, err
	default:
	}
	close(errChan)
	return res, nil
}

func set(o *InventoryObject, f string, v interface{}) error {
	var err error
	switch f {
	case "bucket":
		var bucket string
		bucket, err = cast.ToStringE(v)
		o.Bucket = bucket
	case "key":
		var key string
		key, err = cast.ToStringE(v)
		o.Key = key
	case "is_latest":
		var isLatest bool
		isLatest, err = cast.ToBoolE(v)
		o.IsLatest = swag.Bool(isLatest)
	case "is_delete_marker":
		var isDeleteMarker bool
		isDeleteMarker, err = cast.ToBoolE(v)
		o.IsDeleteMarker = swag.Bool(isDeleteMarker)
	case "size":
		var size int64
		size, err = cast.ToInt64E(v)
		o.Size = swag.Int64(size)
	case "last_modified_date":
		var lastModifiedMillis int64
		lastModifiedMillis, err = cast.ToInt64E(v)
		o.LastModifiedMillis = swag.Int64(lastModifiedMillis)
	case "e_tag":
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
		for _, r := range inventoryFields {
			if fieldInfo.ExName == r {
				res[r] = p.SchemaHandler.IndexMap[int32(i)]
			}
		}
	}
	return res
}
