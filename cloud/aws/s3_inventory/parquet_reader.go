package s3_inventory

import "github.com/xitongsys/parquet-go/reader"

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
