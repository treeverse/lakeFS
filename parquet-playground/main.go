package main

import (
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type S346inventory struct {
	Bucket string `parquet:"name=bucket, type=UTF8, repetitiontype=REQUIRED"`

	Key string `parquet:"name=key, type=UTF8, repetitiontype=REQUIRED"`

	Size *int64 `parquet:"name=size, type=INT64, repetitiontype=OPTIONAL"`

	LastModifiedDate *int64 `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS, repetitiontype=OPTIONAL"`

	ETag *string `parquet:"name=e_tag, type=UTF8, repetitiontype=OPTIONAL"`

	StorageClass *string `parquet:"name=storage_class, type=UTF8, repetitiontype=OPTIONAL"`

	IsMultipartUploaded *bool `parquet:"name=is_multipart_uploaded, type=BOOLEAN, repetitiontype=OPTIONAL"`
}

func main() {
	_, err := readSingleRow()
	if err != nil {
		panic(err)
	}

	//const newFilesCount = 10000
	//moreFiles := make([]*S346inventory, newFilesCount)
	//
	//for i := 0; i < 100000; i++ {
	//	o := files[i%len(files)]
	//	o.Key = o.Key + strconv.Itoa(i)
	//	moreFiles[i] = o
	//}
	//
	//for i := 0; i <= 6; i++ {
	//	generateParquet(moreFiles, i)
	//}
}

const recordNumber = 10000
const lookupKey = "events/dt=2020-01-12/part-00000-db6d9dfd-681b-4eb4-af00-cabc91b8a2b2.c000"
const keyColumn = 1

func readSingleRow() ([]*S346inventory, error) {
	fr, err := local.NewLocalFileReader(file)
	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, new(S346inventory), recordNumber)
	if err != nil {
		return nil, err
	}

	var keyPageNumber int
	for _, rg := range pr.Footer.RowGroups {
		cols := rg.GetColumns()
		keyCol := cols[keyColumn]
		keyStats := keyCol.GetMetaData().GetStatistics()

		// filter raw groups
		if !keyInRange(lookupKey, string(keyStats.GetMinValue()), string(keyStats.GetMaxValue())) {
			continue
		}

		fmt.Printf("found the raw group")
		// read the key column index

		colIndex, err := columnIndex(keyCol)
		if err != nil {
			return nil, err
		}

		// find the matching pages
		for i, isNull := range colIndex.NullPages {
			if isNull {
				// all values for this page are null, nothing to check here
				continue
			}
			if keyInRange(lookupKey, string(colIndex.MinValues[i]), string(colIndex.MaxValues[i])) {
				// found a possible match
				fmt.Printf("found pages for key column: %v", keyPageNumber)
				keyPageNumber = i
				break
			}
		}

		// found the matching rawGroup, no need to keep iterating
		break
	}

	cb := pr.ColumnBuffers["Parquet_go_root.Key"]
	for i := 0; i < keyPageNumber; i++ {
		// skipping pages until the matching page
		if _, err := cb.ReadPageForSkip(); err != nil {
			panic(err)
		}
	}
	if err := cb.ReadPage(); err != nil {
		panic(err)
	}

	vals := cb.DataTable.Values
	fmt.Println("read page, starting to search in it")

	i := sort.Search(len(vals), func(i int) bool { return vals[i].(string) >= lookupKey })
	if vals[i] == lookupKey {
		fmt.Printf("Found the key: %d, %s\n", i, vals[i])
	} else {
		fmt.Printf("Key not found: %s\n", vals[i])
	}

	return nil, nil
}

func keyInRange(key, min, max string) bool {
	return key >= min && key <= max
}

func pageLocations(keyCol *parquet.ColumnChunk) ([]*parquet.PageLocation, error) {
	pf := thrift.NewTCompactProtocolFactory()
	fr, err := local.NewLocalFileReader(file)
	if err != nil {
		return nil, err
	}
	fr.Seek(keyCol.GetOffsetIndexOffset(), io.SeekStart)
	defer fr.Close()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(fr))
	offsetIndex := parquet.NewOffsetIndex()
	if err := offsetIndex.Read(protocol); err != nil {
		panic(err)
	}

	pageLocations := offsetIndex.GetPageLocations()
	return pageLocations, nil
}

func columnIndex(keyCol *parquet.ColumnChunk) (*parquet.ColumnIndex, error) {
	pf := thrift.NewTCompactProtocolFactory()
	fr, err := local.NewLocalFileReader(file)
	if err != nil {
		return nil, err
	}

	fr.Seek(*keyCol.ColumnIndexOffset, io.SeekStart)
	defer fr.Close()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(fr))
	cIndex := parquet.NewColumnIndex()
	if err := cIndex.Read(protocol); err != nil {
		panic(err)
	}
	fmt.Printf("found cIndex")

	return cIndex, nil
}

const file = "/Users/itaiadmi/Downloads/efcdefa7-6222-4330-86fe-c0982aceaf89.parquet"

func readParquet() ([]*S346inventory, error) {
	fr, err := local.NewLocalFileReader(file)
	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, new(S346inventory), recordNumber)
	if err != nil {
		return nil, err
	}
	u := make([]*S346inventory, recordNumber)
	footer := pr.Footer
	if footer == nil {
		panic(footer)
	}
	if err = pr.Read(&u); err != nil {
		return nil, err
	}
	pr.ReadStop()

	fr.Close()
	return u, nil
}

func generateParquet(data []*S346inventory, compression int) error {
	log.Println("generating parquet file")
	compression_type := parquet.CompressionCodec(compression)
	fw, err := local.NewLocalFileWriter(fmt.Sprintf("output.parquet_%s", compression_type))
	if err != nil {
		return err
	}
	//parameters: writer, type of struct, size
	pw, err := writer.NewParquetWriter(fw, new(S346inventory), int64(len(data)))
	if err != nil {
		return err
	}
	//compression type
	pw.CompressionType = compression_type
	defer fw.Close()
	for _, d := range data {
		if err = pw.Write(d); err != nil {
			return err
		}
	}
	if err = pw.WriteStop(); err != nil {
		return err
	}
	return nil
}
