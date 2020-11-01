package main

import (
	"fmt"
	"log"
	"strconv"

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
	files, err := readParquet()
	if err != nil {
		panic(err)
	}

	const newFilesCount = 10000
	moreFiles := make([]*S346inventory, newFilesCount)

	for i := 0; i < 100000; i++ {
		o := files[i%len(files)]
		o.Key = o.Key + strconv.Itoa(i)
		moreFiles[i] = o
	}

	for i := 0; i <= 6; i++ {
		generateParquet(moreFiles, i)
	}
}

const recordNumber = 10000
const lookupBucket = "lakefs-example-data"
const lookupKey = "events/dt=2020-01-12/part-00000-db6d9dfd-681b-4eb4-af00-cabc91b8a2b2.c000"

func readSingleRow() ([]*S346inventory, error) {
	fr, err := local.NewLocalFileReader("/Users/itaiadmi/Downloads/efcdefa7-6222-4330-86fe-c0982aceaf89.parquet")
	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, new(S346inventory), recordNumber)
	if err != nil {
		return nil, err
	}

	for _, rg := range pr.Footer.RowGroups{
		cols := rg.GetColumns()
		bucketCol := cols[1]
		bucketStats :=bucketCol.GetMetaData().GetStatistics()
		keyCol := cols[2]
		keyStats :=keyCol.GetMetaData().GetStatistics()

		if string(bucketStats.MinValue) >=
	}
}

func readParquet() ([]*S346inventory, error) {
	fr, err := local.NewLocalFileReader("/Users/itaiadmi/Downloads/efcdefa7-6222-4330-86fe-c0982aceaf89.parquet")
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
