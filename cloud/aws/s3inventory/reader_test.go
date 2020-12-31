package s3inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cznic/mathutil"
	"github.com/go-openapi/swag"
	"github.com/scritchley/orc"
	"github.com/treeverse/lakefs/logging"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/writer"
)

const inventoryBucketName = "inventory-bucket"

type TestObject struct {
	Bucket             string  `parquet:"name=bucket, type=UTF8"`
	Key                string  `parquet:"name=key, type=UTF8"`
	IsLatest           *bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker     *bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
	Size               *int64  `parquet:"name=size, type=INT_64"`
	LastModifiedMillis *int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	Checksum           *string `parquet:"name=e_tag, type=UTF8"`
}

func parquetSchema(fieldToRemove string) *schema.JSONSchemaItemType {
	fieldMap := map[string]string{
		bucketFieldName:           "name=bucket, inname=Bucket, type=UTF8, repetitiontype=REQUIRED, fieldid=1",
		keyFieldName:              "name=key, inname=Key, type=UTF8, repetitiontype=REQUIRED, fieldid=2",
		isLatestFieldName:         "name=is_latest, inname=IsLatest, type=BOOLEAN, repetitiontype=OPTIONAL, fieldid=3",
		isDeleteMarkerFieldName:   "name=is_delete_marker, inname=IsDeleteMarker, type=BOOLEAN, repetitiontype=OPTIONAL, fieldid=4",
		sizeFieldName:             "name=size, inname=Size, type=INT64, repetitiontype=OPTIONAL, fieldid=5",
		lastModifiedDateFieldName: "name=last_modified_date, inname=LastModifiedMillis, type=INT64, repetitiontype=OPTIONAL, fieldid=6",
		eTagFieldName:             "name=e_tag, inname=Checksum, type=UTF8, repetitiontype=OPTIONAL, fieldid=7",
	}
	fields := make([]*schema.JSONSchemaItemType, 0, len(fieldMap))
	for field, tag := range fieldMap {
		if field == fieldToRemove {
			continue
		}
		fields = append(fields, &schema.JSONSchemaItemType{Tag: tag})
	}
	return &schema.JSONSchemaItemType{
		Tag:    "name=s3inventory, repetitiontype=REQUIRED",
		Fields: fields,
	}
}

func generateParquet(t *testing.T, objs <-chan *TestObject, fieldToRemove string) *os.File {
	f, err := ioutil.TempFile("", "parquettest")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer func() {
		_ = os.Remove(f.Name())
	}()
	fw, err := local.NewLocalFileWriter(f.Name())
	if err != nil {
		t.Fatalf("failed to create parquet file writer: %v", err)
	}
	defer func() {
		_ = fw.Close()
	}()
	jsonSchema, err := json.Marshal(parquetSchema(fieldToRemove))
	if err != nil {
		t.Fatalf("failed to marshal to JSON %+v: %s", parquetSchema(fieldToRemove), err)
	}
	jsonSchemaStr := string(jsonSchema)
	pw, err := writer.NewParquetWriter(fw, jsonSchemaStr, 4)
	if err != nil {
		t.Fatalf("failed to create parquet writer: %v", err)
	}
	for obj := range objs {
		err = pw.Write(obj)
		if err != nil {
			t.Fatalf("failed to write object to parquet: %v", err)
		}
	}
	err = pw.WriteStop()
	if err != nil {
		t.Fatalf("failed to stop parquet writer: %v", err)
	}
	return f
}

func orcSchema(fieldToRemove string) string {
	fieldTypes := map[string]string{
		bucketFieldName:           "string",
		keyFieldName:              "string",
		isLatestFieldName:         "boolean",
		isDeleteMarkerFieldName:   "boolean",
		sizeFieldName:             "int",
		lastModifiedDateFieldName: "timestamp",
		eTagFieldName:             "string",
	}
	var orcSchema strings.Builder
	orcSchema.WriteString("struct<")
	isFirst := true
	for _, field := range inventoryFields {
		if fieldToRemove == field {
			continue
		}
		if !isFirst {
			orcSchema.WriteString(",")
		}
		isFirst = false
		orcSchema.WriteString(fmt.Sprintf("%s:%s", field, fieldTypes[field]))
	}
	orcSchema.WriteString(">")
	return orcSchema.String()
}

func getOrcValues(o *TestObject, fieldToRemove string) []interface{} {
	fieldValues := map[string]interface{}{
		bucketFieldName:         o.Bucket,
		keyFieldName:            o.Key,
		isLatestFieldName:       o.IsLatest == nil || swag.BoolValue(o.IsLatest),
		isDeleteMarkerFieldName: swag.BoolValue(o.IsDeleteMarker),
		sizeFieldName:           swag.Int64Value(o.Size),
		lastModifiedDateFieldName: time.Unix(swag.Int64Value(o.LastModifiedMillis)/1000,
			(swag.Int64Value(o.LastModifiedMillis)%1000)*1_000_000),
		eTagFieldName: swag.StringValue(o.Checksum),
	}
	values := make([]interface{}, 0, len(fieldValues))
	for _, field := range inventoryFields {
		if fieldToRemove == field {
			continue
		}
		values = append(values, fieldValues[field])
	}
	return values
}

func generateOrc(t *testing.T, objs <-chan *TestObject, fieldToRemove string) *os.File {
	f, err := ioutil.TempFile("", "orctest")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer func() {
		_ = os.Remove(f.Name())
	}()
	orcSchema := orcSchema(fieldToRemove)
	s, err := orc.ParseSchema(orcSchema)
	if err != nil {
		t.Fatalf("failed to parse orc schema: %v", err)
	}
	w, err := orc.NewWriter(f, orc.SetSchema(s), orc.SetStripeTargetSize(100))
	if err != nil {
		t.Fatalf("failed to create orc writer: %v", err)
	}
	for o := range objs {
		err = w.Write(getOrcValues(o, fieldToRemove)...)
		if err != nil {
			t.Fatalf("failed to write object to orc: %v", err)
		}
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("failed to close orc writer: %v", err)
	}
	_, _ = f.Seek(0, 0)
	return f
}

func TestReaders(t *testing.T) {
	svc, testServer := getS3Fake(t)
	defer testServer.Close()
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(inventoryBucketName),
	})
	if err != nil {
		t.Fatal(err)
	}
	testdata := map[string]struct {
		ObjectNum           int
		ExpectedReadObjects int
		ExpectedMaxValue    string
		ExpectedMinValue    string
		ExcludeField        string
	}{
		"2 objects": {
			ObjectNum:           2,
			ExpectedReadObjects: 2,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f00001",
		},
		"12500 objects": {
			ObjectNum:           12500,
			ExpectedReadObjects: 12500,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f12499",
		},
		"100 objects": {
			ObjectNum:           100,
			ExpectedReadObjects: 100,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f00099",
		},
		"without size field": {
			ObjectNum:           12500,
			ExpectedReadObjects: 12500,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f12499",
			ExcludeField:        "size",
		},
		"without is_latest field": {
			ObjectNum:           12500,
			ExpectedReadObjects: 12500,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f12499",
			ExcludeField:        "is_latest",
		},
		"without is_delete_marker field": {
			ObjectNum:           12500,
			ExpectedReadObjects: 12500,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f12499",
			ExcludeField:        "is_delete_marker",
		},
		"without e_tag field": {
			ObjectNum:           100,
			ExpectedReadObjects: 100,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f00099",
			ExcludeField:        "e_tag",
		},
	}
	for _, format := range []string{"ORC", "Parquet"} {
		for testName, test := range testdata {
			t.Run(fmt.Sprintf("%s %s", strings.ToLower(format), testName), func(t *testing.T) {
				now := time.Now().Truncate(time.Millisecond)
				lastModified := []time.Time{now, now.Add(-1 * time.Hour), now.Add(-2 * time.Hour), now.Add(-3 * time.Hour)}
				var localFile *os.File
				if format == "ORC" {
					localFile = generateOrc(t, objs(test.ObjectNum, lastModified), test.ExcludeField)
				} else if format == "Parquet" {
					localFile = generateParquet(t, objs(test.ObjectNum, lastModified), test.ExcludeField)
				}
				uploadFile(t, svc, inventoryBucketName, "myFile.inv", localFile)
				reader := NewReader(context.Background(), svc, logging.Default())
				fileReader, err := reader.GetFileReader(format, inventoryBucketName, "myFile.inv")
				if err != nil {
					t.Fatalf("failed to create file reader: %v", err)
				}
				numRowsResult := int(fileReader.GetNumRows())
				if test.ObjectNum != numRowsResult {
					t.Fatalf("unexpected result from GetNumRows. expected=%d, got=%d", test.ObjectNum, numRowsResult)
				}
				minValueResult := fileReader.FirstObjectKey()
				if test.ExpectedMinValue != minValueResult {
					t.Fatalf("unexpected result from FirstObjectKey. expected=%s, got=%s", test.ExpectedMinValue, minValueResult)
				}
				maxValueResult := fileReader.LastObjectKey()
				if test.ExpectedMaxValue != maxValueResult {
					t.Fatalf("unexpected result from LastObjectKey. expected=%s, got=%s", test.ExpectedMaxValue, maxValueResult)
				}
				readBatchSize := 1000
				offset := 0
				readCount := 0
				for {
					res, err := fileReader.Read(readBatchSize)
					if err != nil {
						t.Fatalf("failed to read from file reader: %v", err)
					}
					expectedSize := 500
					if test.ExcludeField == "size" {
						expectedSize = 0
					}
					expectedChecksum := "abcdefg"
					if test.ExcludeField == "e_tag" {
						expectedChecksum = ""
					}
					for i := offset; i < mathutil.Min(offset+readBatchSize, test.ObjectNum); i++ {
						verifyObject(t, res[i-offset], &InventoryObject{
							Bucket:         inventoryBucketName,
							Key:            fmt.Sprintf("f%05d", i),
							IsLatest:       true,
							IsDeleteMarker: false,
							Size:           int64(expectedSize),
							LastModified:   &lastModified[i%len(lastModified)],
							Checksum:       expectedChecksum,
						}, i, offset/readBatchSize, i-offset)
					}
					offset += len(res)
					readCount += len(res)
					if len(res) != readBatchSize {
						break
					}
				}
				if test.ExpectedReadObjects != readCount {
					t.Fatalf("read unexpected number of keys from inventory. expected=%d, got=%d", test.ExpectedReadObjects, readCount)
				}
				if fileReader.Close() != nil {
					t.Fatalf("failed to close file reader")
				}
			})
		}
	}
}
