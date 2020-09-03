package s3

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
}

type InventoryReader struct {
	manifest      *Manifest
	ctx           context.Context
	svc           s3iface.S3API
	orcFilesByKey map[string]*orcFile
	logger        logging.Logger
}

type OrcInventoryFileReader struct {
	reader *orc.Reader
	c      *orc.Cursor
	mgr    *InventoryReader
	key    string
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

func (o *InventoryReader) downloadOrcFile(key string) (string, error) {
	f, err := ioutil.TempFile("", path.Base(key))
	if err != nil {
		return "", err
	}
	o.logger.Debugf("start downloading %s to local file %s", key, f.Name())
	downloader := s3manager.NewDownloaderWithClient(o.svc)
	_, err = downloader.DownloadWithContext(o.ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(o.manifest.inventoryBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	o.logger.Debugf("finished downloading %s to local file %s", key, f.Name())
	return f.Name(), nil
}

func (o *InventoryReader) GetInventoryFileReader(key string) (InventoryFileReader, error) {
	switch o.manifest.Format {
	case OrcFormatName:
		return o.getOrcReader(key)
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

func (o *InventoryReader) getOrcReader(key string) (InventoryFileReader, error) {
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
		localFilename, err := o.downloadOrcFile(key)
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
	res.c = res.reader.Select("bucket", "key", "size", "last_modified_date", "e_tag")
	return res, nil
}

func (p *ParquetInventoryFileReader) Close() error {
	p.ReadStop()
	return p.PFile.Close()
}

func inventoryObjectFromOrc(rowData []interface{}) InventoryObject {
	return InventoryObject{
		Bucket:       rowData[0].(string),
		Key:          rowData[1].(string),
		Size:         swag.Int64(rowData[2].(int64)),
		LastModified: swag.Int64(rowData[3].(time.Time).Unix()),
		Checksum:     swag.String(rowData[4].(string)),
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
		res = append(res, inventoryObjectFromOrc(r.c.Row()))
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
