package s3

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sync"
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

type orcFile struct {
	key           string
	idx           int
	localFilename string
	ready         bool
}

type OrcManifestFileReader struct {
	reader      *orc.Reader
	c           *orc.Cursor
	mgr         *InventoryReader
	manifestURL string
	key         string
}

type IInventoryReader interface {
	GetManifestFileReader(ctx context.Context, m Manifest, key string) (ManifestFileReader, error)
}

type InventoryReader struct {
	svc                     s3iface.S3API
	orcFilesByKeyByManifest map[string]map[string]*orcFile
	//indexByManifestByKey    map[string]map[string]int
	logger logging.Logger
	mux    sync.Mutex
}

func (o *InventoryReader) clean(manifestUrl string, key string) {
	localFilename := o.orcFilesByKeyByManifest[manifestUrl][key].localFilename
	delete(o.orcFilesByKeyByManifest[manifestUrl], key)
	defer func() {
		_ = os.Remove(localFilename)
	}()
}

func (o *InventoryReader) download(m Manifest, key string) (string, error) {
	f, err := ioutil.TempFile("", path.Base(key))
	if err != nil {
		return "", err
	}
	o.logger.Infof("start downloading %s to local file %s", key, f.Name())
	downloader := s3manager.NewDownloaderWithClient(o.svc)
	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(m.inventoryBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	o.logger.Infof("finished downloading %s to local file %s", key, f.Name())

	return f.Name(), nil
}

func NewInventoryReader(svc s3iface.S3API, logger logging.Logger) IInventoryReader {
	logger.Info("creating orc download manager")
	return &InventoryReader{svc: svc, logger: logger, orcFilesByKeyByManifest: make(map[string]map[string]*orcFile)}
}

func (o *InventoryReader) GetManifestFileReader(ctx context.Context, m Manifest, key string) (ManifestFileReader, error) {
	switch m.Format {
	case "ORC":
		return o.getOrcReader(ctx, m, key)
	case "Parquet":
		return o.getParquetReader(ctx, m, key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
}

type ParquetManifestFileReader struct {
	reader.ParquetReader
}

func (p *ParquetManifestFileReader) Close() error {
	p.ReadStop()
	return p.Close()
}

func (o *InventoryReader) getParquetReader(ctx context.Context, m Manifest, key string) (ManifestFileReader, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, o.svc, m.inventoryBucket, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	var rawObject InventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	return &ParquetManifestFileReader{ParquetReader: *pr}, nil
}

func (o *InventoryReader) getOrcReader(ctx context.Context, m Manifest, key string) (ManifestFileReader, error) {
	manifestFilesByKey, ok := o.orcFilesByKeyByManifest[m.URL]
	if !ok {
		manifestFilesByKey = make(map[string]*orcFile)
		o.orcFilesByKeyByManifest[m.URL] = manifestFilesByKey
	}
	file, ok := manifestFilesByKey[key]
	if !ok {
		file = &orcFile{key: key}
		manifestFilesByKey[key] = file
	}
	for idx, f := range m.Files {
		if f.Key == key {
			file.idx = idx
			break
		}
	}
	if !file.ready {
		localFilename, err := o.download(m, key)
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
	res := &OrcManifestFileReader{reader: orcReader, mgr: o, manifestURL: m.URL, key: key}
	res.c = res.reader.Select("bucket", "key", "size", "last_modified_date")
	return res, nil
}

func fromOrc(rowData []interface{}) InventoryObject {
	return InventoryObject{
		Bucket:       rowData[0].(string),
		Key:          rowData[1].(string),
		Size:         swag.Int64(rowData[2].(int64)),
		LastModified: swag.Int64(rowData[3].(time.Time).Unix()),
	}
}

func (r *OrcManifestFileReader) Read(dstInterface interface{}) error {
	num := reflect.ValueOf(dstInterface).Elem().Len()
	res := make([]InventoryObject, 0, num)

	for {
		if !r.c.Next() {
			r.mgr.logger.Infof("start new stripe in file %s", r.key)
			if !r.c.Stripes() {
				return nil
			} else if !r.c.Next() {
				return nil
			}
		}
		res = append(res, fromOrc(r.c.Row()))
		if len(res) == num {
			break
		}
	}
	reflect.ValueOf(dstInterface).Elem().Set(reflect.ValueOf(res))
	return nil
}

func (r *OrcManifestFileReader) GetNumRows() int64 {
	return int64(r.reader.NumRows())
}

func (r *OrcManifestFileReader) SkipRows(i int64) error {
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
	return errors.New("no more rows to skip")
}

func (r *OrcManifestFileReader) Close() error {
	// TODO handle errors
	_ = r.c.Close()
	_ = r.reader.Close()
	r.mgr.clean(r.manifestURL, r.key)
	return nil
}
