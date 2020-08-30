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
	"github.com/cznic/mathutil"
	"github.com/go-openapi/swag"
	"github.com/scritchley/orc"
	"github.com/treeverse/lakefs/logging"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
)

type orcFile struct {
	key             string
	manifestUrls    map[string]bool
	localFilename   string
	downloadStarted bool
	err             error
	ready           bool
}

type OrcManifestFileReader struct {
	reader      orc.Reader
	c           *orc.Cursor
	mgr         *InventoryReader
	manifestURL string
	key         string
}

type IInventoryReader interface {
	GetReader(ctx context.Context, m Manifest, key string) (ManifestFileReader, error)
}

type InventoryReader struct {
	svc                  s3iface.S3API
	orcFiles             map[string]*orcFile
	indexByManifestByKey map[string]map[string]int
	readerByFormat       map[string]ManifestFileReader
	logger               logging.Logger
}

func (o *InventoryReader) clean(manifestUrl string, key string) error {
	delete(o.indexByManifestByKey[key], manifestUrl)
	if len(o.indexByManifestByKey[key]) != 0 {
		return nil
	}
	// file cleaned from all manifests:
	err := o.delete(key)
	o.orcFiles[key].downloadStarted = false
	o.orcFiles[key].localFilename = ""
	return err

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
	return &InventoryReader{svc: svc, logger: logger, orcFiles: make(map[string]*orcFile), indexByManifestByKey: make(map[string]map[string]int)}
}

func (o *InventoryReader) GetReader(ctx context.Context, m Manifest, key string) (ManifestFileReader, error) {
	if manifestFileReader, ok := o.readerByFormat[m.Format]; ok {
		return manifestFileReader, nil
	}
	var err error
	var manifestFileReader ManifestFileReader
	switch m.Format {
	case "ORC":
		manifestFileReader, err = o.getOrcReader(ctx, m, key)
	case "Parquet":
		manifestFileReader, err = o.getParquetReader(ctx, m, key)
	default:
		return nil, ErrUnsupportedInventoryFormat
	}
	if err != nil {
		o.readerByFormat[m.Format] = manifestFileReader
		return nil, err
	}
	return manifestFileReader, nil
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
	file, ok := o.orcFiles[key]
	if !ok {
		file = &orcFile{
			key:          key,
			manifestUrls: map[string]bool{m.URL: true},
		}
		o.orcFiles[key] = file
		if _, ok := o.indexByManifestByKey[key]; !ok {
			o.indexByManifestByKey[key] = make(map[string]int)
		}
		for idx, f := range m.Files {
			if f.Key == key {
				o.indexByManifestByKey[key][m.URL] = idx
				continue
			}
		}
	}
	if !file.downloadStarted {
		file.downloadStarted = true
		localFilename, err := o.download(m, key)
		if err != nil {
			file.err = err
			return nil, err
		}
		file.ready = true
		file.localFilename = localFilename
	}

	go func() {
		// prepare next file
		nextIdx := o.indexByManifestByKey[key][m.URL] + 1
		if nextIdx == len(m.Files) {
			return
		}
		nextKey := m.Files[nextIdx].Key
		file, ok := o.orcFiles[nextKey]
		if !ok || file.downloadStarted {
			return
		}
		file = &orcFile{
			key:          nextKey,
			manifestUrls: map[string]bool{m.URL: true},
		}
		o.indexByManifestByKey[nextKey][m.URL] = nextIdx
		o.orcFiles[nextKey] = file
		file.downloadStarted = true
		localFilename, err := o.download(m, nextKey)
		file.localFilename = localFilename
		if err != nil {
			file.err = err
			return
		}
		file.ready = true
	}()
	if o.orcFiles[key].err != nil {
		return nil, fmt.Errorf("error when trying to download orc file: %v", o.orcFiles[key].err)
	}
	if !o.orcFiles[key].ready {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		o.logger.Info("file not ready on time, wait 30 seconds")
		for {
			if o.orcFiles[key].ready || ctx.Err() != nil {
				break
			}
			time.Sleep(time.Second)
		}
		o.logger.Info("finished waiting")
	}
	if !o.orcFiles[key].ready {
		return nil, errors.New("orc file not ready locally")
	}
	orcReader, err := orc.Open(o.orcFiles[key].localFilename)
	if err != nil {
		return nil, err
	}
	res := &OrcManifestFileReader{reader: *orcReader, mgr: o, manifestURL: m.URL, key: key}
	res.c = res.reader.Select("bucket", "key", "size", "last_modified_date")
	return res, nil
}

func (o *InventoryReader) delete(key string) error {
	return os.Remove(o.orcFiles[key].localFilename)
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
	return int64(mathutil.Min(r.reader.NumRows(), 1000))
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
	_ = r.mgr.clean(r.manifestURL, r.key)
	return nil
}
