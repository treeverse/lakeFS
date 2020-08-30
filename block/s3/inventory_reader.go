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
	idx           int
	key           string
	localFilename string
	err           error
	done          bool
	ready         bool
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
	svc            s3iface.S3API
	orcFiles       map[string]map[string]*orcFile
	readerByFormat map[string]ManifestFileReader
	logger         logging.Logger
}

func (o *InventoryReader) clean(manifestURL string, key string) error {
	defer func() {
		o.orcFiles[manifestURL][key].localFilename = ""
	}()
	return o.delete(manifestURL, key)
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
	return &InventoryReader{svc: svc, logger: logger, orcFiles: make(map[string]map[string]*orcFile)}
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
	manifestOrcFiles, ok := o.orcFiles[m.URL]
	if !ok {
		manifestOrcFiles = make(map[string]*orcFile, len(m.Files))
		o.orcFiles[m.URL] = manifestOrcFiles
		for i, f := range m.Files {
			o.orcFiles[m.URL][f.Key] = &orcFile{
				idx: i,
				key: f.Key,
			}
		}
	}
	file := manifestOrcFiles[key]
	if file.localFilename == "" {
		localFilename, err := o.download(m, key)
		if err != nil {
			file.err = err
			return nil, err
		}
		file.ready = true
		file.localFilename = localFilename
	}
	i := manifestOrcFiles[key].idx
	go func() {
		// prepare next file
		if i == len(m.Files)-1 {
			return
		}
		nextKey := m.Files[i+1].Key
		if _, ok := o.orcFiles[m.URL][nextKey]; ok {
			return
		}
		orcFile := orcFile{
			idx: i + 1,
			key: nextKey,
		}
		o.orcFiles[m.URL][nextKey] = &orcFile
		defer func() {
			orcFile.done = true
		}()
		localFilename, err := o.download(m, nextKey)
		orcFile.localFilename = localFilename
		if err != nil {
			orcFile.err = err
			return
		}
		orcFile.ready = true
	}()
	if o.orcFiles[m.URL][key].err != nil {
		return nil, fmt.Errorf("error when trying to download orc file: %v", o.orcFiles[m.URL][key].err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	o.logger.Info("file not ready on time, wait 30 seconds")
	for {
		if o.orcFiles[m.URL][key].ready || ctx.Err() != nil {
			break
		}
		time.Sleep(time.Second)
	}
	o.logger.Info("finished waiting")
	if !o.orcFiles[m.URL][key].ready {
		return nil, errors.New("orc file not ready locally")
	}
	orcReader, err := orc.Open(o.orcFiles[m.URL][key].localFilename)
	if err != nil {
		return nil, err
	}
	res := &OrcManifestFileReader{reader: *orcReader, mgr: o, manifestURL: m.URL, key: key}
	res.c = res.reader.Select("bucket", "key", "size", "last_modified_date")
	return res, nil
}

func (o *InventoryReader) delete(manifestURL string, key string) error {
	return os.Remove(o.orcFiles[manifestURL][key].localFilename)
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
