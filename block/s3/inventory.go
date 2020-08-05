package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
)

type manifest struct {
	URL                string         `json:"-"`
	InventoryBucketArn string         `json:"destinationBucket"`
	SourceBucket       string         `json:"sourceBucket"`
	Files              []manifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
	inventoryBucket    string
}

type manifestFile struct {
	Key      string `json:"key"`
	firstKey string
	numRows  int
}

type ParquetReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
	SkipRows(int64) error
}

type parquetReaderGetter func(ctx context.Context, svc s3iface.S3API, inventoryBucket string, manifestFileKey string) (ParquetReader, CloseFunc, error)

type CloseFunc func() error

func (s *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string) (block.Inventory, error) {
	return GenerateInventory(ctx, logger, manifestURL, s.s3, getParquetReader)
}

func GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, s3 s3iface.S3API, getParquetReader parquetReaderGetter) (block.Inventory, error) {
	m, err := loadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	err = m.readFileMetadata(ctx, logger, s3, getParquetReader)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = logging.Default()
	}
	sort.Slice(m.Files, func(i, j int) bool {
		return m.Files[i].firstKey < m.Files[j].firstKey
	})
	return &Inventory{Manifest: m, S3: s3, getParquetReader: getParquetReader, logger: logger}, nil
}

type Inventory struct {
	S3               s3iface.S3API
	Manifest         *manifest
	ctx              context.Context //nolint:structcheck // known issue: https://github.com/golangci/golangci-lint/issues/826)
	getParquetReader parquetReaderGetter
	logger           logging.Logger
}

func (inv *Inventory) Iterator() block.InventoryIterator {
	return NewInventoryIterator(inv)
}

func (inv *Inventory) SourceName() string {
	return inv.Manifest.SourceBucket
}

func (inv *Inventory) InventoryURL() string {
	return inv.Manifest.URL
}

func (m *manifest) readFileMetadata(ctx context.Context, logger logging.Logger, s3 s3iface.S3API, getParquetReader parquetReaderGetter) error {
	for i := range m.Files {
		filename := m.Files[i].Key
		pr, closeReader, err := getParquetReader(ctx, s3, m.inventoryBucket, filename)
		if err != nil {
			return err
		}
		m.Files[i].numRows = int(pr.GetNumRows())
		// read first row from file to store the first key:
		rows := make([]ParquetInventoryObject, 1)
		err = pr.Read(&rows)
		if err != nil {
			return err
		}
		err = closeReader()
		if err != nil {
			logger.WithFields(logging.Fields{"bucket": m.inventoryBucket, "key": filename}).
				Error("failed to close parquet reader after reading metadata")
		}
		if len(rows) != 0 {
			m.Files[i].firstKey = rows[0].Key
		}
	}
	return nil
}

func loadManifest(manifestURL string, s3svc s3iface.S3API) (*manifest, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return nil, err
	}
	output, err := s3svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return nil, err
	}
	var m manifest
	err = json.NewDecoder(output.Body).Decode(&m)
	if err != nil {
		return nil, err
	}
	if m.Format != "Parquet" {
		return nil, errors.New("currently only parquet inventories are supported. got: " + m.Format)
	}
	m.URL = manifestURL
	inventoryBucketArn, err := arn.Parse(m.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	m.inventoryBucket = inventoryBucketArn.Resource
	return &m, nil
}

func getParquetReader(ctx context.Context, svc s3iface.S3API, inventoryBucket string, manifestFileKey string) (ParquetReader, CloseFunc, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, inventoryBucket, manifestFileKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	var rawObject ParquetInventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	closer := func() error {
		pr.ReadStop()
		return pf.Close()
	}
	return pr, closer, nil
}
