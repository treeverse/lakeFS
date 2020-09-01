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
)

type Manifest struct {
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
}

type ManifestFileReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
	SkipRows(int64) error
	Close() error
}

type CloseFunc func() error

var ErrUnsupportedInventoryFormat = errors.New("unsupported inventory type. supported types: parquet, orc")

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string) (block.Inventory, error) {
	return GenerateInventory(ctx, logger, manifestURL, a.s3, a.inventoryReader)
}
func GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, s3 s3iface.S3API, inventoryReader IInventoryReader) (block.Inventory, error) {
	if logger == nil {
		logger = logging.Default()
	}
	m, err := loadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	if m.shouldSortManifestFiles() {
		err = m.sortManifestFiles(ctx, logger, inventoryReader)
	}
	if err != nil {
		return nil, err
	}
	return &Inventory{Manifest: m, S3: s3, inventoryReader: inventoryReader, ctx: ctx, logger: logger}, nil
}

type Inventory struct {
	S3              s3iface.S3API
	Manifest        *Manifest
	ctx             context.Context //nolint:structcheck // known issue: https://github.com/golangci/golangci-lint/issues/826)
	inventoryReader IInventoryReader
	logger          logging.Logger
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

func (m *Manifest) sortManifestFiles(ctx context.Context, logger logging.Logger, inventoryReader IInventoryReader) error {
	for i := range m.Files {
		filename := m.Files[i].Key
		pr, err := inventoryReader.GetManifestFileReader(ctx, *m, filename)
		if err != nil {
			return err
		}
		// read first row from file to store the first key:
		rows := make([]InventoryObject, 1)
		err = pr.Read(&rows)
		if err != nil {
			return err
		}
		err = pr.Close()
		if err != nil {
			logger.WithFields(logging.Fields{"bucket": m.inventoryBucket, "key": filename}).
				Error("failed to close parquet reader after reading metadata")
		}
		if len(rows) != 0 {
			m.Files[i].firstKey = rows[0].Key
		}
	}
	sort.Slice(m.Files, func(i, j int) bool {
		return m.Files[i].firstKey < m.Files[j].firstKey
	})
	return nil
}

func (m *Manifest) shouldSortManifestFiles() bool {
	if m.Format == "ORC" {
		return false
	}
	return true
}

func loadManifest(manifestURL string, s3svc s3iface.S3API) (*Manifest, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return nil, err
	}
	output, err := s3svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return nil, err
	}
	var m Manifest
	err = json.NewDecoder(output.Body).Decode(&m)
	if err != nil {
		return nil, err
	}
	if m.Format != "ORC" && m.Format != "Parquet" {
		return nil, fmt.Errorf("%w. got format: %s", ErrUnsupportedInventoryFormat, m.Format)
	}
	m.URL = manifestURL
	inventoryBucketArn, err := arn.Parse(m.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	m.inventoryBucket = inventoryBucketArn.Resource
	return &m, nil
}
