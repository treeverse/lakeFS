package diagnostics

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

// BlockCollector collects diagnostics information and write the collected content into a writer in a zip format
type BlockCollector struct {
	cataloger   catalog.Cataloger
	adapter     block.Adapter
	cfg         *config.Config
	dbCollector *DBCollector
}

// NewDBCollector accepts database to work with during collect
func NewBlockCollector(adb db.Database, cataloger catalog.Cataloger, cfg *config.Config, adapter block.Adapter) *BlockCollector {
	return &BlockCollector{
		cataloger:   cataloger,
		cfg:         cfg,
		adapter:     adapter,
		dbCollector: NewDBCollector(adb),
	}
}

// Collect query information from the database into csv files and write everything to io writer
func (c *BlockCollector) Collect(ctx context.Context, w io.Writer) (err error) {
	writer := zip.NewWriter(w)
	defer func() { err = writer.Close() }()

	errs := c.dbCollector.collectWithZip(ctx, writer)

	rangeErrs := c.rangesStats(ctx, writer)
	errs = append(errs, rangeErrs...)
	if err = writeErrors(writer, errs); err != nil {
		err = fmt.Errorf("write errors: %w", err)
	}

	return nil
}

func (c *BlockCollector) rangesStats(ctx context.Context, writer *zip.Writer) []error {
	var errs []error
	repos, _, err := c.cataloger.ListRepositories(ctx, -1, "")
	if err != nil {
		// Cannot list repos, nothing to do..
		errs = append(errs, fmt.Errorf("listing repositories: %w", err))
		return errs
	}

	tierFSParams, err := c.cfg.GetCommittedTierFSParams()
	if err != nil {
		errs = append(errs, fmt.Errorf("get tierFS params: %w", err))
		return errs
	}

	rangesFile, err := writer.Create("graveler-ranges")
	if err != nil {
		errs = append(errs, fmt.Errorf("creating ranges file: %w", err))
		return errs
	}

	csvWriter := csv.NewWriter(rangesFile)
	defer csvWriter.Flush()
	if err := csvWriter.Write([]string{"repo", "type", "count(max:1000)"}); err != nil {
		errs = append(errs, fmt.Errorf("writing headers: %w", err))
	}

	for _, repo := range repos {
		count := 0
		counter := func(id string) error {
			count++
			return nil
		}

		if err := c.adapter.Walk(block.WalkOpts{
			StorageNamespace: repo.StorageNamespace,
			Prefix:           tierFSParams.BlockStoragePrefix,
		}, counter); err != nil {
			errs = append(errs, fmt.Errorf("listing ranges and meta-ranges: %w", err))
		}
		if err := csvWriter.Write([]string{repo.Name, "range", strconv.Itoa(count)}); err != nil {
			errs = append(errs, fmt.Errorf("writing meta-ranges for repo (%s): %w", repo.Name, err))
		}
	}

	return errs
}
