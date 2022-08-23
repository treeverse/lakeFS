package diagnostics

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	pyramidparams "github.com/treeverse/lakefs/pkg/pyramid/params"
)

// Collector collects diagnostics information and write the collected content into a writer in a zip format
type Collector struct {
	catalog    catalog.Interface
	adapter    block.Adapter
	pyramidCfg *pyramidparams.ExtParams
}

// NewCollector accepts database to work with during collect
func NewCollector(catalog catalog.Interface, cfg *pyramidparams.ExtParams, adapter block.Adapter) *Collector {
	return &Collector{
		catalog:    catalog,
		pyramidCfg: cfg,
		adapter:    adapter,
	}
}

// Collect query information into csv files and write everything to io writer
func (c *Collector) Collect(ctx context.Context, w io.Writer) (merr error) {
	writer := zip.NewWriter(w)

	defer func() {
		err := writer.Close()
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	}()

	if err := c.rangesStats(ctx, writer); err != nil {
		merr = multierror.Append(merr, err)
	}
	if err := writeErrors(writer, merr); err != nil {
		merr = multierror.Append(merr, fmt.Errorf("write errors: %w", err))
	}

	return merr
}

func (c *Collector) rangesStats(ctx context.Context, writer *zip.Writer) error {
	var combinedErr error
	repos, _, err := c.catalog.ListRepositories(ctx, -1, "", "")
	if err != nil {
		// Cannot list repos, nothing to do..
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("listing repositories: %w", err))
		return combinedErr
	}

	rangesFile, err := writer.Create("graveler-ranges.csv")
	if err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("creating ranges file: %w", err))
		return combinedErr
	}

	csvWriter := csv.NewWriter(rangesFile)
	defer csvWriter.Flush()
	if err := csvWriter.Write([]string{"repo", "type", "count(max:1000)"}); err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("writing headers: %w", err))
	}

	for _, repo := range repos {
		count := 0
		counter := func(id string) error {
			count++
			return nil
		}

		if err := c.adapter.Walk(ctx, block.WalkOpts{
			StorageNamespace: repo.StorageNamespace,
			Prefix:           c.pyramidCfg.BlockStoragePrefix,
		}, counter); err != nil {
			combinedErr = multierror.Append(combinedErr, fmt.Errorf("listing ranges and meta-ranges: %w", err))
		}
		if err := csvWriter.Write([]string{repo.Name, "range", strconv.Itoa(count)}); err != nil {
			combinedErr = multierror.Append(combinedErr, fmt.Errorf("writing meta-ranges for repo (%s): %w", repo.Name, err))
		}
	}

	return combinedErr
}

func writeErrors(writer *zip.Writer, err error) error {
	if err == nil {
		return nil
	}

	w, e := writer.Create("errors.log")
	if e != nil {
		return e
	}

	_, e = fmt.Fprint(w, err.Error())
	return e
}
