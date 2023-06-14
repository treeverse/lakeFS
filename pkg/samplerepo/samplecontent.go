package samplerepo

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/samplerepo/assets"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	sampleRepoFSRootPath = "sample"
	sampleRepoCommitMsg  = "Add sample data"
)

func PopulateSampleRepo(ctx context.Context, repo *catalog.Repository, cat catalog.Interface, pathProvider upload.PathProvider, blockAdapter block.Adapter, user *model.User) error {
	// upload sample data
	// we skip checking if the repo and branch exist, since we just created them
	// we also skip checking if the file exists, since we know the repo is empty
	const tmplSuffix = ".tmpl"
	config := map[string]string{
		"RepoName": repo.Name,
	}

	err := fs.WalkDir(assets.SampleData, sampleRepoFSRootPath, func(p string, d fs.DirEntry, topLevelErr error) error {
		// handle a top-level error
		if topLevelErr != nil {
			return topLevelErr
		}

		if d.IsDir() {
			// noop for directories
			return nil
		}

		var (
			contentPath   string
			contentReader io.Reader
			contentSize   int64
		)
		if filepath.Ext(p) == tmplSuffix {
			tmpl, err := template.ParseFS(assets.SampleData, p)
			if err != nil {
				return err
			}
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, config); err != nil {
				return err
			}
			contentPath = strings.TrimSuffix(p, tmplSuffix)
			contentReader = bufio.NewReader(&buf)
			contentSize = int64(buf.Len())
		} else {
			// open file from embedded FS
			file, err := assets.SampleData.Open(p)
			if err != nil {
				return err
			}
			// embed file close does nothing, we just like to keep it aligned with the open
			defer func() { _ = file.Close() }()
			fileStat, err := d.Info()
			if err != nil {
				return err
			}
			contentPath = p
			contentReader = file
			contentSize = fileStat.Size()
		}

		// write file to storage
		address := pathProvider.NewPath()
		blob, err := upload.WriteBlob(ctx, blockAdapter, repo.StorageNamespace, address, contentReader, contentSize, block.PutOpts{})
		if err != nil {
			return err
		}

		// create metadata entry
		writeTime := time.Now()
		entry := catalog.NewDBEntryBuilder().
			Path(strings.TrimPrefix(contentPath, sampleRepoFSRootPath+"/")).
			PhysicalAddress(blob.PhysicalAddress).
			CreationDate(writeTime).
			Size(blob.Size).
			Checksum(blob.Checksum).
			AddressType(catalog.AddressTypeRelative).
			Build()

		// write metadata entry
		err = cat.CreateEntry(ctx, repo.Name, repo.DefaultBranch, entry)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// if we succeeded, commit the changes
	// commit changes
	_, err = cat.Commit(ctx, repo.Name, repo.DefaultBranch, sampleRepoCommitMsg,
		user.Username, map[string]string{}, swag.Int64(time.Now().Unix()), nil)

	return err
}

func SampleRepoAddBranchProtection(ctx context.Context, repo *catalog.Repository, cat catalog.Interface) error {
	// Set branch protection on main branch

	err := cat.CreateBranchProtectionRule(ctx, repo.Name, repo.DefaultBranch, []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_COMMIT})

	return err
}
